"""Kaggle API ingestion with retries, checksum-based idempotency and cloud-agnostic landing.

The Kaggle API token is resolved through the secret backend (Secret Manager / Key Vault),
written to a 0600 temp file, and removed in a finally block — never persisted in the image.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from gaming_lakehouse.config import load_settings
from gaming_lakehouse.logging_utils import get_logger
from gaming_lakehouse.secrets import get_secret

log = get_logger(__name__)

MAX_RETRIES = 5
BACKOFF_BASE_SECONDS = 2


@dataclass(frozen=True)
class IngestResult:
    dataset_key: str
    slug: str
    files: list[str]
    bytes_written: int
    checksum: str
    landing_uri: str
    skipped: bool = False


@contextmanager
def kaggle_credentials() -> Iterator[Path]:
    """Materialize ~/.kaggle/kaggle.json for the lifetime of the block only."""
    token = get_secret("kaggle-api-token")  # {"username": "...", "key": "..."}
    tmp_home = Path(tempfile.mkdtemp(prefix="kaggle_home_"))
    cred_dir = tmp_home / ".kaggle"
    cred_dir.mkdir(parents=True)
    cred_file = cred_dir / "kaggle.json"
    cred_file.write_text(token if isinstance(token, str) else json.dumps(token))
    cred_file.chmod(0o600)
    previous_home = os.environ.get("HOME")
    os.environ["HOME"] = str(tmp_home)
    try:
        yield cred_file
    finally:
        if previous_home:
            os.environ["HOME"] = previous_home
        shutil.rmtree(tmp_home, ignore_errors=True)


def _checksum(directory: Path) -> str:
    """Content hash over the extracted files -> lets us skip an unchanged Kaggle version."""
    digest = hashlib.sha256()
    for path in sorted(directory.rglob("*")):
        if path.is_file():
            digest.update(path.name.encode())
            with path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
    return digest.hexdigest()


def _upload(local_dir: Path, landing_uri: str) -> int:
    """Copy to object storage. fsspec keeps this identical on gs:// and abfss://."""
    import fsspec

    fs, _, _ = fsspec.get_fs_token_paths(landing_uri)
    total = 0
    for path in local_dir.rglob("*"):
        if path.is_file():
            target = f"{landing_uri}/{path.relative_to(local_dir).as_posix()}"
            fs.put_file(str(path), target)
            total += path.stat().st_size
    return total


def download_dataset(dataset_key: str, *, force: bool = False) -> IngestResult:
    from kaggle.api.kaggle_api_extended import KaggleApi

    settings = load_settings()
    spec = settings.get(f"datasets.{dataset_key}")
    if not spec:
        raise KeyError(f"Unknown dataset key {dataset_key!r} — add it to conf/datasets.yaml")
    slug: str = spec["slug"]

    from gaming_lakehouse.storage import landing_path

    landing_uri = landing_path("kaggle", dataset_key)
    workdir = Path(tempfile.mkdtemp(prefix=f"kaggle_{dataset_key}_"))

    try:
        with kaggle_credentials():
            api = KaggleApi()
            api.authenticate()
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    api.dataset_download_files(slug, path=str(workdir), unzip=True, quiet=True)
                    break
                except Exception as exc:
                    if attempt == MAX_RETRIES:
                        raise
                    sleep_for = BACKOFF_BASE_SECONDS**attempt
                    log.warning(
                        "kaggle download failed, retrying",
                        extra={
                            "extra_fields": {
                                "slug": slug,
                                "attempt": attempt,
                                "sleep": sleep_for,
                                "error": str(exc),
                            }
                        },
                    )
                    time.sleep(sleep_for)

        checksum = _checksum(workdir)
        marker_uri = f"{landing_uri}/_CHECKSUM"

        import fsspec

        fs, _, _ = fsspec.get_fs_token_paths(landing_uri)
        if not force and fs.exists(marker_uri) and fs.cat_file(marker_uri).decode().strip() == checksum:
            log.info(
                "dataset unchanged, skipping upload",
                extra={"extra_fields": {"dataset": dataset_key, "checksum": checksum[:12]}},
            )
            return IngestResult(dataset_key, slug, [], 0, checksum, landing_uri, skipped=True)

        written = _upload(workdir, landing_uri)
        fs.pipe_file(marker_uri, checksum.encode())
        files = [p.name for p in workdir.rglob("*") if p.is_file()]
        log.info(
            "landed kaggle dataset",
            extra={
                "extra_fields": {
                    "dataset": dataset_key,
                    "files": len(files),
                    "bytes": written,
                    "uri": landing_uri,
                }
            },
        )
        return IngestResult(dataset_key, slug, files, written, checksum, landing_uri)
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
