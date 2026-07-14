"""Cloud-aware secret resolver: GCP Secret Manager or Azure Key Vault, same call site.

    from gaming_lakehouse.secrets import get_secret
    get_secret("kaggle-api-token")

Local development falls back to environment variables (UPPER_SNAKE_CASE) so nothing
sensitive is ever committed. Databricks jobs prefer dbutils secret scopes when available.
"""

from __future__ import annotations

import os
from functools import lru_cache

from gaming_lakehouse.config import load_settings
from gaming_lakehouse.logging_utils import get_logger

log = get_logger(__name__)


def _from_env(name: str) -> str | None:
    return os.getenv(name.upper().replace("-", "_"))


def _from_dbutils(scope: str, key: str) -> str | None:
    try:  # pragma: no cover - only inside Databricks
        from pyspark.dbutils import DBUtils  # type: ignore
        from pyspark.sql import SparkSession

        return str(DBUtils(SparkSession.getActiveSession()).secrets.get(scope=scope, key=key))
    except Exception:
        return None


@lru_cache(maxsize=64)
def get_secret(name: str, *, required: bool = True) -> str | None:
    settings = load_settings()
    scope = f"{settings.project}-{settings.environment}"

    value = _from_dbutils(scope, name) or _from_env(name)
    if value:
        return value

    backend = settings.get("secrets.backend")
    try:
        if backend == "secret_manager":
            from google.cloud import secretmanager  # type: ignore

            client = secretmanager.SecretManagerServiceClient()
            project_id = os.environ["GCP_PROJECT_ID"]
            path = f"projects/{project_id}/secrets/{name}-{settings.environment}/versions/latest"
            value = client.access_secret_version(name=path).payload.data.decode()
        elif backend == "key_vault":
            from azure.identity import DefaultAzureCredential  # type: ignore
            from azure.keyvault.secrets import SecretClient  # type: ignore

            vault = os.environ["AZURE_KEY_VAULT_URI"]
            client = SecretClient(vault_url=vault, credential=DefaultAzureCredential())
            value = client.get_secret(f"{name}-{settings.environment}").value
    except Exception as exc:
        log.warning("secret backend %s failed for %s: %s", backend, name, exc)

    if not value and required:
        raise RuntimeError(f"Secret {name!r} not resolvable via {backend}, dbutils or env")
    return value
