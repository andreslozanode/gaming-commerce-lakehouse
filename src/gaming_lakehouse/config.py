"""Single source of truth for the CLOUD x ENVIRONMENT toggle.

Everything downstream (Spark, storage paths, secrets, streaming brokers, LLM providers,
Terraform outputs) is derived from exactly two variables:

    CLOUD        gcp | azure
    ENVIRONMENT  dev | qa | prod

Resolution order (last wins): conf/base.yaml -> conf/cloud/<CLOUD>.yaml -> conf/env/<ENV>.yaml -> env vars.
No other module is allowed to read os.environ for these concerns.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

import yaml

Cloud = Literal["gcp", "azure"]
Environment = Literal["dev", "qa", "prod"]

CONF_DIR = Path(os.getenv("GC_CONF_DIR", Path(__file__).resolve().parents[2] / "conf"))


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def _load(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing config file: {path}")
    return yaml.safe_load(path.read_text()) or {}


@dataclass(frozen=True)
class Settings:
    cloud: Cloud
    environment: Environment
    raw: dict[str, Any] = field(repr=False, default_factory=dict)

    # ---------- generic accessor ----------
    def get_str(self, dotted: str, default: str | None = None) -> str:
        """Typed accessor for keys that must resolve to a string."""
        value = self.get(dotted, default)
        if value is None:
            raise KeyError(f"Missing required config key: {dotted}")
        return str(value)

    def get(self, dotted: str, default: Any = None) -> Any:
        node: Any = self.raw
        for part in dotted.split("."):
            if not isinstance(node, dict) or part not in node:
                return default
            node = node[part]
        return node

    # ---------- naming ----------
    @property
    def project(self) -> str:
        return self.get_str("project.name", "gaming-commerce-lakehouse")

    @property
    def short_project(self) -> str:
        return self.project.replace("-", "")

    # ---------- storage ----------
    def layer_uri(self, layer: str, *parts: str) -> str:
        """Return the fully-qualified URI for a medallion layer, cloud-agnostic.

        gcp   -> gs://gaming-commerce-lakehouse-prod-bronze/...
        azure -> abfss://gamingcommercelakehouseprodbronze@gclhprodls.dfs.core.windows.net/...
        """
        suffix = "/".join(p.strip("/") for p in parts if p)
        if self.cloud == "gcp":
            bucket = f"{self.project}-{self.environment}-{layer}"
            return f"gs://{bucket}/{suffix}".rstrip("/")
        account = f"{self.short_project[:16]}{self.environment}dls"
        container = f"{self.short_project[:16]}{self.environment}{layer}"
        return f"abfss://{container}@{account}.dfs.core.windows.net/{suffix}".rstrip("/")

    def table(self, layer: str, name: str) -> str:
        """Unity Catalog three-level name: <catalog>.<schema>.<table>."""
        catalog = f"{self.get('catalog.name', self.short_project[:20])}_{self.environment}"
        schema = self.get(f"catalog.{layer}_schema", layer)
        return f"{catalog}.{schema}.{name}"

    # ---------- streaming ----------
    @property
    def broker(self) -> str:
        return self.get_str("streaming.broker")

    def topic(self, logical: str) -> str:
        return self.get_str(f"streaming.topics.{logical}")

    # ---------- quality / cost ----------
    @property
    def on_violation(self) -> str:
        return self.get_str("quality.on_violation", "warn")

    @property
    def is_prod(self) -> bool:
        return self.environment == "prod"


@lru_cache(maxsize=8)
def load_settings(cloud: str | None = None, environment: str | None = None) -> Settings:
    # `x if x else y` instead of `x or y`: mypy>=2 stopped narrowing the or-join,
    # and the ternary keeps the same semantics (empty string counts as unset).
    cloud = (cloud if cloud else os.getenv("CLOUD", "gcp")).lower()
    environment = (environment if environment else os.getenv("ENVIRONMENT", "dev")).lower()
    if cloud not in ("gcp", "azure"):
        raise ValueError(f"CLOUD must be gcp|azure, got {cloud!r}")
    if environment not in ("dev", "qa", "prod"):
        raise ValueError(f"ENVIRONMENT must be dev|qa|prod, got {environment!r}")

    merged = _load(CONF_DIR / "base.yaml")
    merged = _deep_merge(merged, _load(CONF_DIR / "cloud" / f"{cloud}.yaml"))
    merged = _deep_merge(merged, _load(CONF_DIR / "env" / f"{environment}.yaml"))
    merged = _deep_merge(merged, {"datasets": _load(CONF_DIR / "datasets.yaml").get("datasets", {})})
    merged = _deep_merge(merged, {"oltp": _load(CONF_DIR / "datasets.yaml").get("oltp", {})})
    return Settings(cloud=cloud, environment=environment, raw=merged)  # type: ignore[arg-type]
