"""Path helpers so no job ever hardcodes gs:// or abfss://."""

from __future__ import annotations

from datetime import date

from gaming_lakehouse.config import load_settings


def landing_path(source: str, dataset: str, ingest_date: date | None = None) -> str:
    settings = load_settings()
    day = (ingest_date or date.today()).isoformat()
    return settings.layer_uri("landing", source, dataset, f"ingest_date={day}")


def checkpoint_path(job: str) -> str:
    return load_settings().layer_uri("bronze", "_checkpoints", job)


def schema_path(job: str) -> str:
    return load_settings().layer_uri("bronze", "_schemas", job)
