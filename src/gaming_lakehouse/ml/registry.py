"""MLflow tracking + registry. Databricks-managed on qa/prod, local file store on dev.

Promotion is a CI action, never a notebook click:
    dev  -> logged run only
    qa   -> registered version, alias @challenger, shadow-scored for 24h
    prod -> alias @champion, promoted by the cd-prod pipeline after the gate passes
"""

from __future__ import annotations

from typing import Any

import mlflow

from gaming_lakehouse.config import load_settings
from gaming_lakehouse.logging_utils import get_logger

log = get_logger(__name__)


def _tracking_uri() -> str:
    settings = load_settings()
    if settings.environment == "dev":
        return "file:./mlruns"
    return "databricks"  # Databricks-managed MLflow, workspace resolved by the CLI profile


def log_and_register(model: Any, *, name: str, flavor: str, signature_input: Any = None) -> str:
    settings = load_settings()
    mlflow.set_tracking_uri(_tracking_uri())
    registered_name = f"{settings.short_project[:20]}_{settings.environment}.models.{name}"

    if flavor == "pytorch":
        mlflow.pytorch.log_model(
            model,
            artifact_path="model",
            registered_model_name=None if settings.environment == "dev" else registered_name,
        )
    elif flavor == "tensorflow":
        mlflow.tensorflow.log_model(
            model,
            artifact_path="model",
            registered_model_name=None if settings.environment == "dev" else registered_name,
        )
    else:
        raise ValueError(f"unsupported flavor {flavor}")

    if settings.environment == "dev":
        log.info("model logged (dev: not registered)", extra={"extra_fields": {"name": name}})
        return ""

    client = mlflow.MlflowClient()
    version = client.get_latest_versions(registered_name)[0].version
    alias = "champion" if settings.is_prod else "challenger"
    client.set_registered_model_alias(registered_name, alias, version)
    log.info(
        "model registered",
        extra={"extra_fields": {"name": registered_name, "version": version, "alias": alias}},
    )
    return str(version)


def load_champion(name: str):
    settings = load_settings()
    mlflow.set_tracking_uri(_tracking_uri())
    uri = f"models:/{settings.short_project[:20]}_{settings.environment}.models.{name}@champion"
    return mlflow.pyfunc.load_model(uri)
