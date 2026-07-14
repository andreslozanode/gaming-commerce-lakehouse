"""Alerting callbacks. Cloud-aware sink: Cloud Monitoring/Chat on GCP, Azure Monitor/Teams on Azure."""

from __future__ import annotations

import os
from typing import Any

import requests


def _post(payload: dict) -> None:
    webhook = os.getenv("ALERT_WEBHOOK_URL")
    if not webhook:
        return
    if not webhook.startswith("https://"):
        raise ValueError("ALERT_WEBHOOK_URL must be an https endpoint")
    requests.post(webhook, json=payload, timeout=10).raise_for_status()


def on_failure_alert(context: dict[str, Any]) -> None:
    ti = context["task_instance"]
    _post(
        {
            "severity": "ERROR",
            "text": (
                f"[{os.getenv('ENVIRONMENT', 'dev')}/{os.getenv('CLOUD', 'gcp')}] "
                f"DAG {ti.dag_id} task {ti.task_id} failed "
                f"(try {ti.try_number}) — {context.get('exception')}"
            ),
            "log_url": ti.log_url,
        }
    )


def on_sla_miss(dag: Any, task_list: Any, blocking_task_list: Any, slas: Any, blocking_tis: Any) -> None:
    _post({"severity": "WARNING", "text": f"SLA miss on {dag.dag_id}: {[s.task_id for s in slas]}"})
