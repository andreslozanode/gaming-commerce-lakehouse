"""Apply orchestration/airbyte/connections.yaml through the Airbyte Configuration API.

GitOps for Airbyte: the YAML is the source of truth, CI applies it, nobody clicks in the UI.
Idempotent — existing sources/destinations/connections are updated, not duplicated.
"""

from __future__ import annotations

import os
import string
import sys
from pathlib import Path

import requests
import yaml

AIRBYTE_URL = os.environ["AIRBYTE_API_URL"].rstrip("/")
TOKEN = os.environ["AIRBYTE_API_TOKEN"]
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}


def _render(raw: str) -> dict:
    return yaml.safe_load(string.Template(raw).safe_substitute(os.environ))


def _upsert(kind: str, body: dict) -> str:
    existing = requests.post(
        f"{AIRBYTE_URL}/v1/{kind}/list",
        json={"workspaceId": os.environ["AIRBYTE_WORKSPACE_ID"]},
        headers=HEADERS,
        timeout=60,
    ).json()
    match = next((x for x in existing.get("data", []) if x["name"] == body["name"]), None)
    if match:
        body["id"] = match[f"{kind[:-1]}Id"]
        requests.patch(f"{AIRBYTE_URL}/v1/{kind}/{body['id']}", json=body, headers=HEADERS, timeout=60)
        return body["id"]
    created = requests.post(f"{AIRBYTE_URL}/v1/{kind}", json=body, headers=HEADERS, timeout=60)
    created.raise_for_status()
    return created.json()[f"{kind[:-1]}Id"]


def main(path: str) -> None:
    config = _render(Path(path).read_text())
    ids = {}
    for source in config["sources"]:
        ids[source["name"]] = _upsert(
            "sources",
            {
                "name": source["name"],
                "workspaceId": os.environ["AIRBYTE_WORKSPACE_ID"],
                "definitionId": source["definition"],
                "configuration": source["config"],
            },
        )
    for destination in config["destinations"]:
        ids[destination["name"]] = _upsert(
            "destinations",
            {
                "name": destination["name"],
                "workspaceId": os.environ["AIRBYTE_WORKSPACE_ID"],
                "definitionId": destination["definition"],
                "configuration": destination["config"],
            },
        )
    for connection in config["connections"]:
        _upsert(
            "connections",
            {
                "name": connection["name"],
                "sourceId": ids[connection["source"]],
                "destinationId": ids[connection["destination"]],
                "schedule": connection["schedule"],
                "configurations": {"streams": connection["streams"]},
                "nonBreakingSchemaUpdatesBehavior": connection["non_breaking_schema_updates_behavior"],
            },
        )
    print(f"applied {len(config['connections'])} connections")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "orchestration/airbyte/connections.yaml")
