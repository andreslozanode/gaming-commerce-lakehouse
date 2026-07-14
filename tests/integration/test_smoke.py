"""Post-deployment smoke tests. Run by both CD pipelines after every environment deploy."""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.smoke


def test_config_resolves_for_the_deployed_target():
    from gaming_lakehouse.config import load_settings

    s = load_settings(os.getenv("CLOUD", "gcp"), os.getenv("ENVIRONMENT", "dev"))
    assert s.layer_uri("gold")
    assert s.broker in ("pubsub", "eventhubs")
    assert s.table("gold", "gold_player_360").count(".") == 2


@pytest.mark.skipif(not os.getenv("DATABRICKS_HOST"), reason="no workspace configured")
def test_gold_marts_exist_and_are_fresh():
    from databricks.sdk import WorkspaceClient

    from gaming_lakehouse.config import load_settings

    s = load_settings()
    client = WorkspaceClient()
    warehouse = next(iter(client.warehouses.list()))
    for mart in ("gold_sales_by_era_platform", "gold_player_360", "gold_membership_mrr"):
        table = s.table("gold", mart)
        result = client.statement_execution.execute_statement(
            warehouse_id=warehouse.id,
            statement=f"SELECT count(*) FROM {table} WHERE _generated_at > current_timestamp() - INTERVAL 2 DAYS",
        )
        assert int(result.result.data_array[0][0]) > 0, f"{table} is stale or empty"
