"""The CDC MERGE is the riskiest code in the repo — it gets the most tests."""

from __future__ import annotations

import pytest
from pyspark.sql import Row

from gaming_lakehouse.cdc.cdc_merge_silver import upsert_scd2


@pytest.fixture(autouse=True)
def _require_delta(delta_spark):
    """Every test in this module exercises Delta (MERGE / managed tables)."""


@pytest.fixture
def target(spark, tmp_path):
    spark.sql("CREATE DATABASE IF NOT EXISTS test_cdc")
    spark.sql("DROP TABLE IF EXISTS test_cdc.orders_history")
    spark.sql("""
        CREATE TABLE test_cdc.orders_history (
            order_id STRING, player_id STRING, order_status STRING,
            _pk STRING, _op STRING, _lsn LONG, _committed_at TIMESTAMP,
            is_deleted BOOLEAN, valid_from TIMESTAMP, valid_to TIMESTAMP, is_current BOOLEAN
        ) USING DELTA
    """)
    return "test_cdc.orders_history"


def _envelope(spark, order_id: str, status: str, op: str, lsn: int):
    return spark.createDataFrame(
        [
            Row(
                before=None,
                after=Row(order_id=order_id, player_id="P1", order_status=status),
                op=op,
                ts_ms=1_700_000_000_000 + lsn,
                source=Row(ts_ms=1_700_000_000_000 + lsn, lsn=lsn, txId=1, table="orders", snapshot="false"),
            )
        ]
    )


def test_insert_then_update_closes_the_previous_version(spark, target):
    upsert_scd2(_envelope(spark, "O1", "PLACED", "c", 100), 0, target, "order_id")
    upsert_scd2(_envelope(spark, "O1", "SHIPPED", "u", 200), 1, target, "order_id")

    rows = spark.table(target).orderBy("_lsn").collect()
    assert len(rows) == 2
    assert rows[0].order_status == "PLACED" and rows[0].is_current is False
    assert rows[0].valid_to is not None
    assert rows[1].order_status == "SHIPPED" and rows[1].is_current is True


def test_replay_of_the_same_lsn_is_idempotent(spark, target):
    batch = _envelope(spark, "O2", "PLACED", "c", 300)
    upsert_scd2(batch, 0, target, "order_id")
    upsert_scd2(batch, 0, target, "order_id")  # same micro-batch replayed after a restart
    assert spark.table(target).filter("order_id = 'O2'").count() == 1


def test_delete_is_a_tombstone_not_a_physical_delete(spark, target):
    upsert_scd2(_envelope(spark, "O3", "PLACED", "c", 400), 0, target, "order_id")
    upsert_scd2(_envelope(spark, "O3", "CANCELLED", "d", 500), 1, target, "order_id")
    current = spark.table(target).filter("order_id = 'O3' AND is_current").collect()
    assert len(current) == 1 and current[0].is_deleted is True
