import pytest
from pyspark.sql import Row

from gaming_lakehouse.transform.quality.expectations import Expectation, apply_expectations


@pytest.fixture(autouse=True)
def _require_delta(delta_spark):
    """Every test in this module exercises Delta (MERGE / managed tables)."""


@pytest.fixture
def events(spark):
    return spark.createDataFrame(
        [
            Row(event_id="a", unit_price=59.99, discount_pct=0.1, channel_code="digital_store"),
            Row(event_id="b", unit_price=-1.0, discount_pct=0.0, channel_code="digital_store"),  # invalid
            Row(event_id="c", unit_price=0.0, discount_pct=None, channel_code="membership_included"),
        ]
    )


def test_drop_action_removes_invalid_rows(spark, events, tmp_path, monkeypatch):
    spark.sql("CREATE DATABASE IF NOT EXISTS test_q")
    rules = [Expectation("price_positive", "unit_price > 0 OR channel_code = 'membership_included'", "drop")]
    result = apply_expectations(events, rules, quarantine_table="test_q.quarantine_events")
    ids = {r.event_id for r in result.collect()}
    assert ids == {"a", "c"}
    assert spark.table("test_q.quarantine_events").count() == 1


def test_fail_action_raises(spark, events):
    spark.sql("CREATE DATABASE IF NOT EXISTS test_q2")
    rules = [Expectation("price_positive", "unit_price > 0", "fail")]
    with pytest.raises(ValueError, match="Hard data-quality expectation breached"):
        apply_expectations(events, rules, quarantine_table="test_q2.quarantine_events")
