"""Batch scoring: churn + next-best-title per player, written to Gold for BI and the API.

Uses a Pandas UDF so the model is deserialized once per executor, not once per row — the
single most common performance mistake in Spark inference.
"""

from __future__ import annotations

import pandas as pd
from pyspark.sql import functions as F

from gaming_lakehouse.config import load_settings
from gaming_lakehouse.logging_utils import get_logger
from gaming_lakehouse.ml.registry import load_champion
from gaming_lakehouse.spark import build_spark

log = get_logger(__name__)


def main() -> None:
    s = load_settings()
    spark = build_spark("batch-inference")
    features = spark.table(s.table("gold", "feat_player"))

    model = load_champion("gc_two_tower_recsys")
    broadcast_model = spark.sparkContext.broadcast(model)

    # The pyspark stubs do not model the decorator-only pandas_udf form; runtime is fine.
    @F.pandas_udf("double")  # type: ignore[call-overload]
    def churn_score(recency: pd.Series, tenure: pd.Series, log_ltv: pd.Series) -> pd.Series:
        frame = pd.DataFrame({"recency_days": recency, "tenure_days": tenure, "log_ltv": log_ltv})
        return pd.Series(broadcast_model.value.predict(frame)).astype("float64")

    scored = (
        features.withColumn("churn_probability", churn_score("recency_days", "tenure_days", "log_ltv"))
        .withColumn(
            "risk_band",
            F.when(F.col("churn_probability") > 0.7, "HIGH")
            .when(F.col("churn_probability") > 0.4, "MEDIUM")
            .otherwise("LOW"),
        )
        .withColumn("scored_at", F.current_timestamp())
    )
    table = s.table("gold", "gold_player_churn_scores")
    scored.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    spark.sql(f"ALTER TABLE {table} CLUSTER BY (risk_band)")
    log.info("batch inference complete", extra={"extra_fields": {"table": table}})


if __name__ == "__main__":
    main()
