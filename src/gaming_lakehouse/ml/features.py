"""Feature engineering shared by PyTorch (recsys/churn) and TensorFlow (forecast).

Written once, in Spark, and materialized to a Delta feature table so training and batch
inference read *the same* rows — the cheapest way to avoid training/serving skew.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from gaming_lakehouse.config import load_settings
from gaming_lakehouse.spark import build_spark


def player_features(spark: SparkSession) -> DataFrame:
    s = load_settings()
    p360 = spark.table(s.table("gold", "gold_player_360"))
    return (
        p360.select(
            "player_id",
            "recency_days",
            "tenure_days",
            "purchase_count",
            "lifetime_value",
            "avg_order_value",
            "digital_share",
            "platforms_used",
            "membership_tier",
            "membership_status",
            "mrr",
            "is_churned",
        )
        .fillna({"digital_share": 0.0, "mrr": 0.0, "membership_tier": "NONE", "membership_status": "NONE"})
        # log1p on the heavy tails: LTV and purchase counts are power-law distributed
        .withColumn("log_ltv", F.log1p("lifetime_value"))
        .withColumn("log_purchases", F.log1p("purchase_count"))
    )


def interaction_features(spark: SparkSession) -> DataFrame:
    """Implicit-feedback matrix for the two-tower recommender (player x title)."""
    s = load_settings()
    events = spark.table(s.table("silver", "silver_purchase_events"))
    interactions = (
        events.filter(F.col("product_type") == "GAME")
        .groupBy("player_id", "product_id", "title", "platform_code", "console_family")
        .agg(
            F.count("*").alias("purchases"),
            F.sum("net_revenue").alias("spend"),
            F.max("occurred_at").alias("last_seen"),
        )
        .withColumn("confidence", F.log1p(F.col("purchases")) + F.log1p(F.col("spend") / 10.0))
    )
    # Chronological split index -> prevents leakage in evaluation (no random shuffling of time).
    w = Window.partitionBy("player_id").orderBy(F.col("last_seen").desc())
    return interactions.withColumn("_rank_desc", F.row_number().over(w))


def sales_timeseries(spark: SparkSession) -> DataFrame:
    """Daily revenue per platform x channel for the TF forecaster, with calendar + lag features."""
    s = load_settings()
    events = spark.table(s.table("silver", "silver_purchase_events"))
    daily = events.groupBy(F.to_date("occurred_at").alias("ds"), "platform_code", "channel_type").agg(
        F.sum("net_revenue").alias("revenue"), F.count("*").alias("transactions")
    )
    w = Window.partitionBy("platform_code", "channel_type").orderBy("ds")
    for lag in (1, 7, 28):
        daily = daily.withColumn(f"revenue_lag_{lag}", F.lag("revenue", lag).over(w))
    return (
        daily.withColumn("revenue_roll_7", F.avg("revenue").over(w.rowsBetween(-6, 0)))
        .withColumn("revenue_roll_28", F.avg("revenue").over(w.rowsBetween(-27, 0)))
        .withColumn("dow", F.dayofweek("ds"))
        .withColumn("month", F.month("ds"))
        .withColumn("is_holiday_season", F.month("ds").isin(11, 12).cast("int"))
        .na.drop(subset=["revenue_lag_28"])
    )


def materialize() -> None:
    s = load_settings()
    spark = build_spark("feature-engineering")
    for name, builder in (
        ("feat_player", player_features),
        ("feat_interactions", interaction_features),
        ("feat_sales_ts", sales_timeseries),
    ):
        table = s.table("gold", name)
        (
            builder(spark)
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table)
        )


if __name__ == "__main__":
    materialize()
