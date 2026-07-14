"""Silver -> Gold: business marts. One table per question the business actually asks.

gold_sales_by_era_platform   physical vs digital vs membership, per era, per console family
gold_console_lifecycle       units + attach rate across a console's life (PS1..PS5, Xbox..Series)
gold_player_360              per-player spend, tenure, membership state, churn label (ML label source)
gold_title_performance       title-level revenue, critic/user score, retro long-tail resale signal
gold_membership_mrr          MRR / churn / reactivation from CDC subscription history (SCD2)
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from gaming_lakehouse.config import load_settings
from gaming_lakehouse.logging_utils import get_logger
from gaming_lakehouse.spark import build_spark

log = get_logger(__name__)


def gold_sales_by_era_platform(spark: SparkSession) -> DataFrame:
    s = load_settings()
    sales = spark.table(s.table("silver", "silver_game_sales"))
    events = spark.table(s.table("silver", "silver_purchase_events"))

    physical = (
        sales.groupBy("era", "console_family", "platform_code", "genre")
        .agg(
            F.sum("global_sales_musd").alias("physical_units_m"),
            F.countDistinct("title").alias("titles_released"),
            F.avg("critic_score").alias("avg_critic_score"),
        )
        .withColumn("channel_type", F.lit("PHYSICAL"))
    )
    digital = events.groupBy("era", "console_family", "platform_code", "channel_type").agg(
        F.sum("net_revenue").alias("net_revenue"),
        F.countDistinct("player_id").alias("buyers"),
        F.count("*").alias("transactions"),
    )
    return physical.join(
        digital, ["era", "console_family", "platform_code", "channel_type"], "full_outer"
    ).withColumn("_generated_at", F.current_timestamp())


def gold_player_360(spark: SparkSession) -> DataFrame:
    """One row per player: spend, recency/frequency/monetary, membership state, churn label.

    The 90-day inactivity churn label is what train_torch_recsys.py and the TF forecaster consume.
    """
    s = load_settings()
    events = spark.table(s.table("silver", "silver_purchase_events"))
    subs = spark.table(s.table("silver", "silver_subscriptions_history")).filter(
        F.col("is_current") & ~F.col("is_deleted")
    )

    rfm = (
        events.groupBy("player_id")
        .agg(
            F.max("occurred_at").alias("last_purchase_at"),
            F.min("occurred_at").alias("first_purchase_at"),
            F.count("*").alias("purchase_count"),
            F.sum("net_revenue").alias("lifetime_value"),
            F.avg("net_revenue").alias("avg_order_value"),
            F.sum(F.when(F.col("is_physical"), F.col("net_revenue")).otherwise(0)).alias("physical_spend"),
            F.sum(F.when(F.col("channel_type") == "DIGITAL", F.col("net_revenue")).otherwise(0)).alias(
                "digital_spend"
            ),
            F.sum(F.when(F.col("is_membership"), F.col("net_revenue")).otherwise(0)).alias(
                "membership_spend"
            ),
            F.countDistinct("platform_code").alias("platforms_used"),
            F.collect_set("console_family").alias("console_families"),
        )
        .withColumn("recency_days", F.datediff(F.current_date(), F.to_date("last_purchase_at")))
        .withColumn("tenure_days", F.datediff(F.current_date(), F.to_date("first_purchase_at")))
        .withColumn("digital_share", F.col("digital_spend") / F.nullif(F.col("lifetime_value"), F.lit(0.0)))
        .withColumn("is_churned", (F.col("recency_days") > 90).cast("int"))  # ML label
    )
    return rfm.join(
        subs.select(
            F.col("player_id"),
            F.col("membership_tier"),
            F.col("status").alias("membership_status"),
            F.col("mrr").cast("double").alias("mrr"),
        ),
        ["player_id"],
        "left",
    ).withColumn("_generated_at", F.current_timestamp())


def gold_console_lifecycle(spark: SparkSession) -> DataFrame:
    s = load_settings()
    sales = spark.table(s.table("silver", "silver_game_sales"))
    window = Window.partitionBy("platform_code").orderBy("release_year")
    return (
        sales.groupBy("console_family", "platform_code", "generation", "launch_year", "release_year")
        .agg(F.sum("global_sales_musd").alias("software_units_m"), F.countDistinct("title").alias("titles"))
        .withColumn("years_since_launch", F.col("release_year") - F.col("launch_year"))
        .withColumn("cumulative_units_m", F.sum("software_units_m").over(window))
        .withColumn("attach_rate_proxy", F.col("software_units_m") / F.nullif(F.col("titles"), F.lit(0)))
        .withColumn("_generated_at", F.current_timestamp())
    )


def gold_membership_mrr(spark: SparkSession) -> DataFrame:
    """MRR waterfall straight off the SCD2 CDC history — new / expansion / churn per month."""
    s = load_settings()
    hist = spark.table(s.table("silver", "silver_subscriptions_history"))
    monthly = (
        hist.withColumn("month", F.date_trunc("month", F.col("valid_from")))
        .groupBy("month", "membership_tier")
        .agg(
            F.sum(F.when(F.col("status") == "ACTIVE", F.col("mrr").cast("double")).otherwise(0)).alias("mrr"),
            F.sum(F.when(F.col("status") == "ACTIVE", 1).otherwise(0)).alias("active_subs"),
            F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("churned_subs"),
        )
    )
    prior = Window.partitionBy("membership_tier").orderBy("month")
    return (
        monthly.withColumn("prior_mrr", F.lag("mrr").over(prior))
        .withColumn("net_new_mrr", F.col("mrr") - F.coalesce(F.col("prior_mrr"), F.lit(0.0)))
        .withColumn(
            "churn_rate",
            F.col("churned_subs") / F.nullif(F.col("active_subs") + F.col("churned_subs"), F.lit(0)),
        )
        .withColumn("_generated_at", F.current_timestamp())
    )


BUILDERS = {
    "gold_sales_by_era_platform": (gold_sales_by_era_platform, ["era", "console_family"]),
    "gold_player_360": (gold_player_360, ["is_churned", "membership_tier"]),
    "gold_console_lifecycle": (gold_console_lifecycle, ["console_family"]),
    "gold_membership_mrr": (gold_membership_mrr, ["membership_tier"]),
}


def main() -> None:
    s = load_settings()
    spark = build_spark("silver-to-gold")
    for name, (builder, cluster_cols) in BUILDERS.items():
        table = s.table("gold", name)
        (
            builder(spark)
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table)
        )
        # Liquid clustering: no partition-shape decision to regret later, and it survives skew.
        spark.sql(f"ALTER TABLE {table} CLUSTER BY ({', '.join(cluster_cols)})")
        spark.sql(f"ANALYZE TABLE {table} COMPUTE STATISTICS FOR ALL COLUMNS")
        log.info("gold table built", extra={"extra_fields": {"table": table}})


if __name__ == "__main__":
    main()
