"""Bronze -> Silver: conform, deduplicate, type-cast, enforce quality, unify the three purchase
channels (physical / digital / membership) plus console hardware into one conformed event grain.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from gaming_lakehouse.config import load_settings
from gaming_lakehouse.ingestion.reference_dims import build_dim_console, build_dim_purchase_channel
from gaming_lakehouse.logging_utils import get_logger
from gaming_lakehouse.spark import build_spark
from gaming_lakehouse.transform.quality.expectations import (
    SILVER_EVENTS_RULES,
    SILVER_SALES_RULES,
    apply_expectations,
)

log = get_logger(__name__)


def publish_reference_dims(spark: SparkSession) -> None:
    """pandas -> Arrow -> Delta. These are tiny, broadcast-friendly and read on every job."""
    settings = load_settings()
    for name, builder in (
        ("dim_console", build_dim_console),
        ("dim_purchase_channel", build_dim_purchase_channel),
    ):
        pdf = builder().astype(str)  # Arrow-safe: categories/Int8 -> string on the wire
        (
            spark.createDataFrame(pdf)
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(settings.table("silver", name))
        )
        log.info("reference dim published", extra={"extra_fields": {"table": name, "rows": len(pdf)}})


def silver_game_sales(spark: SparkSession) -> DataFrame:
    """Conform the VGChartz family (physical-era sales) into one table with era + generation."""
    settings = load_settings()
    classic = spark.table(settings.table("bronze", "bronze_vgchartz_sales"))
    ratings = spark.table(settings.table("bronze", "bronze_vgchartz_ratings"))
    dim_console = spark.table(settings.table("silver", "dim_console"))

    base = (
        classic.select(
            F.trim(F.col("Name")).alias("title"),
            F.upper(F.trim(F.col("Platform"))).alias("platform_code"),
            F.col("Year").cast("int").alias("release_year"),
            F.col("Genre").alias("genre"),
            F.col("Publisher").alias("publisher"),
            F.col("NA_Sales").cast("double").alias("na_sales"),
            F.col("EU_Sales").cast("double").alias("eu_sales"),
            F.col("JP_Sales").cast("double").alias("jp_sales"),
            F.col("Other_Sales").cast("double").alias("other_sales"),
            F.col("Global_Sales").cast("double").alias("global_sales_musd"),
        ).filter(F.col("release_year") >= 1990)  # scope: 90s / 00s / 10s
    )

    # Dedup on the natural key; VGChartz re-lists regional SKUs of the same title.
    natural_key = ["title", "platform_code", "release_year"]
    base = (
        base.withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy(*natural_key).orderBy(F.col("global_sales_musd").desc_nulls_last())
            ),
        )
        .filter("_rn = 1")
        .drop("_rn")
    )

    scores = ratings.select(
        F.trim(F.col("Name")).alias("title"),
        F.upper(F.trim(F.col("Platform"))).alias("platform_code"),
        F.col("Critic_Score").cast("double").alias("critic_score"),
        F.col("User_Score").cast("double").alias("user_score"),
        F.col("Rating").alias("esrb_rating"),
    ).dropDuplicates(["title", "platform_code"])

    enriched = (
        base.join(F.broadcast(scores), ["title", "platform_code"], "left")  # small side -> broadcast
        .join(F.broadcast(dim_console), ["platform_code"], "left")
        .withColumn(
            "era",
            F.coalesce(
                F.col("era"),
                F.expr("""
                CASE WHEN release_year < 2000 THEN '90s'
                     WHEN release_year < 2010 THEN '00s'
                     WHEN release_year < 2020 THEN '10s' ELSE '20s' END"""),
            ),
        )
        .filter(F.col("console_family").isin("PlayStation", "Xbox"))  # PS + Xbox scope
    )
    return apply_expectations(
        enriched,
        SILVER_SALES_RULES,
        quarantine_table=load_settings().table("silver", "quarantine_game_sales"),
    )


def silver_purchase_events(spark: SparkSession) -> DataFrame:
    """Conform the real-time purchase stream. Reads Bronze *incrementally* via Change Data Feed."""
    settings = load_settings()
    bronze = settings.table("bronze", "bronze_purchase_events")

    df = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", _last_processed_version(spark, bronze))
        .table(bronze)
        .filter(F.col("_change_type").isin("insert", "update_postimage"))
    )

    dim_channel = spark.table(settings.table("silver", "dim_purchase_channel"))
    conformed = (
        df.withColumn(
            "net_revenue",
            F.round(
                F.col("unit_price") * F.col("quantity") * (1 - F.coalesce(F.col("discount_pct"), F.lit(0.0))),
                4,
            ),
        )
        .withColumn("platform_code", F.upper(F.col("platform")))
        .join(F.broadcast(dim_channel), ["channel_code"], "left")
        .withColumn("is_membership", F.col("channel_type") == "MEMBERSHIP")
        .withColumn("is_physical", F.col("channel_type") == "PHYSICAL")
        .drop("_change_type", "_commit_version", "_commit_timestamp")
    )
    return apply_expectations(
        conformed,
        SILVER_EVENTS_RULES,
        quarantine_table=settings.table("silver", "quarantine_purchase_events"),
    )


def _last_processed_version(spark: SparkSession, table: str) -> int:
    """CDF watermark stored in a control table -> incremental, restartable, no full rescan."""
    settings = load_settings()
    control = settings.table("silver", "_cdf_watermarks")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {control} (table_name STRING, last_version LONG) USING DELTA")
    row = spark.table(control).filter(F.col("table_name") == table).select("last_version").head()
    return int(row["last_version"]) + 1 if row else 0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="all", choices=["all", "dims", "sales", "events"])
    args = parser.parse_args()

    settings = load_settings()
    spark = build_spark("bronze-to-silver")

    if args.target in ("all", "dims"):
        publish_reference_dims(spark)

    if args.target in ("all", "sales"):
        table = settings.table("silver", "silver_game_sales")
        (
            silver_game_sales(spark)
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table)
        )
        spark.sql(f"ALTER TABLE {table} CLUSTER BY (era, console_family, platform_code)")
        log.info("silver_game_sales written", extra={"extra_fields": {"table": table}})

    if args.target in ("all", "events"):
        table = settings.table("silver", "silver_purchase_events")
        (
            silver_purchase_events(spark)
            .write.format("delta")
            .mode("append")
            .option("mergeSchema", "false")
            .saveAsTable(table)
        )
        log.info("silver_purchase_events appended", extra={"extra_fields": {"table": table}})


if __name__ == "__main__":
    main()
