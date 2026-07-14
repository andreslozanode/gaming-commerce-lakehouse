"""Real-time CDC: Debezium envelope -> Silver Delta with SCD Type 2 history.

Design decisions worth calling out:
  * No ExtractNewRecordState SMT. The full envelope (before/after/op/source) reaches Spark, so
    deletes and out-of-order replays are resolvable without a second source of truth.
  * foreachBatch + MERGE, because a streaming sink cannot express upsert semantics.
  * Deduplication inside the batch on (pk, source.lsn) before the MERGE — Delta rejects a MERGE
    that matches the same target row twice, and Debezium *will* deliver repeats after a restart.
  * Idempotent by construction: replaying a micro-batch produces the same Silver state.
"""

from __future__ import annotations

import argparse
from typing import cast

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from gaming_lakehouse.config import load_settings
from gaming_lakehouse.logging_utils import get_logger
from gaming_lakehouse.spark import build_spark
from gaming_lakehouse.storage import checkpoint_path

log = get_logger(__name__)

SOURCE_META = StructType(
    [
        StructField("ts_ms", LongType(), True),
        StructField("lsn", LongType(), True),
        StructField("txId", LongType(), True),
        StructField("table", StringType(), True),
        StructField("snapshot", StringType(), True),
    ]
)


def debezium_envelope(payload_schema: StructType) -> StructType:
    return StructType(
        [
            StructField("before", payload_schema, True),
            StructField("after", payload_schema, True),
            StructField("op", StringType(), True),  # c=create u=update d=delete r=snapshot
            StructField("ts_ms", LongType(), True),
            StructField("source", SOURCE_META, True),
        ]
    )


def read_cdc_stream(spark: SparkSession, topic: str, payload_schema: StructType) -> DataFrame:
    settings = load_settings()
    envelope = debezium_envelope(payload_schema)

    if settings.broker == "pubsub":
        raw = (
            spark.readStream.format("pubsub")
            .option("subscriptionId", f"{topic}-sub")
            .option("projectId", settings.get("gcp.project_id", ""))
            .load()
            .selectExpr("CAST(payload AS STRING) AS value")
        )
    else:
        from gaming_lakehouse.secrets import get_secret

        jaas = (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="$ConnectionString" password="{get_secret("eventhubs-connection-string")}";'
        )
        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", settings.get("streaming.kafka_bootstrap"))
            .option("subscribe", topic)
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "PLAIN")
            .option("kafka.sasl.jaas.config", jaas)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", 200_000)
            .load()
            .selectExpr("CAST(value AS STRING) AS value")
        )

    return raw.select(F.from_json("value", envelope).alias("d")).select("d.*")


def upsert_scd2(batch: DataFrame, batch_id: int, target_table: str, primary_key: str) -> None:
    """MERGE one micro-batch into Silver, maintaining is_current / valid_from / valid_to."""
    spark = batch.sparkSession
    if batch.isEmpty():
        return

    flat = (
        batch.withColumn("_pk", F.coalesce(F.col(f"after.{primary_key}"), F.col(f"before.{primary_key}")))
        .withColumn("_lsn", F.col("source.lsn"))
        .withColumn("_op", F.col("op"))
        .withColumn("_committed_at", (F.col("ts_ms") / 1000).cast("timestamp"))
    )

    # Last-writer-wins within the batch, ordered by the Postgres LSN (monotonic, not wall-clock).
    window_spec = F.row_number().over(
        __import__("pyspark.sql.window", fromlist=["Window"])
        .Window.partitionBy("_pk")
        .orderBy(F.col("_lsn").desc_nulls_last())
    )
    deduped = flat.withColumn("_rn", window_spec).filter("_rn = 1").drop("_rn")

    after_struct = cast(StructType, deduped.schema["after"].dataType)
    payload_cols = [f.name for f in after_struct.fields]
    staged = (
        deduped.select(
            "_pk",
            "_op",
            "_lsn",
            "_committed_at",
            *[F.col(f"after.{c}").alias(c) for c in payload_cols],
        )
        .withColumn("is_deleted", F.col("_op") == F.lit("d"))
        .withColumn("valid_from", F.col("_committed_at"))
        .withColumn("valid_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
    )

    delta_table = DeltaTable.forName(spark, target_table)

    # 1) Close the currently-open version of every changed key.
    (
        delta_table.alias("t")
        .merge(staged.alias("s"), f"t.{primary_key} = s._pk AND t.is_current = true AND t._lsn < s._lsn")
        .whenMatchedUpdate(set={"is_current": F.lit(False), "valid_to": F.col("s.valid_from")})
        .execute()
    )
    # 2) Insert the new open version (deletes land as tombstones, keeping the audit trail).
    (
        delta_table.alias("t")
        .merge(staged.alias("s"), f"t.{primary_key} = s._pk AND t._lsn = s._lsn")
        .whenNotMatchedInsertAll()
        .execute()
    )
    log.info(
        "cdc batch merged",
        extra={"extra_fields": {"batch_id": batch_id, "table": target_table, "rows": staged.count()}},
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--entity", required=True, choices=["orders", "subscriptions", "customers"])
    args = parser.parse_args()

    settings = load_settings()
    spark = build_spark(f"cdc-{args.entity}", streaming=True)

    schemas = {
        "orders": StructType(
            [
                StructField("order_id", StringType(), False),
                StructField("player_id", StringType(), True),
                StructField("channel_code", StringType(), True),
                StructField("platform", StringType(), True),
                StructField("order_status", StringType(), True),
                StructField("order_total", StringType(), True),
                StructField("currency", StringType(), True),
                StructField("created_at", StringType(), True),
                StructField("updated_at", StringType(), True),
            ]
        ),
        "subscriptions": StructType(
            [
                StructField("subscription_id", StringType(), False),
                StructField("player_id", StringType(), True),
                StructField("membership_tier", StringType(), True),
                StructField("status", StringType(), True),  # ACTIVE | CANCELLED | LAPSED
                StructField("started_at", StringType(), True),
                StructField("renews_at", StringType(), True),
                StructField("mrr", StringType(), True),
            ]
        ),
        "customers": StructType(
            [
                StructField("player_id", StringType(), False),
                StructField("country", StringType(), True),
                StructField("signup_at", StringType(), True),
                StructField("primary_platform", StringType(), True),
            ]
        ),
    }
    pk = {"orders": "order_id", "subscriptions": "subscription_id", "customers": "player_id"}[args.entity]
    topic = settings.topic(f"cdc_{args.entity}") or f"gc-cdc-{args.entity}"
    target = settings.table("silver", f"silver_{args.entity}_history")

    stream = read_cdc_stream(spark, topic, schemas[args.entity])
    query = (
        stream.writeStream.foreachBatch(lambda df, bid: upsert_scd2(df, bid, target, pk))
        .option("checkpointLocation", checkpoint_path(f"cdc_{args.entity}"))
        .trigger(processingTime=settings.get("spark.streaming.trigger_interval", "30 seconds"))
        .queryName(f"cdc_{args.entity}")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
