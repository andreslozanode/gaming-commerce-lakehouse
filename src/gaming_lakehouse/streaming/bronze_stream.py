"""Event-driven Bronze: Pub/Sub (GCP) or Event Hubs Kafka endpoint (Azure) -> Delta.

Exactly-once is achieved through the checkpoint + idempotent Delta writes; late events are
handled with a watermark and dropped duplicates keyed on event_id.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from gaming_lakehouse.config import load_settings
from gaming_lakehouse.logging_utils import get_logger
from gaming_lakehouse.spark import build_spark
from gaming_lakehouse.storage import checkpoint_path
from gaming_lakehouse.streaming.event_schemas import PURCHASE_EVENT_V1

log = get_logger(__name__)


def read_events(spark: SparkSession) -> DataFrame:
    settings = load_settings()
    topic = settings.topic("purchase_events")

    if settings.broker == "pubsub":
        raw = (
            spark.readStream.format("pubsub")
            .option("subscriptionId", f"{topic}-sub")
            .option("projectId", settings.get("gcp.project_id", "${GCP_PROJECT_ID}"))
            .option("maxBytesPerTrigger", settings.get("spark.streaming.max_bytes_per_trigger", "512m"))
            .load()
            .selectExpr("CAST(payload AS STRING) AS value", "publishTimestampInMillis AS broker_ts")
        )
    else:  # Azure Event Hubs speaks Kafka
        from gaming_lakehouse.secrets import get_secret

        bootstrap = settings.get("streaming.kafka_bootstrap")
        conn = get_secret("eventhubs-connection-string")
        jaas = (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="$ConnectionString" password="{conn}";'
        )
        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("subscribe", topic)
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "PLAIN")
            .option("kafka.sasl.jaas.config", jaas)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", 500_000)  # bounded micro-batches => predictable latency
            .option("failOnDataLoss", "false")
            .load()
            .selectExpr("CAST(value AS STRING) AS value", "timestamp AS broker_ts")
        )

    return (
        raw.select(F.from_json("value", PURCHASE_EVENT_V1).alias("e"), "broker_ts")
        .select("e.*", "broker_ts")
        .withWatermark("occurred_at", "15 minutes")
        .dropDuplicatesWithinWatermark(["event_id"])  # exactly-once at the semantic level
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("event_date", F.to_date("occurred_at"))
    )


def main() -> None:
    settings = load_settings()
    spark = build_spark("bronze-purchase-events", streaming=True)
    target = settings.table("bronze", "bronze_purchase_events")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target} (
            event_id STRING, event_type STRING, occurred_at TIMESTAMP, player_id STRING,
            platform STRING, channel_code STRING, product_id STRING, product_type STRING,
            title STRING, quantity INT, unit_price DOUBLE, discount_pct DOUBLE, currency STRING,
            country STRING, is_preowned BOOLEAN, membership_tier STRING, producer_version STRING,
            broker_ts TIMESTAMP, _ingested_at TIMESTAMP, event_date DATE
        ) USING DELTA
        CLUSTER BY (event_date, platform)
        TBLPROPERTIES (delta.enableChangeDataFeed = true, delta.enableDeletionVectors = true)
    """)

    query = (
        read_events(spark)
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path("bronze_purchase_events"))
        .option("mergeSchema", "false")
        .queryName("bronze_purchase_events")
        .trigger(processingTime=settings.get("spark.streaming.trigger_interval", "30 seconds"))
        .toTable(target)
    )
    log.info("streaming bronze started", extra={"extra_fields": {"table": target, "broker": settings.broker}})
    query.awaitTermination()


if __name__ == "__main__":
    main()
