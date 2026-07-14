"""Contract-first event schemas. Registered in Schema Registry (Confluent on GCP, Azure Schema Registry).

A producer that cannot serialize against these schemas is rejected at the broker, so Bronze
never has to guess. Avro is the wire format; the Spark StructType below mirrors it 1:1.
"""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

PURCHASE_EVENT_V1 = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField(
            "event_type", StringType(), False
        ),  # PURCHASE | REFUND | SUBSCRIPTION_RENEWAL | CONSOLE_SALE
        StructField("occurred_at", TimestampType(), False),
        StructField("player_id", StringType(), False),
        StructField("platform", StringType(), False),  # PS5 | XS | PS4 | XOne ...
        StructField(
            "channel_code", StringType(), False
        ),  # physical_retail | digital_store | membership_* | console_hardware
        StructField("product_id", StringType(), False),
        StructField("product_type", StringType(), False),  # GAME | CONSOLE | MEMBERSHIP | DLC
        StructField("title", StringType(), True),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DoubleType(), False),
        StructField("discount_pct", DoubleType(), True),
        StructField("currency", StringType(), False),
        StructField("country", StringType(), True),
        StructField("is_preowned", BooleanType(), True),  # retro/second-hand physical market
        StructField("membership_tier", StringType(), True),  # PS_PLUS_ESSENTIAL | GAME_PASS_ULTIMATE | null
        StructField("producer_version", StringType(), False),
    ]
)

AVRO_SUBJECT = "gc-purchase-events-value"
SCHEMA_VERSION = 1
