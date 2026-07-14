"""Replays historical Kaggle sales as a live event stream (demo/QA + load testing).

Sales curves from VGChartz seed the product mix, so the synthetic stream keeps a realistic
PlayStation/Xbox split per era instead of uniform noise.
"""

from __future__ import annotations

import argparse
import json
import random
import time
import uuid
from datetime import UTC, datetime

from gaming_lakehouse.config import load_settings
from gaming_lakehouse.logging_utils import get_logger

log = get_logger(__name__)

CHANNELS = [
    "physical_retail",
    "physical_online",
    "digital_store",
    "membership_included",
    "membership_fee",
    "console_hardware",
]
PLATFORMS = ["PS", "PS2", "PS3", "PS4", "PS5", "XB", "X360", "XOne", "XS"]
TIERS = [None, "PS_PLUS_ESSENTIAL", "PS_PLUS_PREMIUM", "GAME_PASS_CORE", "GAME_PASS_ULTIMATE"]


def make_event(catalog: list[dict]) -> dict:
    product = random.choice(catalog)
    channel = random.choices(CHANNELS, weights=[18, 12, 40, 14, 12, 4])[0]
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "CONSOLE_SALE" if channel == "console_hardware" else "PURCHASE",
        "occurred_at": datetime.now(UTC).isoformat(),
        "player_id": f"P{random.randint(1, 250_000):08d}",
        "platform": product.get("platform", random.choice(PLATFORMS)),
        "channel_code": channel,
        "product_id": product["product_id"],
        "product_type": "CONSOLE" if channel == "console_hardware" else "GAME",
        "title": product.get("title"),
        "quantity": 1,
        "unit_price": round(random.uniform(9.99, 79.99), 2),
        "discount_pct": round(random.choice([0, 0, 0.1, 0.25, 0.5, 0.75]), 2),
        "currency": "USD",
        "country": random.choice(["US", "GB", "JP", "CO", "MX", "DE", "BR"]),
        "is_preowned": channel.startswith("physical") and random.random() < 0.3,
        "membership_tier": random.choice(TIERS) if channel.startswith("membership") else None,
        "producer_version": "1.0.0",
    }


def publish(events: list[dict]) -> None:
    settings = load_settings()
    topic = settings.topic("purchase_events")
    if settings.broker == "pubsub":
        from google.cloud import pubsub_v1  # type: ignore

        publisher = pubsub_v1.PublisherClient(
            publisher_options=pubsub_v1.types.PublisherOptions(enable_message_ordering=False),
            batch_settings=pubsub_v1.types.BatchSettings(max_messages=1000, max_latency=0.05),
        )
        path = publisher.topic_path(settings.get("gcp.project_id", ""), topic)
        for event in events:
            publisher.publish(path, json.dumps(event).encode())
        publisher.stop()
    else:
        from confluent_kafka import Producer  # type: ignore

        from gaming_lakehouse.secrets import get_secret

        producer = Producer(
            {
                "bootstrap.servers": settings.get("streaming.kafka_bootstrap"),
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "PLAIN",
                "sasl.username": "$ConnectionString",
                "sasl.password": get_secret("eventhubs-connection-string"),
                "linger.ms": 50,
                "compression.type": "zstd",
                "enable.idempotence": True,
            }
        )
        for event in events:
            producer.produce(topic, json.dumps(event).encode(), key=event["player_id"])
        producer.flush()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--eps", type=int, default=200, help="events per second")
    parser.add_argument("--seconds", type=int, default=60)
    args = parser.parse_args()

    catalog = [
        {"product_id": f"G{i:06d}", "title": f"Title {i}", "platform": random.choice(PLATFORMS)}
        for i in range(5000)
    ]

    for _ in range(args.seconds):
        batch = [make_event(catalog) for _ in range(args.eps)]
        publish(batch)
        log.info("published batch", extra={"extra_fields": {"count": len(batch)}})
        time.sleep(1)


if __name__ == "__main__":
    main()
