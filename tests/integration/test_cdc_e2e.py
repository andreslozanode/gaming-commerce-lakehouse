"""End-to-end: write to Postgres -> Debezium -> Kafka -> Spark MERGE -> Silver.

Runs against the docker-compose stack (`make cdc-up`). Marked `integration` so unit runs stay fast.
"""

from __future__ import annotations

import json
import time
import uuid

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def postgres():
    import psycopg2

    conn = psycopg2.connect("postgresql://postgres:postgres@localhost:5432/gaming_commerce")
    conn.autocommit = True
    yield conn
    conn.close()


def test_insert_propagates_to_kafka(postgres):
    from confluent_kafka import Consumer

    order_id = f"O-{uuid.uuid4().hex[:8]}"
    with postgres.cursor() as cur:
        cur.execute("INSERT INTO customers (player_id) VALUES (%s) ON CONFLICT DO NOTHING", ("P-TEST",))
        cur.execute(
            "INSERT INTO orders (order_id, player_id, channel_code, order_total) VALUES (%s, %s, %s, %s)",
            (order_id, "P-TEST", "digital_store", 59.99),
        )

    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": f"test-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(["gc.public.orders"])

    deadline = time.time() + 60
    found = None
    while time.time() < deadline and not found:
        msg = consumer.poll(1.0)
        if msg and not msg.error():
            payload = json.loads(msg.value())
            if payload.get("after", {}).get("order_id") == order_id:
                found = payload
    consumer.close()

    assert found, "Debezium did not deliver the insert within 60s"
    assert found["op"] in ("c", "r")
    assert found["source"]["table"] == "orders"


def test_update_carries_the_before_image(postgres):
    """The whole reason we keep the full envelope: `before` must survive the trip."""
    from confluent_kafka import Consumer

    order_id = f"O-{uuid.uuid4().hex[:8]}"
    with postgres.cursor() as cur:
        cur.execute(
            "INSERT INTO orders (order_id, player_id, channel_code, order_total) VALUES (%s, %s, %s, %s)",
            (order_id, "P-TEST", "physical_retail", 49.99),
        )
        time.sleep(2)
        cur.execute("UPDATE orders SET order_status = 'SHIPPED' WHERE order_id = %s", (order_id,))

    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": f"test-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(["gc.public.orders"])
    deadline = time.time() + 60
    update = None
    while time.time() < deadline and not update:
        msg = consumer.poll(1.0)
        if msg and not msg.error():
            payload = json.loads(msg.value())
            if payload.get("op") == "u" and payload["after"]["order_id"] == order_id:
                update = payload
    consumer.close()

    assert update, "no update event received"
    assert update["before"]["order_status"] == "PLACED"
    assert update["after"]["order_status"] == "SHIPPED"
