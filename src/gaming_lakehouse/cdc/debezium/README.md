# CDC connector notes

* **No `ExtractNewRecordState` SMT.** Spark consumes the raw Debezium envelope, so `op=d`
  and `before` images survive the trip. Flattening at the connector would lose them.
* **Heartbeats are mandatory.** Low-traffic tables otherwise stall the replication slot and
  Postgres WAL grows until the disk fills. `heartbeat.interval.ms=10000` + a heartbeat table.
* **Least-privilege role**: the `debezium` role gets `REPLICATION`, `SELECT` on the five tables
  and ownership of the publication. Nothing else. See `scripts/seed_oltp.sql`.
* **Topics are compacted** — a replay from the earliest offset rebuilds the current state.
* **Cloud mapping**: on GCP the Connect cluster writes to Pub/Sub (Kafka Connect + Pub/Sub sink,
  or Confluent Cloud); on Azure it writes straight to the Event Hubs Kafka endpoint.
