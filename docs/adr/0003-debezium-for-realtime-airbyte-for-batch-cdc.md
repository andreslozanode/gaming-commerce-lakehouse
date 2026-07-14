# ADR-0003: Debezium owns real-time CDC; Airbyte owns batch CDC and the SaaS long tail

- **Status:** Accepted
- **Date:** 2026-07-14
- **Deciders:** data-platform

## Context

The OLTP store (PostgreSQL: `orders`, `order_items`, `subscriptions`, `customers`, `consoles`) must
reach Silver with sub-minute latency for the operational marts, and must also be reconcilable in
bulk when a schema change, an outage or a replication-slot loss leaves a gap. One tool being asked
to do both jobs does one of them badly: log-based CDC engines are poor at full refresh and
backfills; ELT connector platforms are poor at sub-minute latency and at preserving before-images.

## Decision

Run **both**, with a hard split of responsibility:

- **Debezium** (Kafka Connect → Pub/Sub / Event Hubs) is the *streaming* path. It reads the
  PostgreSQL WAL through a logical replication slot with a least-privilege `debezium` role
  (`REPLICATION` + `SELECT` on the publication only). The **full Debezium envelope is preserved** —
  `ExtractNewRecordState` is deliberately **not** applied — because `before`, `op`, `ts_ms`, `lsn`
  and `snapshot` are exactly what the SCD2 merge needs (ADR-0007). `decimal.handling.mode=string`
  keeps money exact across the wire.
- **Airbyte** is the *batch* path: scheduled incremental syncs as a CDC backstop (gap-filling after
  slot loss or connector downtime), full refreshes on demand, and the SaaS long tail (Kaggle
  metadata, reference sources) where writing a bespoke connector is not a good use of anyone's time.

Both land in the same Bronze tables; the SCD2 merge is idempotent, so an Airbyte replay of rows
Debezium already delivered is a no-op.

## Consequences

**Positive.** Each tool does what it is good at. A Debezium outage is survivable without data loss —
Airbyte reconciles. Backfills do not compete with the streaming path for the replication slot.
Least-privilege plus a publication scoped to five tables keeps the CDC blast radius small.

**Negative.** Two systems to operate, monitor and upgrade. The replication slot becomes a
first-class SLO: an unconsumed slot grows the WAL until the primary runs out of disk, so
`dag_streaming_ops` health-checks slot lag explicitly and pages on it.

**Neutral.** Keeping the raw envelope makes Bronze noisier and harder to read by eye. Correct, not
convenient — and Silver is the layer people are supposed to read.

## Alternatives considered

- **Debezium only.** Backfills through a snapshot re-run are disruptive and slow, and there is no
  answer for the SaaS sources. Rejected.
- **Airbyte only.** Its CDC is fine but its floor is minutes-to-tens-of-minutes; the operational
  marts and the streaming aggregates would both miss their SLOs. Rejected.
- **Cloud-native CDC (Datastream on GCP / ADF change tracking on Azure).** Excellent per-cloud, but
  each is a different product with a different envelope — which would put a cloud-shaped branch
  right in the middle of the merge logic and violate ADR-0001. Rejected.
- **`ExtractNewRecordState` SMT to flatten the envelope.** Discards the before-image and the `op`
  code, which are precisely the inputs SCD2 and tombstone handling need. Rejected.
