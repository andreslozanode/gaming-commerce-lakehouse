# ADR-0004: Two streaming engines behind one Avro contract

- **Status:** Accepted
- **Date:** 2026-07-14
- **Deciders:** data-platform

## Context

Two streaming requirements pull in opposite directions. The lakehouse needs exactly-once,
Delta-native writes with schema evolution and CDF — that is Spark Structured Streaming's home
ground. The real-time enrichment path (sliding-window revenue by console/channel, DLQ for
malformed events) needs low per-event latency and windowing semantics that do not pay Spark's
micro-batch tax, and it must run as a managed service on both clouds.

## Decision

Run **two engines over one contract**. The contract is the Avro schema in
`streaming/event_schemas.py` (`PURCHASE_EVENT_V1`), registered in the schema registry on both
clouds and enforced at the producer.

- **Spark Structured Streaming** owns the **lakehouse path**: broker → Bronze Delta (watermark +
  `dropDuplicatesWithinWatermark`, RocksDB state store), and Bronze → Silver via
  `foreachBatch` + `MERGE`.
- **Apache Beam** owns the **low-latency path**: `DataflowRunner` on GCP, `FlinkRunner` (on AKS) on
  Azure, selected by the same `CLOUD` toggle. Sliding-window aggregates, DLQ, and the real-time
  serving feed.

Neither engine is allowed to redefine the event; both read the same schema module.

## Consequences

**Positive.** Each path meets its SLO without compromising the other. Because the contract lives in
one Python module and one registry, a schema change is a single reviewable diff, and a producer that
violates it fails at publish time rather than corrupting Bronze.

**Negative.** Two runtimes, two sets of operational knowledge, two deploy paths
(`orchestration/beam/run_beam.sh` vs the Databricks Asset Bundle streaming jobs). The Beam pipeline
is portable in principle and, as always, mildly runner-specific in practice.

**Neutral.** Some aggregate numbers exist in two places (Beam's near-real-time view and Gold's
authoritative view). Gold is the system of record; the Beam output is explicitly labelled as
provisional in the serving API.

## Alternatives considered

- **Structured Streaming only.** Simpler, but the micro-batch floor and Spark's windowing make the
  sub-10s enrichment SLO expensive to reach and expensive to run. Rejected.
- **Beam only (write Delta from Beam).** The Delta sink from Beam is immature relative to Spark's,
  and we would lose `foreachBatch` + `MERGE`, which is the backbone of the CDC path. Rejected.
- **Flink everywhere (including on GCP).** Would work, but it means operating Flink on GKE to
  duplicate what Dataflow gives us as a managed service. Rejected on operational cost.
