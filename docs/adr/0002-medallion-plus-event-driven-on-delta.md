# ADR-0002: Medallion layers + event-driven ingress, Delta Lake as the single table format

- **Status:** Accepted
- **Date:** 2026-07-14
- **Deciders:** data-platform

## Context

The workload has two very different shapes. Historical console/title sales (VGChartz, Metacritic,
Game Pass and PlayStation catalogues from Kaggle) arrive as **files**, in bulk, occasionally, and
are reprocessed whenever a dataset is revised. Commerce activity (physical, digital, membership and
hardware purchases) arrives as **events**, continuously, and is expected to be queryable within
minutes. A single ingestion style would either make the batch path absurdly complicated or make the
streaming path unacceptably slow.

## Decision

Adopt the medallion contract — **landing → bronze → silver → gold** — with two ingress paths that
converge at Bronze: Auto Loader for files, Structured Streaming from Pub/Sub / Event Hubs for
events. Every layer is a **Delta** table, with Change Data Feed enabled on Bronze and Silver,
deletion vectors on, and auto-optimize/auto-compact on the streaming targets.

Layer contract:
- **Landing** — bytes as received, immutable, checksum-gated. Never queried by consumers.
- **Bronze** — raw + `_rescued_data`, source lineage columns, append-only. Replayable.
- **Silver** — conformed, deduplicated, expectation-checked; CDC history as SCD2 (see ADR-0007).
- **Gold** — business marts (`gold_sales_by_era_platform`, `gold_console_lifecycle`,
  `gold_player_360`, `gold_membership_mrr`) and ML feature tables.

## Consequences

**Positive.** Change Data Feed makes Bronze→Silver genuinely incremental instead of a nightly full
scan — the single largest cost line in the batch path. ACID + time travel gives us a rollback
mechanism that does not involve restoring a backup (see `RUNBOOK.md`). One table format means one
maintenance job, one set of optimizations, and no format-conversion boundary between the batch and
streaming halves of the platform.

**Negative.** Delta ties us to a Spark-centric read path for writers; non-Spark consumers read Gold
through BigQuery/Synapse rather than the lake directly, which is an extra publish step
(`delivery/publish_marts.py`).

**Neutral.** Four layers means data is physically written more than once. That is the price of
replayability and it is paid consciously: Landing and Bronze are on cheap tiers with lifecycle
rules, not on the hot tier.

## Alternatives considered

- **Lambda architecture (separate batch and speed layers with separate code).** Two implementations
  of the same business logic, reconciled by hope. Rejected.
- **Iceberg or Hudi.** Both are credible; Iceberg in particular has the stronger multi-engine story.
  Delta wins here because the compute is Databricks on both clouds, and liquid clustering, CDF and
  deletion vectors are first-class there today. Revisit if compute moves off Databricks.
- **Warehouse-first (load everything into BigQuery/Synapse and transform there).** Cheap to start,
  but pins the ML/GenAI path to warehouse egress and makes the streaming half awkward. Rejected.
