# ADR-0005: Liquid clustering instead of `ZORDER BY` on Silver/Gold

- **Status:** Accepted
- **Date:** 2026-07-14
- **Deciders:** data-platform

## Context

The Gold marts are queried along axes that are not stable over time. Analysts slice
`gold_sales_by_era_platform` by era and platform; the console lifecycle mart by `console_id` and
release window; `gold_player_360` by `player_id` for lookups and by `churn_score` for segments.
`ZORDER BY` requires committing to a column list up front and re-writing the whole table to change
it, and it degrades under skew — which this data has in abundance: a handful of platforms and a
handful of blockbuster titles carry most of the rows.

## Decision

Use **liquid clustering** (`CLUSTER BY`) on the Silver and Gold tables that are read
interactively, and let Delta maintain the layout incrementally. Clustering keys are declared in the
table DDL and can be changed with `ALTER TABLE ... CLUSTER BY` without rewriting history. Partition
columns are used only where they are also natural pruning boundaries with high cardinality-to-size
ratios (ingest date on Bronze); Gold is not physically partitioned.

## Consequences

**Positive.** Clustering keys can evolve as the questions evolve — which, on an analytics mart, is
guaranteed. Incremental clustering means the maintenance job re-organises only new data instead of
rewriting the table, so `OPTIMIZE` cost stays proportional to the delta, not to the table. Skew is
handled by the clustering algorithm rather than by us hand-tuning `ZORDER` column order.

**Negative.** Liquid clustering is a Delta/Databricks capability — it deepens the coupling accepted
in ADR-0002. It also cannot be combined with Hive-style partitioning on the same table, which
required us to drop partition columns from Gold entirely (a deliberate, tested change, not an
oversight).

**Neutral.** Query plans no longer show partition pruning; they show file skipping via clustering
statistics. Anyone debugging a slow Gold query must read the right thing in the plan.

## Alternatives considered

- **`ZORDER BY (platform, era)`.** The obvious default. Loses on this workload for three reasons:
  the column list is effectively frozen, every `OPTIMIZE` rewrites a large slice of the table, and
  Z-ordering's benefit collapses as the number of ordered columns grows past two or three — which is
  exactly where the analyst queries land. Rejected.
- **Hive partitioning by `platform`/`era`.** Produces the classic small-files disaster: a few huge
  partitions (PS2, PS4) and dozens of near-empty ones, plus a full rewrite whenever the era
  boundaries are revised. Rejected.
- **No layout optimization.** Viable on dev volumes, indefensible on prod. Rejected.
