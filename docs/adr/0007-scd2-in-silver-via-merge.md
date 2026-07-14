# ADR-0007: CDC history is materialised as SCD Type 2 in Silver via `MERGE`

- **Status:** Accepted
- **Date:** 2026-07-14
- **Deciders:** data-platform

## Context

Subscription and order rows mutate: a Game Pass tier is upgraded, an order is refunded, a customer
changes region. Analytics questions depend on *when* a row looked a certain way — "MRR by tier at
the end of each month", "revenue by the region the customer was in at purchase time". A Silver table
that only holds the latest state cannot answer them, and re-deriving history from the Bronze event
log at query time is both slow and duplicated across every consumer.

## Decision

`cdc/cdc_merge_silver.py::upsert_scd2` consumes the **full Debezium envelope** (ADR-0003) in a
`foreachBatch` and merges it into a Silver SCD Type 2 table:

- Within a micro-batch, rows are **deduplicated by natural key ordered on LSN** — the WAL sequence
  number is the only ordering that is correct under retries and out-of-order delivery; wall-clock
  `ts_ms` is not.
- An update **closes the current version** (`_valid_to`, `_is_current = false`) and inserts a new
  one. A `d` (delete) op writes a **tombstone**: the version is closed and marked deleted rather
  than physically removed, so downstream marts can distinguish "gone" from "never existed".
- The merge is **idempotent**: replaying the same LSN range produces no new versions. This is what
  makes the Airbyte backstop in ADR-0003 safe, and what makes recovery from a checkpoint loss a
  non-event.

Natural keys, not surrogate keys. Money is `DOUBLE` in the lake and `NUMERIC` in Postgres, carried
across the wire as a string (`decimal.handling.mode=string`) so no precision is lost in flight.

## Consequences

**Positive.** Point-in-time queries are a `WHERE _valid_from <= ts AND ts < _valid_to` away, for
every consumer, computed once. Idempotency turns a class of on-call incidents (replay, duplicate
delivery, checkpoint reset) into no-ops.

**Negative.** Silver grows with the *rate of change*, not the row count — a chatty table can grow
faster than its business volume suggests. Mitigated by clustering (ADR-0005) and by `_is_current`
being the default filter in every downstream view. This is also the single most delicate piece of
code in the repo, which is why it carries the densest test suite (`tests/unit/test_cdc_merge.py`:
insert, update-closes-previous-version, replay-is-idempotent, delete-becomes-tombstone).

**Neutral.** Consumers must remember `_is_current`. The Gold marts do it for them; ad-hoc queriers
have to know.

## Alternatives considered

- **SCD Type 1 (overwrite).** Cheapest, and structurally unable to answer the questions the business
  actually asks. Rejected.
- **Append-only event log in Silver, history reconstructed in views.** Pushes the window function
  onto every reader and every query; the cost is paid repeatedly instead of once at write time.
  Rejected.
- **Delta Change Data Feed as the history mechanism.** CDF is a *transport* for changes to this
  table, not a business-grade history of the source entity — it is tied to retention and to file
  compaction, and it does not survive a rebuild. Used for Bronze→Silver incrementality (ADR-0002),
  not as the SCD. Rejected for this purpose.
- **Ordering by `ts_ms` instead of `lsn`.** Simpler and wrong: transaction commit timestamps are not
  monotonic across sessions under load. Rejected.
