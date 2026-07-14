# Data model

## Silver

| Table | Grain | Notes |
|---|---|---|
| `dim_console` | platform_code | PS/Xbox families, generation, launch year, era (90s/00s/10s/20s). Built in pandas. |
| `dim_purchase_channel` | channel_code | PHYSICAL / DIGITAL / MEMBERSHIP / HARDWARE |
| `silver_game_sales` | title × platform × year | VGChartz conformed + Metacritic scores. Clustered by `(era, console_family, platform_code)`. |
| `silver_purchase_events` | event | Conformed real-time purchases. Net revenue computed here, once. |
| `silver_orders_history` | order_id × LSN | **SCD2** from CDC. `is_current`, `valid_from`, `valid_to`, `is_deleted`. |
| `silver_subscriptions_history` | subscription_id × LSN | SCD2. Source of MRR and churn. |
| `silver_customers_history` | player_id × LSN | SCD2. |
| `quarantine_*` | row | Every expectation violation, with the failed rule names. Nothing is dropped silently. |

## Gold

| Mart | Question it answers |
|---|---|
| `gold_sales_by_era_platform` | How did the physical/digital/membership mix shift from the 90s to the 10s, per console family? |
| `gold_console_lifecycle` | How does software attach evolve across a console's life, PS1 through PS5, Xbox through Series? |
| `gold_player_360` | Who is this player: RFM, LTV, digital share, membership tier, churn label. |
| `gold_membership_mrr` | MRR waterfall, churn rate and reactivation per tier per month, straight off the SCD2 history. |
| `gold_title_performance` | Which titles carried a generation, and which have a retro long tail. |
| `gold_catalog_enriched` | LLM-extracted sub-genre, franchise, retro appeal, audience. |
| `gold_player_churn_scores` | Batch-scored churn probability + risk band. |

## Feature tables

`feat_player`, `feat_interactions`, `feat_sales_ts` — materialized in Gold so training and batch
inference read *the same rows*. That is the cheapest defence against training/serving skew.

## Key conventions

- **Natural keys**, never surrogate autoincrements: `(title, platform_code, release_year)` for
  titles, `event_id` for events, the OLTP PK for CDC entities. Surrogates break idempotent replay.
- **`_` prefix** on lineage/technical columns (`_ingested_at`, `_source_file`, `_lsn`, `_dq_*`).
- **UTC everywhere**, enforced in the Spark session.
- **Money is `DOUBLE` in the lake, `NUMERIC` in Postgres.** Debezium ships decimals as strings
  (`decimal.handling.mode=string`) so precision survives the trip.
