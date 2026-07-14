# Optimizations

Each entry says *what*, *where*, and *why it matters here* — not generic advice.

## Spark / Delta (`src/gaming_lakehouse/spark.py`)

| Setting | Why on this workload |
|---|---|
| `adaptive.enabled` + `skewJoin.enabled` | The title↔platform join is brutally skewed: a handful of platforms (PS2, X360) carry most rows. AQE splits those partitions at runtime instead of leaving one straggler task. |
| `adaptive.advisoryPartitionSizeInBytes=128m` | Post-shuffle coalescing; without it the Gold aggregations produce thousands of tiny files. |
| `autoBroadcastJoinThreshold=64m` + explicit `F.broadcast()` | `dim_console` and `dim_purchase_channel` are a few dozen rows. Broadcasting them removes a shuffle from every Silver job. |
| `dynamicPartitionPruning.enabled` | The event fact table is clustered on `event_date`; DPP prunes it when joining against a date-filtered dimension. |
| Liquid clustering (not Z-ORDER) | Clustering keys change as the analysis evolves (era → console_family → genre). Liquid re-clusters incrementally; Z-ORDER needs a full rewrite and locks in a partition shape. See ADR-0005. |
| `optimizeWrite` + `autoCompact` | Streaming micro-batches create small files by construction. These two turn a nightly firefight into a no-op. |
| Deletion vectors | The CDC MERGE rewrites rows constantly. Deletion vectors turn a file rewrite into a metadata write; `REORG ... APPLY (PURGE)` reclaims later, off the hot path. |
| Change Data Feed | Makes Bronze→Silver *incremental*. Without CDF you rescan Bronze nightly, which is the single most expensive mistake in a medallion design. |
| `zstd` Parquet codec | ~20–30% smaller than snappy at comparable CPU; storage is the recurring cost. |
| Kryo + Arrow | Arrow makes `toPandas()` and pandas UDFs zero-copy — the ML feature load depends on it. |
| `ansi.enabled=true` | A bad cast fails loudly instead of silently producing `null` revenue. |
| `speculation=false` | Speculative execution duplicates tasks; on a MERGE-heavy job that breaks idempotency. |
| Photon (prod job clusters) | Vectorized execution on the Silver/Gold scans. |
| RAPIDS (GPU cluster only) | Feature prep for the recsys runs on the same A100 as the training job. |

## Streaming

- **Bounded micro-batches** — `maxOffsetsPerTrigger` / `maxBytesPerTrigger`. Unbounded first
  batches after a restart are how streaming jobs OOM.
- **RocksDB state store** — the dedup keyspace (`event_id` over a 15-minute watermark) exceeds
  what the in-memory store handles comfortably at prod volume.
- **`dropDuplicatesWithinWatermark`** — bounded-memory exactly-once, unlike unbounded `dropDuplicates`.
- **`failOnDataLoss=false`** on Event Hubs — retention expiry must not kill the stream; the gap
  is closed by the Airbyte batch path.
- **DLQ everywhere** — Pub/Sub dead-letter topics, Kafka Connect `errors.deadletterqueue`, Beam
  tagged outputs. A poison pill parks; it never stops the pipeline.

## CDC

- **Full Debezium envelope, no `ExtractNewRecordState`** — keeps `before` images and deletes.
- **Dedup on `(pk, lsn)` before the MERGE** — Delta throws if one target row matches twice, and
  Debezium *will* redeliver after a restart.
- **Order by LSN, not by wall-clock** — `ts_ms` is not monotonic across a failover; the LSN is.
- **Compacted topics** — a replay from offset 0 rebuilds current state.
- **Least-privilege role** — `REPLICATION` + `SELECT` on five tables. See `scripts/seed_oltp.sql`.

## pandas (`ingestion/reference_dims.py`)

PyArrow-backed dtypes, `category` on low-cardinality keys, `pd.cut` instead of `.apply(lambda)`,
downcast numerics. Three to five times less memory and zero-copy handoff to Spark. pandas is the
right tool below ~1M rows and the wrong one above it — that boundary is the whole rule.

## PyTorch / CUDA (`ml/train_torch_recsys.py`)

| Technique | Effect |
|---|---|
| `autocast(bf16)` with fp16 + `GradScaler` fallback | ~2x throughput on A100; bf16 needs no loss scaling, fp16 does. Capability is detected at runtime. |
| TF32 matmul + cuDNN | Free speedup on the fp32 paths. |
| `cudnn.benchmark=True` + `drop_last=True` | Fixed shapes let cuDNN autotune. Ragged final batches defeat both this and `torch.compile`. |
| `torch.compile(mode="max-autotune")` | Fuses the embedding→MLP→normalize chain. |
| DDP with `gradient_as_bucket_view=True` | Removes a gradient copy; `broadcast_buffers=False` because there are no buffers to sync. |
| `pin_memory` + `non_blocking=True` + `persistent_workers` + `prefetch_factor` | Overlaps H2D transfer with compute. The DataLoader is the bottleneck in embedding models far more often than the GPU is. |
| Fused AdamW | One kernel instead of several per parameter group. |
| `zero_grad(set_to_none=True)` | Skips a memset over every gradient tensor. |
| In-batch sampled softmax | Every other item in the batch is a free negative — no separate negative-sampling pass. |

## TensorFlow (`ml/train_tf_forecast.py`)

- `mixed_float16` policy with a **float32 output head** — the head must stay fp32 for numerical safety.
- `jit_compile=True` (XLA) on the train step.
- `tf.data`: `cache → shuffle → batch → prefetch(AUTOTUNE)`, in that order. Caching after
  shuffling defeats the cache.
- `set_memory_growth(True)` — TF otherwise grabs the entire card and starves anything else on it.
- `MirroredStrategy` when more than one GPU is visible; `OneDeviceStrategy` otherwise.
- Huber loss — Black-Friday and launch-week spikes would otherwise dominate an MSE gradient.
- **Chronological split, never random** — random splits leak the future into training and produce
  a forecaster that looks excellent and is worthless.

## Cost

- Lifecycle rules: landing/temp expire at 30 days; Bronze cools to Nearline/Cool at 90.
- Spot/preemptible workers in dev and qa; on-demand in prod. Enforced through workspace cluster policies (Terraform), which keeps the bundle's cluster specs cloud-neutral.
- Autoscaling bounds per environment; ephemeral job clusters, never all-purpose ones.
- BigQuery Storage Write API in Beam — no GCS staging round-trip.
- GenAI enrichment is a `left_anti` join against what is already enriched: the LLM bill scales
  with *new* titles, not with table size.
- Budget alerts at 50/90/100% wired into Terraform in both clouds.
