# MANIFEST

Every file in `gaming-commerce-lakehouse`, and what it is for.
**127 files · ~5,600 lines of Python / HCL / YAML / SQL / Groovy / Bash.**

Read this top-to-bottom once and you know where everything lives. The one file that matters more
than the others is `src/gaming_lakehouse/config.py`: it is the only place `CLOUD` and `ENVIRONMENT`
are read, and everything else is downstream of it.

---

## Root

| File | Purpose |
|---|---|
| `README.md` | Overview, the two-toggle model, quickstart, deploy, layout, doc index. |
| `MANIFEST.md` | This file. |
| `LICENSE` | MIT. |
| `pyproject.toml` | Package metadata, the twelve wheel entry points consumed by the Databricks bundle (`ingest`, `bronze_stream`, `cdc_merge`, `bronze_to_silver`, `silver_to_gold`, `maintenance`, `features`, `train_torch`, `train_tf`, `batch_inference`, `publish_marts`, `genai_enrich`), plus ruff / mypy / pytest / coverage configuration. |
| `Makefile` | The developer interface: `setup`, `lint`, `test`, `test-integration`, `cdc-up/down`, `ingest`, `stream`, `simulate`, `silver`, `gold`, `features`, `train-torch`, `train-tf`, `beam`, `tf-plan`, `tf-apply`, `bundle-deploy`, `docker-gpu`, `clean`. `make help` prints them all. |
| `.env.example` | The contract with the operator: `CLOUD` and `ENVIRONMENT` first, everything else optional. Shapes only — no secrets. |
| `.gitignore` | Excludes `.env`, checkpoints, Spark warehouse dirs, MLflow runs, Terraform state, wheels. |
| `.pre-commit-config.yaml` | ruff, ruff-format, gitleaks, terraform fmt, end-of-file/whitespace hooks. |

## `conf/` — configuration layer

Resolved in one direction, once: `base` → `cloud/<CLOUD>` → `env/<ENVIRONMENT>` → environment vars.

| File | Purpose |
|---|---|
| `conf/base.yaml` | Cloud-agnostic defaults: medallion layer names, Unity Catalog schemas, all Spark/Delta tuning (AQE, skew join, DPP, broadcast threshold, auto-optimize, deletion vectors, CDF, liquid clustering), streaming defaults (trigger, RocksDB state store, watermark), data-quality severity, ML hyperparameters for Torch and TF. |
| `conf/cloud/gcp.yaml` | GCS protocol and bucket pattern, Pub/Sub topics, BigQuery, Composer, `a2-highgpu-1g` GPU nodes, Secret Manager, Vertex AI, Dataflow runner. |
| `conf/cloud/azure.yaml` | ADLS Gen2 (`abfss://`), Event Hubs, Synapse, Azure Databricks, `Standard_NC24ads_A100_v4`, Key Vault, Azure OpenAI, Flink runner. |
| `conf/env/dev.yaml` | Small clusters, quality violations `warn`, local MLflow file store, streaming on demand. |
| `conf/env/qa.yaml` | Mid clusters, violations `drop` + quarantine, Databricks-managed MLflow, continuous streaming. |
| `conf/env/prod.yaml` | Large clusters (autoscale 4–32, spot workers off), violations `fail`, 30-day VACUUM retention, SLO-monitored streaming. |
| `conf/datasets.yaml` | The seven Kaggle datasets (slug, files, Bronze table, checksum policy) and the five PostgreSQL CDC tables with their natural keys. |

## `src/gaming_lakehouse/` — the platform package

### Core

| File | Purpose |
|---|---|
| `__init__.py` | Version. |
| `config.py` | **The single source of truth.** Loads and merges the config layers, validates the toggles (an invalid `CLOUD`/`ENVIRONMENT` fails at import, not at the first cloud call), and exposes `Settings` with `layer_uri()` (`gs://` vs `abfss://`) and `table()` (three-level Unity Catalog names, `gamingcommerce_<env>.<schema>.<table>`). Cached — read once per process. |
| `spark.py` | One tuned `SparkSession` builder: AQE + skew join + partition coalescing, dynamic partition pruning, Kryo, Arrow, Delta extensions and catalog, Photon on Databricks, optional RAPIDS for the GPU path, shuffle partitions derived from the environment. |
| `secrets.py` | Resolves a secret *name* to a *value* without the caller knowing where it lives: Secret Manager (GCP) → Key Vault (Azure) → `dbutils` on Databricks → environment variable (local dev only). |
| `storage.py` | Path helpers for the medallion layers on both object stores. |
| `logging_utils.py` | Structured JSON logging with an `extra_fields` convention, so logs are queryable in Cloud Logging / Azure Monitor without a parser. |

### `ingestion/`

| File | Purpose |
|---|---|
| `kaggle_client.py` | Authenticated Kaggle API client: download, checksum, idempotent landing (a dataset whose checksum has not changed is not re-processed). |
| `run_ingest.py` | Landing → Bronze with Auto Loader: schema inference with `_rescued_data`, source lineage columns, incremental file discovery. Wheel entry point `ingest`. |
| `reference_dims.py` | Pure pandas (small, no Spark tax). Builds `dim_console` — PlayStation (PS, PS2, PS3, PS4, PS5, PSP, PSV) and Xbox (XB, X360, XOne, XS) families with generation and era (90s/00s/10s/20s) — and `dim_purchase_channel` (`physical_retail`, `physical_online`, `digital_store`, `membership_included`, `membership_fee`, `console_hardware`). |

### `streaming/`

| File | Purpose |
|---|---|
| `event_schemas.py` | `PURCHASE_EVENT_V1` — the Avro contract. One definition, shared by the producer, Spark and Beam, and registered in the schema registry on both clouds. |
| `bronze_stream.py` | Pub/Sub / Event Hubs → Bronze Delta. Watermark + `dropDuplicatesWithinWatermark`, RocksDB state store, exactly-once checkpointing. Wheel entry point `bronze_stream`. |
| `beam_enrichment.py` | The low-latency path. `DataflowRunner` or `FlinkRunner` selected by `CLOUD`; sliding-window revenue by console and channel, malformed events to a DLQ. |
| `producer_simulator.py` | Synthetic purchase events across all four channels — makes the streaming path runnable locally without production traffic. |

### `cdc/`

| File | Purpose |
|---|---|
| `cdc_merge_silver.py` | **The riskiest code in the repo.** `upsert_scd2()` consumes the full Debezium envelope and merges it into Silver as SCD Type 2: dedupe by natural key ordered on **LSN** (not wall clock), updates close the previous version, deletes become tombstones, replays are idempotent. Wheel entry point `cdc_merge`. |
| `debezium/postgres-connector.json` | Connector config: logical replication slot, publication scoped to five tables, `decimal.handling.mode=string` (money survives the wire), **no** `ExtractNewRecordState` — the before-image is the point. |
| `debezium/docker-compose.yml` | Local CDC stack: PostgreSQL + Kafka (KRaft, no ZooKeeper) + Schema Registry + Kafka Connect. |
| `debezium/README.md` | How to bring the stack up, register the connector, and inspect the topics. |
| `airbyte/connections.yaml` | The batch CDC backstop and the SaaS long tail, as declarative config. |

### `transform/`

| File | Purpose |
|---|---|
| `quality/expectations.py` | The expectations engine: a small `Expectation` type and `apply_expectations()` with `warn` / `drop` / `fail` semantics driven by the environment. Dropped rows are written to a quarantine table with the rules they failed — nothing is silently discarded. |
| `bronze_to_silver.py` | Conform, deduplicate, type, apply expectations. Reads incrementally via Change Data Feed. Wheel entry point `bronze_to_silver`. |
| `silver_to_gold.py` | The four marts: `gold_sales_by_era_platform`, `gold_console_lifecycle`, `gold_player_360` (with the 90-day-inactivity churn label), `gold_membership_mrr`. Wheel entry point `silver_to_gold`. |
| `optimize_maintenance.py` | `OPTIMIZE`, liquid re-clustering, `VACUUM` with per-environment retention. Wheel entry point `maintenance`. |

### `ml/`

| File | Purpose |
|---|---|
| `features.py` | `feat_player`, `feat_interactions`, `feat_sales_ts` — the feature tables, materialised in Gold so training and batch inference read the same rows. Wheel entry point `features`. |
| `train_torch_recsys.py` | Two-tower retrieval (player tower × title tower) + churn head on CUDA: bf16 autocast (fp16 + GradScaler on older SM), TF32, `torch.compile(mode="max-autotune")`, DDP with gradient bucketing, fused AdamW, in-batch sampled softmax, pinned-memory DataLoader. Logs to MLflow. Wheel entry point `train_torch`. |
| `train_tf_forecast.py` | Conv1D + BiLSTM sales forecaster: `mixed_float16`, XLA, `MirroredStrategy`, Huber loss, **chronological** split (a random split would leak the future). Wheel entry point `train_tf`. |
| `registry.py` | MLflow registration and the champion/challenger alias logic that the promotion gate calls. |
| `serving/batch_inference.py` | Churn scoring with a Pandas UDF; always resolves `@champion`, never a version number, so rollback is an alias move. Wheel entry point `batch_inference`. |

### `genai/`

| File | Purpose |
|---|---|
| `llm_provider.py` | `LLMProvider` interface with `VertexProvider` (Gemini) and `AzureOpenAIProvider`; `get_provider()` selects by `CLOUD`. |
| `catalog_insights.py` | Catalogue enrichment (thematic tags, audience, positioning) written to Delta, and the semantic index built into Vertex AI Vector Search or Azure AI Search. No PII in prompts; a `left_anti` join means only new titles are ever sent to the model. Wheel entry point `genai_enrich`. |

### `delivery/`

| File | Purpose |
|---|---|
| `publish_marts.py` | Gold → BigQuery or Synapse, selected by `CLOUD`. Wheel entry point `publish_marts`. |
| `serving_api.py` | FastAPI: `/health`, `/v1/sales/by-era`, `/v1/players/{id}/churn`. |

## `orchestration/`

| File | Purpose |
|---|---|
| `airflow/dags/dag_batch_medallion.py` | ingest → silver → gold → features → genai → publish → maintenance. `databricks_task()` helper with a GPU toggle and per-environment worker sizing; a Bronze freshness check fails the run rather than propagating empty marts. |
| `airflow/dags/dag_streaming_ops.py` | Supervises the continuous jobs: restarts crashed streams, checks consumer lag against the SLO (300s on prod), health-checks the PostgreSQL replication slot (an unconsumed slot eats the primary's disk), reports the quarantine tables. |
| `airflow/dags/dag_ml_training.py` | GPU train → `evaluate_challenger` → `promotion_gate` (a `ShortCircuitOperator`) → promote → score. A model that does not beat the champion by `MIN_IMPROVEMENT` never reaches production. |
| `airflow/plugins/callbacks.py` | Failure and SLA-miss alerting to the cloud-appropriate webhook. |
| `beam/run_beam.sh` | Launches the Beam pipeline on Dataflow or Flink from the same `CLOUD` toggle. |
| `airbyte/apply_config.py` | GitOps for Airbyte: upserts sources, destinations and connections through the Config API so connector state lives in git, not in a UI. |

## `infra/terraform/` — infrastructure

One module is selected by `count` on `var.cloud`; a `terraform plan` for GCP never evaluates Azure resources.

| File | Purpose |
|---|---|
| `versions.tf`, `backend.tf`, `variables.tf`, `main.tf`, `outputs.tf` | Root: provider constraints, remote state, the validated `cloud` / `environment` variables, module selection. |
| `modules/gcp/main.tf` | Layer buckets with lifecycle rules and CMEK, KMS, Pub/Sub topics + Avro schema + DLQ + exactly-once subscriptions, BigQuery, Composer, Cloud SQL PostgreSQL with `logical_decoding` on, the Workload Identity Federation pool and provider, pipeline service account and IAM, Secret Manager, a billing budget. |
| `modules/gcp/variables.tf`, `outputs.tf`, `schemas/purchase_event.avsc` | Inputs, outputs, and the registered event schema. |
| `modules/azure/main.tf` | ADLS Gen2 containers with lifecycle, Event Hubs + schema group, Synapse, PostgreSQL Flexible Server with `wal_level=logical`, the Databricks workspace, Azure ML + Application Insights, Key Vault, a user-assigned identity with a federated credential for GitHub OIDC, a consumption budget. |
| `modules/azure/variables.tf`, `outputs.tf` | Inputs and outputs. |
| `envs/{dev,qa,prod}/terraform.tfvars` | Per-environment sizing and toggles. |
| `envs/{dev,qa,prod}/backend.{gcp,azure}.hcl` | Remote state, one per `(cloud, environment)` pair. |

## `databricks/` — workloads

| File | Purpose |
|---|---|
| `databricks.yml` | The bundle: targets `dev` (development mode) / `qa` / `prod`. Workloads only — it has no permission to touch infrastructure. |
| `resources/clusters.yml` | Shared cluster shapes as **complex variables** (`${var.batch_cluster}` Photon autoscaling, `${var.gpu_cluster}` single-node A100) — the Asset Bundle idiom for one spec used by many jobs; YAML anchors do not cross include files. Cloud-neutral: spot policy lives in Terraform cluster policies. |
| `resources/jobs.yml` | `gc_batch_medallion` and `gc_ml_training`, wired to the wheel entry points. |
| `resources/streaming.yml` | The continuous jobs: `gc_bronze_purchase_events`, `gc_cdc_orders`, `gc_cdc_subscriptions`. |

## `.github/workflows/` and `jenkins/` — CI/CD

| File | Purpose |
|---|---|
| `.github/workflows/ci.yml` | Quality (ruff, mypy, sqlfluff) · security (bandit, pip-audit, gitleaks, checkov) · tests on Python 3.10/3.11/3.12 behind an 80% coverage gate · the CDC integration test against the docker stack · wheel build · `terraform validate` for both clouds. |
| `.github/workflows/cd.yml` | `resolve → terraform → databricks → airflow-and-airbyte → smoke → promote-to-prod`. Keyless OIDC to both clouds. `develop` → dev; `main` → qa → prod behind a manual approval. |
| `jenkins/Jenkinsfile` | The same pipeline as a declarative Jenkins job: `CLOUD` / `ENVIRONMENT` / `SKIP_TESTS` parameters, parallel quality gates, unit + integration, build, a prod approval `input`, parallel multi-cloud deploy, smoke, Slack notification. |
| `jenkins/vars/gcCloudLogin.groovy` | Federated login to GCP or Azure. |
| `jenkins/vars/gcTerraformApply.groovy` | Plan/apply with the right backend for the `(cloud, environment)` pair. |
| `jenkins/vars/gcDatabricksBundleDeploy.groovy` | Bundle validate + deploy per target. |
| `jenkins/vars/gcSyncOrchestration.groovy` | Ships DAGs and applies the Airbyte config. |
| `jenkins/vars/gcNotify.groovy` | Build notifications. |

## `docker/`

| File | Purpose |
|---|---|
| `Dockerfile.jobs` | Multi-stage CPU image for the batch and streaming jobs. Non-root. |
| `Dockerfile.gpu` | CUDA 12.4.1 + cuDNN 9 with PyTorch and TensorFlow, and a build-time sanity check that both actually see the device — a broken GPU image fails at build, not at 3 a.m. |
| `Dockerfile.api` | The FastAPI serving image, with a healthcheck. |

## `scripts/` and `requirements/`

| File | Purpose |
|---|---|
| `scripts/seed_oltp.sql` | The five OLTP tables with triggers, the least-privilege `debezium` role (`REPLICATION` + `SELECT` on the publication only), the `gc_pub` publication, and nine seeded console SKUs from PS1 to PS5 and Xbox to Series X. |
| `scripts/bootstrap_local.sh` | One command from a clean clone to a running local stack. |
| `requirements/base.txt` | Runtime: PySpark 3.5.3, delta-spark 3.2.1, pandas 2.2.3, pyarrow 17, Beam, Kaggle, MLflow, FastAPI, both cloud SDKs. |
| `requirements/gpu.txt` | PyTorch 2.5.1 + TensorFlow 2.18.0 on CUDA 12.4. |
| `requirements/dev.txt` | pytest, ruff, mypy, bandit, Airflow, and the rest of the toolchain. |

## `tests/`

| File | Purpose |
|---|---|
| `conftest.py` | A session-scoped local Spark + Delta session, with an offline fallback: when the Delta jars cannot be resolved (no Maven access), it degrades to vanilla Spark and Delta-dependent suites skip via the `delta_spark` fixture instead of dying in Ivy. |
| `unit/test_config.py` | The cloud contract, asserted without a cloud: `gs://` vs `abfss://`, broker and LLM switching, data-quality severity per environment, three-level table names, and invalid toggles rejected. |
| `unit/test_cdc_merge.py` | The densest suite in the repo, because SCD2 is the most dangerous code: insert, update-closes-the-previous-version, replay-is-idempotent, delete-becomes-a-tombstone. |
| `unit/test_expectations.py` | `drop` removes invalid rows *and* quarantines them; `fail` raises. |
| `unit/test_reference_dims.py` | Console families, era binning, and full coverage of the purchase channels. |
| `unit/test_transforms.py` | Era binning and net-revenue-after-discount in the marts. |
| `integration/test_cdc_e2e.py` | PostgreSQL → Debezium → Kafka, asserting both the insert and the update *before-image*. Marked `integration`; needs the docker stack. |
| `integration/test_smoke.py` | Post-deployment: config resolves against the real cloud, Gold marts are fresh. Marked `smoke`; run by CD. |

## `docs/`

| File | Purpose |
|---|---|
| `ARCHITECTURE.md` | The two-variable design, the end-to-end mermaid flow, layer contracts, the two-streaming-engines split, CDC real-time + batch, deployment topology, the environment matrix. |
| `OPTIMIZATIONS.md` | Spark/Delta, streaming, CDC, pandas, PyTorch/CUDA, TensorFlow and cost — each entry says *what*, *where*, and *why on this workload*, not in general. |
| `DATASETS.md` | The seven Kaggle sources, why they compose into a coherent commerce picture, and their licensing. |
| `DATA_MODEL.md` | Silver, Gold and feature tables; natural keys; the naming and lineage conventions (`_` prefix on lineage columns, UTC everywhere, money as `DOUBLE` in the lake and `NUMERIC` in Postgres). |
| `RUNBOOK.md` | Deploys, and the incidents: CDC lag, crash-looping streams, stale Gold, small files, GPU OOM — plus the three rollback paths (bundle redeploy, Delta time travel, champion alias move). |
| `adr/README.md` | The ADR index. |
| `adr/0001…0010` | The ten decisions: the two-variable model · medallion + event-driven on Delta · Debezium vs Airbyte · two streaming engines · liquid clustering over Z-ORDER · Terraform vs Asset Bundles · SCD2 via MERGE · champion/challenger as a CI gate · the provider-agnostic GenAI layer · keyless federated identity. |
| `adr/template.md` | For the eleventh. |
