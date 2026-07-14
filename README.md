# gaming-commerce-lakehouse

A production-grade, multi-cloud **Data Engineering + AI/ML/GenAI** platform over three decades of
PlayStation and Xbox commerce: physical retail, digital storefronts, subscription memberships and
console hardware, from the 90s to today.

It runs on **GCP or Azure**, in **dev, qa or prod**, from the same codebase — and which one it is
comes down to two environment variables.

```bash
CLOUD=gcp        ENVIRONMENT=dev     # gs://   Pub/Sub    BigQuery   Secret Manager   Vertex AI
CLOUD=azure      ENVIRONMENT=prod    # abfss:// Event Hubs Synapse    Key Vault        Azure OpenAI
```

---

## 1. What it does

| Stage | What happens |
|-------|--------------|
| **Ingest** | Kaggle datasets (VGChartz sales, Metacritic ratings, Steam/PS/Xbox player profiles, Game Pass library, PS4/PS5 catalogues) pulled via the Kaggle API, checksum-gated into Landing, then Auto Loader → Bronze. |
| **Stream** | Purchase events (physical · digital · membership · hardware) published as Avro to Pub/Sub / Event Hubs. Spark Structured Streaming lands them in Bronze; Apache Beam (Dataflow on GCP, Flink on Azure) computes sliding-window revenue with a DLQ. |
| **CDC** | PostgreSQL OLTP (`orders`, `order_items`, `subscriptions`, `customers`, `consoles`) → Debezium (WAL, full envelope) for real-time; Airbyte for batch reconciliation and the SaaS long tail. |
| **Process** | Medallion on Delta: Bronze → Silver (conform, dedupe, expectations, SCD Type 2) → Gold marts. Change Data Feed makes Bronze→Silver incremental; liquid clustering keeps the marts fast. |
| **ML** | PyTorch two-tower recommender + churn head on CUDA (bf16 AMP, TF32, `torch.compile`, DDP). TensorFlow Conv1D+BiLSTM sales forecaster (mixed precision, XLA). MLflow champion/challenger promotion enforced as a CI gate. |
| **GenAI** | Catalogue enrichment and a semantic index behind one provider interface — Gemini/Vertex on GCP, Azure OpenAI on Azure. No PII in prompts; enrichment is a `left_anti` join so the bill scales with new titles only. |
| **Deliver** | Gold published to BigQuery / Synapse; a FastAPI service serves era/platform aggregates and per-player churn scores. |

The four Gold marts: `gold_sales_by_era_platform`, `gold_console_lifecycle`, `gold_player_360`
(with churn), `gold_membership_mrr`.

---

## 2. Architecture at a glance

```
                        ┌──────────────────────── CLOUD × ENVIRONMENT ────────────────────────┐
                        │            resolved exactly once, in config.py                       │
                        └─────────────────────────────────────────────────────────────────────┘

  Kaggle API ──batch──▶ Landing ──Auto Loader──▶ ┐
                                                 │
  PostgreSQL ──Debezium (WAL)──▶ Broker ─────────┼──▶ BRONZE ──CDF──▶ SILVER ──▶ GOLD ──▶ BigQuery/Synapse
       └─────Airbyte (batch backstop)────────────┤     Delta        SCD2 +        marts +      + FastAPI
                                                 │                  expectations   features
  Purchase events ──Avro──▶ Broker ──────────────┘                        │
                              └── Beam (Dataflow|Flink) ──▶ real-time aggregates   ├──▶ PyTorch / TensorFlow (CUDA)
                                                                                   └──▶ GenAI enrichment + semantic index
```

Full diagrams, layer contracts and the environment matrix: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).
The reasoning behind every load-bearing choice: [`docs/adr/`](docs/adr/README.md).

---

## 3. The two-toggle model

Everything is a derived property of `CLOUD` and `ENVIRONMENT`. Configuration resolves in one
direction, and only in `config.py`:

```
conf/base.yaml  →  conf/cloud/<CLOUD>.yaml  →  conf/env/<ENVIRONMENT>.yaml  →  environment variables
```

| Concern | `CLOUD=gcp` | `CLOUD=azure` |
|---|---|---|
| Object store | GCS (`gs://`) | ADLS Gen2 (`abfss://`) |
| Broker | Pub/Sub | Event Hubs |
| Warehouse | BigQuery | Synapse |
| Secrets | Secret Manager | Key Vault |
| Beam runner | Dataflow | Flink on AKS |
| LLM | Vertex AI / Gemini | Azure OpenAI |
| Vector store | Vertex AI Vector Search | Azure AI Search |
| GPU node | `a2-highgpu-1g` (A100) | `Standard_NC24ads_A100_v4` |
| Federation | Workload Identity Federation | Managed Identity + federated credential |

| Concern | `dev` | `qa` | `prod` |
|---|---|---|---|
| Data-quality violations | `warn` | `drop` + quarantine | `fail` |
| MLflow backend | local file store | Databricks-managed | Databricks-managed |
| Model promotion | logged only | `@challenger` | `@champion` if it beats the incumbent |
| Cluster autoscale | 1–2 | 2–8 | 4–32, spot workers off |
| Streaming | on demand | continuous | continuous, SLO-monitored |

No business logic branches on either variable. That is the whole point — see
[ADR-0001](docs/adr/0001-two-variable-configuration-model.md).

---

## 4. Quickstart (local)

Requirements: Python 3.10–3.12, JDK 17, Docker, a Kaggle API token.

```bash
git clone <repo> && cd gaming-commerce-lakehouse
cp .env.example .env                 # set CLOUD, ENVIRONMENT, KAGGLE_API_TOKEN
make setup                           # venv + requirements/dev.txt + pre-commit + editable install

make lint                            # ruff + mypy + sqlfluff
make test                            # unit suite (local Spark + Delta)

make cdc-up                          # Postgres + Kafka (KRaft) + Schema Registry + Debezium Connect
make ingest                          # Kaggle → Landing → Bronze
make silver                          # Bronze → Silver (expectations + SCD2)
make gold                            # Silver → Gold marts
make simulate                        # publish synthetic purchase events
make stream                          # Structured Streaming: broker → Bronze
make features && make train-torch    # feature tables + GPU training run
make test-integration                # end-to-end CDC test against the docker stack
```

`make help` lists every target. `make cdc-down` tears the stack down.

## 5. Deploy

```bash
export CLOUD=gcp ENVIRONMENT=qa

make tf-plan                         # terraform plan   (infrastructure)
make tf-apply                        # terraform apply  (gated in CI)
make bundle-deploy                   # databricks bundle deploy (workloads)
```

Infrastructure and workloads are deployed by different tools, on purpose
([ADR-0006](docs/adr/0006-terraform-for-infra-bundles-for-workloads.md)). A workload deploy cannot
touch infrastructure.

**CI** (`.github/workflows/ci.yml`) — ruff · mypy · sqlfluff · bandit · pip-audit · gitleaks ·
checkov · pytest matrix on Python 3.10/3.11/3.12 with an 80% coverage gate · CDC integration test ·
wheel build · `terraform validate` for both clouds.

**CD** (`.github/workflows/cd.yml`) — `resolve → terraform → databricks → airflow+airbyte → smoke →
promote`. `develop` deploys to **dev**; `main` deploys to **qa** and then to **prod** behind a manual
approval. Authentication is keyless GitHub OIDC on both clouds — there is no cloud key in CI
([ADR-0010](docs/adr/0010-keyless-federated-identity.md)).

**Jenkins** (`jenkins/Jenkinsfile`) — the same pipeline for teams on Jenkins, with `CLOUD` /
`ENVIRONMENT` as build parameters and a shared library in `jenkins/vars/`.

---

## 6. Layout

```
conf/                YAML config: base → cloud/<CLOUD> → env/<ENVIRONMENT>; datasets + CDC tables
src/gaming_lakehouse/
  config.py          the only place CLOUD and ENVIRONMENT are read
  spark.py           AQE, Delta, Kryo, Arrow, Photon, RAPIDS — one tuned session builder
  secrets.py         Secret Manager | Key Vault | dbutils | env, resolved by CLOUD
  ingestion/         Kaggle client, Auto Loader → Bronze, reference dimensions (console, channel)
  streaming/         Avro contract, broker → Bronze, Beam enrichment, event simulator
  cdc/               Debezium connector + compose stack, SCD2 merge, Airbyte GitOps config
  transform/         Bronze→Silver, Silver→Gold, expectations engine, table maintenance
  ml/                features, PyTorch two-tower + churn, TF forecaster, registry, batch inference
  genai/             LLM provider interface, catalogue enrichment, semantic index
  delivery/          Gold → BigQuery/Synapse, FastAPI serving
orchestration/       Airflow DAGs (batch · streaming ops · ML training), Beam launcher, Airbyte apply
infra/terraform/     modules/gcp + modules/azure, one selected by `count` on var.cloud; envs/{dev,qa,prod}
databricks/          Asset Bundle: jobs, clusters, continuous streaming workloads
.github/workflows/   ci.yml, cd.yml
jenkins/             Jenkinsfile + shared library
docker/              Dockerfile.jobs (CPU) · Dockerfile.gpu (CUDA 12.4) · Dockerfile.api
tests/               unit (config, dims, expectations, SCD2 merge, transforms) + integration (CDC e2e, smoke)
docs/                ARCHITECTURE · OPTIMIZATIONS · DATASETS · DATA_MODEL · RUNBOOK · adr/
```

A file-by-file description is in [`MANIFEST.md`](MANIFEST.md).

---

## 7. Docs

| Document | Read it when |
|---|---|
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | You want the end-to-end picture and the layer contracts. |
| [`docs/adr/`](docs/adr/README.md) | You want to know *why* — CDC split, liquid clustering, engine choices, promotion gates. |
| [`docs/OPTIMIZATIONS.md`](docs/OPTIMIZATIONS.md) | You are tuning Spark, Delta, streaming, CUDA or cost — each entry says what, where, and why *on this workload*. |
| [`docs/DATASETS.md`](docs/DATASETS.md) | You want the seven Kaggle sources and why they compose. |
| [`docs/DATA_MODEL.md`](docs/DATA_MODEL.md) | You are writing a query or a mart. |
| [`docs/RUNBOOK.md`](docs/RUNBOOK.md) | Something is on fire: CDC lag, crash-looping stream, stale Gold, small files, GPU OOM, rollback. |

## 8. License

MIT — see [`LICENSE`](LICENSE). The Kaggle datasets carry their own licenses; see
[`docs/DATASETS.md`](docs/DATASETS.md).
