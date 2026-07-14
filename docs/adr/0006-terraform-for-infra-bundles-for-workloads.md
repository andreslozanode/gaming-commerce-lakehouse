# ADR-0006: Terraform for infrastructure, Databricks Asset Bundles for workloads

- **Status:** Accepted
- **Date:** 2026-07-14
- **Deciders:** data-platform

## Context

Infrastructure and workloads change at different speeds and carry different blast radii. Buckets,
key vaults, KMS keys, Postgres instances, Event Hubs namespaces and IAM change rarely, and a bad
change can be unrecoverable. Job definitions, cluster specs, notebooks and wheels change on every
merge, and a bad change is recoverable by redeploying the previous version. If both live in the
same pipeline, then either every job tweak runs a `terraform plan` against production KMS (slow and
terrifying), or the deploy gets an override switch that eventually gets used on the wrong thing.

## Decision

Split them, physically and in CI:

- **Terraform** (`infra/terraform`, `modules/gcp` + `modules/azure`, one module selected by `count`
  on the `CLOUD` variable) owns **infrastructure**: storage layers with lifecycle rules and CMEK,
  KMS/Key Vault, Pub/Sub topics and schemas / Event Hubs, BigQuery / Synapse, Composer, Cloud SQL /
  Postgres Flexible Server with logical decoding enabled, workload identity federation, IAM, budgets.
  Remote state per `(cloud, environment)`. Every apply passes a plan/approval gate.
- **Databricks Asset Bundles** (`databricks/`) own **workloads**: jobs, continuous streaming jobs,
  cluster definitions, the Python wheel. Deployed on every merge, per target (`dev`/`qa`/`prod`).
- **Airbyte GitOps** (`orchestration/airbyte/apply_config.py`) owns connectors; Composer bucket-sync
  (GCP) / git-sync (Azure) owns DAG delivery.

**A workload deploy cannot touch infrastructure.** The bundle has no permission to.

## Consequences

**Positive.** Job deploys are fast and boring; infrastructure changes are slow and reviewed — which
is the correct assignment of friction. Blast radius is bounded by the tool: the worst a bad bundle
deploy can do is break a job, which `databricks bundle deploy` of the previous commit undoes.

**Negative.** Two deployment mechanisms in the CD pipeline, and a genuine ordering dependency: a new
job that reads a new bucket needs the Terraform stage to run first. The `cd.yml` workflow encodes
that ordering explicitly (`resolve → terraform → databricks → orchestration → smoke → promote`).

**Neutral.** Cluster policies straddle the line conceptually. They are placed with the workloads,
because in practice they change with the jobs.

## Alternatives considered

- **Terraform for everything, including Databricks jobs** (via the Databricks provider). Puts a
  `terraform apply` in the path of every job change, drags job state into infrastructure state, and
  makes a routine job edit capable of proposing a KMS diff. Rejected.
- **Bundles for everything.** Bundles do not manage cloud infrastructure, and pretending otherwise
  means hand-clicked buckets. Rejected.
- **Pulumi / CDK.** Fine tools; no advantage here that outweighs Terraform's ubiquity in this team
  and its state model across two clouds. Rejected.
