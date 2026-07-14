# ADR-0001: `CLOUD` x `ENVIRONMENT` is the only configuration surface

- **Status:** Accepted
- **Date:** 2026-07-14
- **Deciders:** data-platform

## Context

The platform must run on GCP and Azure, across dev/qa/prod. The usual outcome of that requirement
is two codebases with a shared name: `pipeline_gcp.py` and `pipeline_azure.py` drift within a
quarter, and environment differences leak into `if os.environ["ENV"] == "prod"` branches scattered
across the transformation code. Once that happens, "multi-cloud" means "twice the maintenance",
and a bug fixed in one path silently survives in the other.

## Decision

Two variables — `CLOUD ∈ {gcp, azure}` and `ENVIRONMENT ∈ {dev, qa, prod}` — are read **exactly
once**, in `src/gaming_lakehouse/config.py`. They resolve a layered configuration
(`conf/base.yaml` → `conf/cloud/<CLOUD>.yaml` → `conf/env/<ENVIRONMENT>.yaml` → environment
variables) into a single immutable `Settings` object. Storage URIs (`gs://` vs `abfss://`), broker
(Pub/Sub vs Event Hubs), warehouse (BigQuery vs Synapse), secret backend (Secret Manager vs Key
Vault), LLM provider, cluster sizes, data-quality severity and Terraform module selection are all
*derived properties* of those two variables. No other module reads `os.environ` for these concerns,
and no business logic branches on cloud or environment.

## Consequences

**Positive.** Business logic is written once. A cloud migration is a config change plus a Terraform
apply, not a rewrite. Unit tests can assert the entire cloud contract (`test_config.py`) without
touching a cloud. Onboarding is one page: change two variables, everything follows.

**Negative.** The lowest-common-denominator trap is real: a GCP-only feature (e.g. BigQuery BI
Engine) cannot be used casually — it must be expressed as a capability in the config layer or not
at all. `config.py` becomes a high-traffic file and a single point of failure; it is therefore the
most heavily tested module in the repo.

**Neutral.** Invalid toggle combinations fail at import time rather than at the first cloud call.
That is loud by design.

## Alternatives considered

- **Separate repos per cloud.** Honest about the differences, but guarantees drift and doubles the
  CI matrix, the on-call surface and the review load. Rejected.
- **Abstraction framework (e.g. a cloud-agnostic SDK wrapper).** Adds a dependency whose release
  cadence we do not control, and still leaks at the edges (Auto Loader, Event Hubs Kafka endpoint).
  Rejected in favour of a ~200-line config module we own.
- **Runtime feature flags.** Moves the branch from build time to run time, which is strictly worse:
  the failure surfaces in production instead of in `terraform plan`. Rejected.
