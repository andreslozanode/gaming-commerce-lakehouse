# ADR-0010: Keyless federated identity everywhere; no long-lived credentials

- **Status:** Accepted
- **Date:** 2026-07-14
- **Deciders:** data-platform

## Context

The platform authenticates in a lot of places: CI to two clouds, Databricks jobs to storage, Spark
to Postgres, Debezium to the WAL, the serving API to the warehouse, everything to the secret store.
The default path — a service-account JSON key or a storage account key, pasted into a CI secret and
into a cluster config — produces credentials that never expire, are copied by hand, and appear in
the one place they must never appear: a stack trace, a notebook, or a commit.

## Decision

No long-lived credentials anywhere in the repo or in CI.

- **CI → cloud:** GitHub OIDC. On GCP, a Workload Identity Federation pool/provider trusts the
  repository's OIDC issuer and impersonates the pipeline service account. On Azure, a user-assigned
  managed identity with a federated credential does the same. Both are created in Terraform
  (`modules/gcp`, `modules/azure`); the CD workflow holds *no* cloud key.
- **Compute → storage:** the job's identity (service account / managed identity) is authorized on the
  layer buckets or ADLS containers. **Storage account keys are never inlined** in a Spark conf.
- **Secrets:** resolved at run time by `secrets.py` through Secret Manager (GCP) or Key Vault
  (Azure), with a `dbutils` path on Databricks and an environment-variable fallback for local dev
  only. The resolution order is fixed and cloud-aware; the caller asks for a name, not a location.
- **Debezium → Postgres:** a dedicated `debezium` role with `REPLICATION` and `SELECT` on a
  five-table publication. Nothing else.
- CI runs `gitleaks` on every push; `.env` is git-ignored and `.env.example` contains only shapes.

## Consequences

**Positive.** There is no key to leak, rotate, or forget to rotate. Access is bounded in time (OIDC
tokens are minutes-lived) and in scope (per-environment identities). Revoking CI access is a
Terraform change, not a scramble through a secret store.

**Negative.** Federation is fiddly to set up and its failure modes are opaque — an expired trust
condition surfaces as a generic 403 in a CI log. `RUNBOOK.md` documents the specific error shapes
for both clouds. Local development needs its own story (ADC / `az login`), which is a small tax on
every new developer.

**Neutral.** Anything that genuinely cannot federate (the Kaggle API token, for one) is a secret in
the secret store with an owner and an expiry — the exception is explicit and enumerable, rather than
the norm.

## Alternatives considered

- **Service-account JSON keys / storage account keys in CI secrets.** The status quo of a thousand
  repos, and the origin of a thousand incidents. Rejected.
- **A self-hosted runner with an attached instance identity.** Solves CI→cloud, does not solve
  compute→storage or the secret store, and adds a machine to own. Rejected.
- **Vault as a central secret broker.** A good answer if we were cloud-neutral in the secret layer
  too, but it adds an HA service to operate in order to abstract two managed services we already
  pay for. Rejected.
