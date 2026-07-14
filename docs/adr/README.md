# Architecture Decision Records

Each ADR records one decision that was expensive to make and would be expensive to reverse.
They are immutable: a decision that no longer holds is not edited, it is superseded by a new ADR.

| ADR | Decision | Status |
|-----|----------|--------|
| [0001](0001-two-variable-configuration-model.md) | `CLOUD` x `ENVIRONMENT` is the only configuration surface | Accepted |
| [0002](0002-medallion-plus-event-driven-on-delta.md) | Medallion layers + event-driven ingress, Delta Lake as the single table format | Accepted |
| [0003](0003-debezium-for-realtime-airbyte-for-batch-cdc.md) | Debezium owns real-time CDC; Airbyte owns batch CDC and the SaaS long tail | Accepted |
| [0004](0004-two-streaming-engines-beam-and-structured-streaming.md) | Two streaming engines behind one Avro contract | Accepted |
| [0005](0005-liquid-clustering-over-zorder.md) | Liquid clustering instead of `ZORDER BY` on Silver/Gold | Accepted |
| [0006](0006-terraform-for-infra-bundles-for-workloads.md) | Terraform for infrastructure, Databricks Asset Bundles for workloads | Accepted |
| [0007](0007-scd2-in-silver-via-merge.md) | CDC history is materialised as SCD Type 2 in Silver via `MERGE` | Accepted |
| [0008](0008-mlflow-champion-challenger-promotion.md) | Model promotion is a CI gate over MLflow aliases, not a human decision | Accepted |
| [0009](0009-provider-agnostic-genai-layer.md) | GenAI behind a provider interface (Vertex/Gemini \| Azure OpenAI) | Accepted |
| [0010](0010-keyless-federated-identity.md) | Keyless federated identity everywhere; no long-lived credentials | Accepted |

New ADRs start from [`template.md`](template.md).
