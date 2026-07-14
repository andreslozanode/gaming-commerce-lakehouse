# ADR-0009: GenAI behind a provider interface (Vertex/Gemini | Azure OpenAI)

- **Status:** Accepted
- **Date:** 2026-07-14
- **Deciders:** data-platform

## Context

The GenAI layer enriches the title catalogue (thematic tags, audience descriptors, positioning
summaries used by the marts and by semantic search) and builds a semantic index. The natural
implementation — call the cloud's LLM SDK where the job happens to run — would put a
`if cloud == "gcp"` branch inside the enrichment logic, break ADR-0001, and pin the prompt layer to
a vendor's SDK release cycle. LLM APIs also move faster than any other dependency in this repo.

## Decision

`genai/llm_provider.py` defines an abstract `LLMProvider` with two implementations —
`VertexProvider` (Gemini) and `AzureOpenAIProvider` — selected by `get_provider()` from the `CLOUD`
toggle. `genai/catalog_insights.py` depends only on the interface.

Two rules constrain the layer:

1. **No PII in prompts.** `player_id` and any customer attribute never leave the lakehouse. The
   enrichment operates on catalogue entities (title, platform, genre, release year), not on people.
2. **Enrichment is a `left_anti` join against what is already enriched.** Only genuinely new titles
   are sent to the model, so the LLM bill scales with catalogue *growth*, not with catalogue *size*
   or with DAG frequency. Re-running the DAG on an unchanged catalogue costs nothing.

Outputs are written to a Delta table (auditable, diffable, replayable); the semantic index is built
into Vertex AI Vector Search or Azure AI Search, again by the same toggle.

## Consequences

**Positive.** The prompt and the schema of the enrichment are portable and reviewable. Swapping
model versions is a config change. Cost is bounded by construction rather than by vigilance. Because
the output lands in Delta, a bad enrichment run is a `RESTORE`, not an incident.

**Negative.** The interface is the lowest common denominator of two providers; provider-specific
features (e.g. Gemini's long context, Azure OpenAI's structured-output mode) are only usable if they
can be expressed for both, or degraded gracefully. Output quality differs between providers and the
enrichment is therefore not bit-for-bit reproducible across clouds — the Delta table records which
provider and model produced each row.

**Neutral.** LLM output is treated as *data*, not as *logic*: nothing in the pipeline branches on it.

## Alternatives considered

- **Call the cloud SDK directly at the call site.** Fastest to write, violates ADR-0001, and makes
  the enrichment untestable without a cloud. Rejected.
- **A framework (LangChain et al.).** Buys abstraction we do not need at this scope and adds a large
  dependency surface to a repo that already has Spark, Beam, Torch and TF. Rejected.
- **Self-hosted OSS model on the GPU cluster.** Attractive on cost at high volume; not at this
  volume, given the `left_anti` bound above. Revisit if enrichment volume grows by an order of
  magnitude.
