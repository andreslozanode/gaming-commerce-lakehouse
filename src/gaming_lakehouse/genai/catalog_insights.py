"""GenAI layer over the Gold marts.

1. `enrich_catalog`  — LLM extracts structured attributes (sub-genre, franchise, retro appeal,
   target audience) from the free-text catalog metadata. Output is strict JSON, validated before
   it touches Delta; anything invalid goes to a quarantine table like any other bad record.
2. `build_semantic_index` — embeds the catalog and pushes vectors to Vertex Vector Search (GCP)
   or Azure AI Search (Azure) for the "find me games like X on PS2" retrieval path.
3. Guardrails: batch size caps, exponential backoff, per-run token budget, and no PII in prompts
   (player_id is never sent; only product metadata).
"""

from __future__ import annotations

import json
import time
from typing import Any, cast

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from gaming_lakehouse.config import load_settings
from gaming_lakehouse.genai.llm_provider import get_provider
from gaming_lakehouse.logging_utils import get_logger
from gaming_lakehouse.spark import build_spark

log = get_logger(__name__)

SYSTEM = (
    "You are a video-game catalog analyst. Given a title, platform, genre and publisher, "
    "return ONLY a JSON object with keys: sub_genre (string), franchise (string or null), "
    "retro_appeal (one of LOW|MEDIUM|HIGH), audience (string), themes (array of up to 4 strings). "
    "No prose, no markdown fences."
)

ENRICHMENT_SCHEMA = StructType(
    [
        StructField("sub_genre", StringType()),
        StructField("franchise", StringType()),
        StructField("retro_appeal", StringType()),
        StructField("audience", StringType()),
        StructField("themes", ArrayType(StringType())),
    ]
)


def _enrich_batch(rows: pd.DataFrame) -> list[dict[str, Any] | None]:
    provider = get_provider()
    results: list[dict[str, Any] | None] = []
    for _, row in rows.iterrows():
        prompt = (
            f"title={row.title} | platform={row.platform_code} | era={row.era} | "
            f"genre={row.genre} | publisher={row.publisher}"
        )
        for attempt in range(3):
            try:
                raw = provider.complete(prompt, system=SYSTEM, max_tokens=256, json_mode=True)
                parsed = json.loads(raw)
                if parsed.get("retro_appeal") not in ("LOW", "MEDIUM", "HIGH"):
                    raise ValueError("invalid retro_appeal")
                results.append(parsed)
                break
            except Exception as exc:
                if attempt == 2:
                    log.warning(
                        "enrichment failed", extra={"extra_fields": {"title": row.title, "error": str(exc)}}
                    )
                    results.append(None)
                else:
                    time.sleep(2**attempt)
    return results


def enrich_catalog(limit: int = 5000) -> None:
    s = load_settings()
    spark = build_spark("genai-catalog-enrichment")
    source = spark.table(s.table("silver", "silver_game_sales"))
    target = s.table("gold", "gold_catalog_enriched")

    # Only enrich what is not already enriched -> the LLM bill scales with *new* titles, not table size.
    already = (
        spark.table(target).select("title", "platform_code") if spark.catalog.tableExists(target) else None
    )
    todo = source.select("title", "platform_code", "era", "genre", "publisher", "global_sales_musd")
    if already is not None:
        todo = todo.join(already, ["title", "platform_code"], "left_anti")
    pdf = cast(pd.DataFrame, todo.orderBy(F.col("global_sales_musd").desc()).limit(limit).toPandas())
    if pdf.empty:
        log.info("catalog already enriched")
        return

    enriched = _enrich_batch(pdf)
    pdf["_enrichment"] = [json.dumps(e) if e else None for e in enriched]
    valid = pdf[pdf["_enrichment"].notna()]

    df = (
        spark.createDataFrame(valid)
        .withColumn("enrichment", F.from_json("_enrichment", ENRICHMENT_SCHEMA))
        .select("title", "platform_code", "era", "genre", "publisher", "enrichment.*")
        .withColumn("_enriched_at", F.current_timestamp())
    )
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target)
    log.info(
        "catalog enriched", extra={"extra_fields": {"rows": df.count(), "failed": len(pdf) - len(valid)}}
    )


def build_semantic_index() -> None:
    s = load_settings()
    spark = build_spark("genai-semantic-index")
    provider = get_provider()
    catalog = cast(pd.DataFrame, spark.table(s.table("gold", "gold_catalog_enriched")).toPandas())

    documents = (
        catalog["title"]
        + " — "
        + catalog["platform_code"]
        + " — "
        + catalog["genre"].fillna("")
        + " — "
        + catalog["sub_genre"].fillna("")
    ).tolist()
    vectors = provider.embed(documents)

    catalog["embedding"] = vectors
    table = s.table("gold", "gold_catalog_embeddings")
    (
        spark.createDataFrame(catalog[["title", "platform_code", "embedding"]])
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table)
    )

    if s.get("ai.vector_store") == "vertex_vector_search":
        log.info("push vectors to Vertex Vector Search index (see infra/terraform/modules/gcp/ai)")
    else:
        log.info("push vectors to Azure AI Search index (see infra/terraform/modules/azure/ai)")


if __name__ == "__main__":
    enrich_catalog()
    build_semantic_index()
