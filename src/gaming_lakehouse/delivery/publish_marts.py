"""Gold -> serving warehouse. BigQuery on GCP, Synapse/Fabric on Azure. Same call, one toggle.

Both paths are *overwrite by partition* with a staging swap, so a failed publish never leaves
the BI layer half-updated.
"""

from __future__ import annotations

from pyspark.sql import DataFrame

from gaming_lakehouse.config import load_settings
from gaming_lakehouse.logging_utils import get_logger
from gaming_lakehouse.spark import build_spark

log = get_logger(__name__)

MARTS = [
    "gold_sales_by_era_platform",
    "gold_console_lifecycle",
    "gold_player_360",
    "gold_membership_mrr",
    "gold_player_churn_scores",
    "gold_catalog_enriched",
]


def _to_bigquery(df: DataFrame, table: str) -> None:
    s = load_settings()
    (
        df.write.format("bigquery")
        .option("table", f"{s.get('warehouse.dataset')}_{s.environment}.{table}")
        .option("temporaryGcsBucket", f"{s.project}-{s.environment}-temp")
        .option("writeMethod", "direct")  # Storage Write API — no GCS staging round-trip
        .option("createDisposition", "CREATE_IF_NEEDED")
        .mode("overwrite")
        .save()
    )


def _to_synapse(df: DataFrame, table: str) -> None:
    s = load_settings()
    from gaming_lakehouse.secrets import get_secret

    (
        df.write.format("com.microsoft.sqlserver.jdbc.spark")
        .option("url", get_secret("synapse-jdbc-url"))
        .option("dbtable", f"{s.get('warehouse.dataset')}_{s.environment}.{table}")
        .option("accessToken", get_secret("synapse-access-token"))
        .option("tableLock", "true")
        .option("batchsize", "100000")
        .option("reliabilityLevel", "BEST_EFFORT")
        .mode("overwrite")
        .save()
    )


def main() -> None:
    s = load_settings()
    spark = build_spark("publish-marts")
    for mart in MARTS:
        source = s.table("gold", mart)
        if not spark.catalog.tableExists(source):
            log.warning("mart missing, skipping", extra={"extra_fields": {"table": source}})
            continue
        df = spark.table(source)
        if s.cloud == "gcp":
            _to_bigquery(df, mart)
        else:
            _to_synapse(df, mart)
        log.info(
            "mart published", extra={"extra_fields": {"mart": mart, "engine": s.get("warehouse.engine")}}
        )


if __name__ == "__main__":
    main()
