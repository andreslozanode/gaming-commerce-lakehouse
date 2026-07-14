"""Batch ingestion entrypoint: Kaggle -> landing -> Bronze Delta (Auto Loader semantics).

python -m gaming_lakehouse.ingestion.run_ingest --datasets all
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyspark.sql import functions as F

from gaming_lakehouse.config import load_settings
from gaming_lakehouse.ingestion.kaggle_client import download_dataset
from gaming_lakehouse.logging_utils import get_logger
from gaming_lakehouse.spark import build_spark
from gaming_lakehouse.storage import checkpoint_path, schema_path

log = get_logger(__name__)


def land_to_bronze(spark, dataset_key: str, landing_uri: str, bronze_table: str) -> int:
    """Incremental file discovery. Auto Loader on Databricks; cloudFiles falls back to
    a plain readStream on OSS Spark, keeping the same checkpoint contract."""
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_path(f"bronze_{dataset_key}"))
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.maxFilesPerTrigger", 256)
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued_data")  # nothing is ever silently dropped
    )
    df = (
        reader.load(landing_uri)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_dataset_key", F.lit(dataset_key))
    )
    query = (
        df.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path(f"bronze_{dataset_key}"))
        .option("mergeSchema", "true")
        .trigger(availableNow=True)  # batch semantics, streaming bookkeeping
        .toTable(bronze_table)
    )
    query.awaitTermination()
    return int(spark.table(bronze_table).count())


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--datasets", default="all", help="comma-separated keys, or 'all'")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--parallelism", type=int, default=4)
    args = parser.parse_args()

    settings = load_settings()
    catalog = settings.get("datasets", {})
    keys = list(catalog) if args.datasets == "all" else args.datasets.split(",")

    # Kaggle downloads are I/O bound -> thread pool. Spark writes stay serial per table.
    results = {}
    with ThreadPoolExecutor(max_workers=args.parallelism) as pool:
        futures = {pool.submit(download_dataset, key, force=args.force): key for key in keys}
        for future in as_completed(futures):
            key = futures[future]
            results[key] = future.result()

    spark = build_spark("kaggle-ingest", streaming=True)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {settings.table('bronze', 'x').rsplit('.', 1)[0]}")

    for key, result in results.items():
        table = settings.table("bronze", catalog[key]["bronze_table"])
        rows = land_to_bronze(spark, key, result.landing_uri, table)
        log.info(
            "bronze refreshed",
            extra={"extra_fields": {"dataset": key, "table": table, "rows": rows, "skipped": result.skipped}},
        )


if __name__ == "__main__":
    main()
