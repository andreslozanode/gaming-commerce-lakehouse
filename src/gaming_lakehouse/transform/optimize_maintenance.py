"""Nightly table maintenance. Runs on a small job cluster; cheap, and the single largest
contributor to query-cost reduction after clustering.

  OPTIMIZE  -> compact small files produced by streaming micro-batches
  VACUUM    -> reclaim storage past the retention window (env-configurable)
  ANALYZE   -> refresh statistics so AQE and CBO make the right join choices
  REORG     -> purge deletion vectors after heavy MERGE traffic
"""

from __future__ import annotations

from gaming_lakehouse.config import load_settings
from gaming_lakehouse.logging_utils import get_logger
from gaming_lakehouse.spark import build_spark

log = get_logger(__name__)


def main() -> None:
    s = load_settings()
    spark = build_spark("delta-maintenance")
    retention = s.get("retention.vacuum_hours", 168)

    for layer in ("bronze", "silver", "gold"):
        catalog_schema = s.table(layer, "x").rsplit(".", 1)[0]
        tables = [
            r.tableName
            for r in spark.sql(f"SHOW TABLES IN {catalog_schema}").collect()
            if not r.tableName.startswith("_")
        ]
        for name in tables:
            fq = f"{catalog_schema}.{name}"
            try:
                spark.sql(f"OPTIMIZE {fq}")
                if layer != "bronze":  # keep Bronze history longer for replays
                    spark.sql(f"REORG TABLE {fq} APPLY (PURGE)")
                spark.sql(f"VACUUM {fq} RETAIN {retention} HOURS")
                spark.sql(f"ANALYZE TABLE {fq} COMPUTE STATISTICS")
                log.info("maintained", extra={"extra_fields": {"table": fq, "retain_h": retention}})
            except Exception as exc:
                log.warning("maintenance failed", extra={"extra_fields": {"table": fq, "error": str(exc)}})


if __name__ == "__main__":
    main()
