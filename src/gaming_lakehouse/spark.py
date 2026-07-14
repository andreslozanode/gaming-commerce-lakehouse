"""SparkSession factory. Every optimization we rely on is declared here, once.

Rationale for each flag is in docs/OPTIMIZATIONS.md — this file must stay the only place
where Spark/Delta tuning is set, so a change is a one-line diff instead of a manhunt.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from gaming_lakehouse.config import Settings, load_settings
from gaming_lakehouse.logging_utils import get_logger

log = get_logger(__name__)


def _cloud_auth_conf(settings: Settings) -> dict[str, str]:
    """Storage auth. Prefer keyless federation; never inline account keys."""
    if settings.cloud == "gcp":
        return {
            "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            "spark.hadoop.google.cloud.auth.service.account.enable": "true",
            # Workload Identity Federation on GKE/Composer; DBFS mounts are not used.
            "spark.hadoop.fs.gs.inputstream.fadvise": "AUTO",
            "spark.hadoop.fs.gs.status.parallel.enable": "true",
        }
    account = f"{settings.short_project[:16]}{settings.environment}dls"
    return {
        f"spark.hadoop.fs.azure.account.auth.type.{account}.dfs.core.windows.net": "OAuth",
        f"spark.hadoop.fs.azure.account.oauth.provider.type.{account}.dfs.core.windows.net": "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider",  # Managed Identity
        "spark.hadoop.fs.azure.enable.readahead": "true",
    }


def build_spark(app_name: str, *, gpu: bool = False, streaming: bool = False) -> SparkSession:
    settings = load_settings()
    shuffle = settings.get("spark.shuffle_partitions", 200)
    broadcast_mb = settings.get("spark.autoBroadcastJoinThreshold_mb", 64)

    conf: dict[str, str] = {
        # --- Adaptive Query Execution: the single biggest win on skewed game/platform joins ---
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128m",
        "spark.sql.adaptive.localShuffleReader.enabled": "true",
        # --- Join / pruning ---
        "spark.sql.autoBroadcastJoinThreshold": f"{broadcast_mb}m",
        "spark.sql.optimizer.dynamicPartitionPruning.enabled": "true",
        "spark.sql.shuffle.partitions": str(shuffle),
        # --- Delta Lake ---
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.databricks.delta.autoCompact.enabled": "true",
        "spark.databricks.delta.properties.defaults.enableDeletionVectors": "true",
        "spark.databricks.delta.properties.defaults.enableChangeDataFeed": "true",
        "spark.databricks.delta.schema.autoMerge.enabled": "false",  # explicit evolution only
        "spark.databricks.delta.retentionDurationCheck.enabled": "true",
        # --- I/O & serialization ---
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.parquet.compression.codec": "zstd",
        "spark.sql.files.maxPartitionBytes": "128m",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.execution.arrow.maxRecordsPerBatch": "20000",
        # --- Reliability ---
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.ansi.enabled": "true",
        "spark.speculation": "false",  # kills idempotency on MERGE-heavy jobs
    }

    if streaming:
        conf.update(
            {
                "spark.sql.streaming.stateStore.providerClass": "com.databricks.sql.streaming.state.RocksDBStateStoreProvider",
                "spark.sql.streaming.statefulOperator.checkCorrectness.enabled": "true",
                "spark.sql.streaming.metricsEnabled": "true",
                "spark.sql.streaming.noDataMicroBatches.enabled": "false",
            }
        )

    if gpu:
        # RAPIDS Accelerator for Spark — used only on the GPU cluster (feature prep for the recsys).
        conf.update(
            {
                "spark.plugins": "com.nvidia.spark.SQLPlugin",
                "spark.rapids.sql.enabled": "true",
                "spark.rapids.sql.concurrentGpuTasks": "2",
                "spark.rapids.memory.pinnedPool.size": "2G",
                "spark.executor.resource.gpu.amount": "1",
                "spark.task.resource.gpu.amount": "0.25",
            }
        )

    conf.update(_cloud_auth_conf(settings))

    builder = SparkSession.builder.appName(f"{settings.project}::{app_name}::{settings.environment}")
    for key, value in conf.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    log.info(
        "spark session ready", extra={"extra_fields": {"app": app_name, "cloud": settings.cloud, "gpu": gpu}}
    )
    return spark
