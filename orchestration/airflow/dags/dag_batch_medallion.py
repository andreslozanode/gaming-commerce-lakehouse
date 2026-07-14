"""Daily batch medallion: Kaggle -> Bronze -> Silver -> Gold -> Features -> Marts.

One DAG, three environments. The environment comes from the Airflow Variable `ENVIRONMENT`
and the cloud from `CLOUD`, so the same DAG file is deployed to Composer (GCP) and to the
self-managed Airflow on AKS (Azure) without a diff.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import task
from airflow.models import DAG, Variable
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from callbacks import on_failure_alert, on_sla_miss  # plugins/

CLOUD = Variable.get("CLOUD", default_var="gcp")
ENVIRONMENT = Variable.get("ENVIRONMENT", default_var="dev")
DATASETS = [
    "vgchartz_sales_classic",
    "vgchartz_sales_extended",
    "vgchartz_with_ratings",
    "gaming_profiles",
    "xbox_game_pass",
    "playstation_catalog_ps4",
    "playstation_catalog_ps5",
]

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": on_failure_alert,
    "sla": timedelta(hours=2),
    "depends_on_past": False,
}


def databricks_task(task_id: str, entrypoint: str, params: list[str] | None = None, gpu: bool = False):
    """Submit a Python task to Databricks. Job clusters are ephemeral and sized by env."""
    from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

    node_type = {"gcp": "n2-standard-8", "azure": "Standard_D8ds_v5"}[CLOUD]
    gpu_node = {"gcp": "a2-highgpu-1g", "azure": "Standard_NC24ads_A100_v4"}[CLOUD]
    workers = {"dev": 2, "qa": 4, "prod": 8}[ENVIRONMENT]

    return DatabricksSubmitRunOperator(
        task_id=task_id,
        databricks_conn_id=f"databricks_{CLOUD}_{ENVIRONMENT}",
        new_cluster={
            "spark_version": "15.4.x-gpu-ml-scala2.12" if gpu else "15.4.x-scala2.12",
            "node_type_id": gpu_node if gpu else node_type,
            "num_workers": 0 if gpu else workers,
            "data_security_mode": "SINGLE_USER",
            "spark_env_vars": {"CLOUD": CLOUD, "ENVIRONMENT": ENVIRONMENT},
            "spark_conf": {"spark.databricks.delta.preview.enabled": "true"},
            "autoscale": None if gpu else {"min_workers": max(workers // 2, 1), "max_workers": workers},
            **({"aws_attributes": {}} if False else {}),
        },
        spark_python_task={
            "python_file": f"dbfs:/Shared/gaming_commerce/{ENVIRONMENT}/{entrypoint}",
            "parameters": params or [],
        },
        libraries=[
            {"whl": f"dbfs:/Shared/gaming_commerce/{ENVIRONMENT}/gaming_lakehouse-latest-py3-none-any.whl"}
        ],
    )


with DAG(
    dag_id=f"gc_batch_medallion_{ENVIRONMENT}",
    description="Kaggle -> Bronze -> Silver -> Gold -> Marts",
    start_date=datetime(2026, 1, 1),
    schedule="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    sla_miss_callback=on_sla_miss,
    tags=["medallion", "kaggle", CLOUD, ENVIRONMENT],
) as dag:
    start = EmptyOperator(task_id="start")

    with TaskGroup("ingest") as ingest:
        # Datasets are independent -> Airflow parallelizes them; each is idempotent (checksum-gated).
        for key in DATASETS:
            databricks_task(f"ingest_{key}", "ingestion/run_ingest.py", ["--datasets", key])

    @task(task_id="assert_bronze_freshness")
    def assert_freshness() -> None:
        """Fail fast if Bronze did not move — cheaper than discovering it in Gold."""
        from gaming_lakehouse.config import load_settings
        from gaming_lakehouse.spark import build_spark

        spark = build_spark("freshness-check")
        s = load_settings()
        stale = []
        for key in DATASETS:
            table = s.table("bronze", s.get(f"datasets.{key}.bronze_table"))
            # S608: {table} is resolved by Settings.table() from conf/, never from request data.
            rows = spark.sql(f"SELECT count(*) c FROM {table}").first()["c"]  # noqa: S608
            if rows == 0:
                stale.append(table)
        if stale:
            raise ValueError(f"Empty bronze tables: {stale}")

    silver_dims = databricks_task("silver_dims", "transform/bronze_to_silver.py", ["--target", "dims"])
    silver_sales = databricks_task("silver_sales", "transform/bronze_to_silver.py", ["--target", "sales"])
    silver_events = databricks_task("silver_events", "transform/bronze_to_silver.py", ["--target", "events"])
    gold = databricks_task("gold_marts", "transform/silver_to_gold.py")
    features = databricks_task("features", "ml/features.py")
    genai = databricks_task("genai_enrichment", "genai/catalog_insights.py")
    publish = databricks_task("publish_marts", "delivery/publish_marts.py")
    maintenance = databricks_task("delta_maintenance", "transform/optimize_maintenance.py")
    end = EmptyOperator(task_id="end")

    (
        start
        >> ingest
        >> assert_freshness()
        >> silver_dims
        >> [silver_sales, silver_events]
        >> gold
        >> [features, genai]
        >> publish
        >> maintenance
        >> end
    )
