"""Weekly ML retraining: PyTorch two-tower (GPU) + TensorFlow forecaster (GPU) -> MLflow -> gate.

Promotion gate: a new model only becomes @champion if it beats the incumbent on the holdout
metric by a configured margin. Otherwise the run is logged, the alias is untouched, and the
pipeline exits green — a non-improving model is not a failure, it is information.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import task
from airflow.models import DAG, Variable
from airflow.operators.python import ShortCircuitOperator
from callbacks import on_failure_alert
from dag_batch_medallion import databricks_task

ENVIRONMENT = Variable.get("ENVIRONMENT", default_var="dev")
MIN_IMPROVEMENT = float(Variable.get("MIN_MODEL_IMPROVEMENT", default_var="0.01"))

with DAG(
    dag_id=f"gc_ml_training_{ENVIRONMENT}",
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * 1",
    catchup=False,
    default_args={
        "owner": "ml-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "on_failure_callback": on_failure_alert,
    },
    tags=["ml", "gpu", "mlflow", ENVIRONMENT],
) as dag:
    features = databricks_task("refresh_features", "ml/features.py")
    torch_train = databricks_task("train_two_tower", "ml/train_torch_recsys.py", gpu=True)
    tf_train = databricks_task("train_forecaster", "ml/train_tf_forecast.py", gpu=True)

    @task
    def evaluate_challenger() -> dict[str, float]:
        import mlflow

        client = mlflow.MlflowClient()
        model = f"gc_{ENVIRONMENT}.models.gc_two_tower_recsys"
        challenger = client.get_model_version_by_alias(model, "challenger")
        try:
            champion = client.get_model_version_by_alias(model, "champion")
            champion_metric = client.get_run(champion.run_id).data.metrics.get("val_recall_at_10", 0.0)
        except Exception:
            champion_metric = 0.0
        challenger_metric = client.get_run(challenger.run_id).data.metrics.get("val_recall_at_10", 0.0)
        return {
            "challenger": challenger_metric,
            "champion": champion_metric,
            "delta": challenger_metric - champion_metric,
        }

    def _gate(**context) -> bool:
        metrics = context["ti"].xcom_pull(task_ids="evaluate_challenger")
        return metrics["delta"] >= MIN_IMPROVEMENT

    gate = ShortCircuitOperator(
        task_id="promotion_gate", python_callable=_gate, ignore_downstream_trigger_rules=False
    )

    @task
    def promote() -> None:
        import mlflow

        client = mlflow.MlflowClient()
        model = f"gc_{ENVIRONMENT}.models.gc_two_tower_recsys"
        challenger = client.get_model_version_by_alias(model, "challenger")
        client.set_registered_model_alias(model, "champion", challenger.version)

    score = databricks_task("batch_inference", "ml/serving/batch_inference.py")

    features >> [torch_train, tf_train] >> evaluate_challenger() >> gate >> promote() >> score
