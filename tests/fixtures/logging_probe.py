"""Opt-in smoke DAG; copy into dags/ only while verifying a deployment."""

from datetime import datetime, timezone
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator


def emit_probe_message(**context):
    logging.getLogger(__name__).info(
        "AIRFLOW_LOGGING_PROBE dag=%s run=%s task=%s",
        context["dag"].dag_id, context["run_id"], context["task"].task_id,
    )


with DAG(
    dag_id="airflow_logging_verification",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    tags=["maintenance", "logging-verification"],
) as dag:
    for number in range(32):
        PythonOperator(task_id=f"log_probe_{number:02d}", python_callable=emit_probe_message)
