
from __future__ import annotations
import sys
from datetime import timedelta
import pendulum

sys.path.insert(0, "/opt/airflow/include")

from airflow.sdk import dag, task
from retailpulse.health import check_flink, check_minio
from retailpulse.iceberg_control import run_spark_job

from retailpulse.task_logging import log_task_start

@dag(default_args={"on_execute_callback": log_task_start}, 
    dag_id="retailpulse_iceberg_incremental",
    schedule=None,  # Manual; end-to-end streaming owns the ordered CDC/dbt run.
    start_date=pendulum.datetime(2026, 9, 10, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    tags=["retailpulse", "iceberg", "incremental", "cdc"],
)
def retailpulse_iceberg_incremental():

    @task
    def flink_health():
        return check_flink()

    @task
    def minio_health():
        return check_minio()

    @task
    def incremental_silver():
        return run_spark_job(
            "lakehouse/iceberg/jobs/incremental_cdc_to_silver.py"
        )

    @task
    def gold_marts():
        return run_spark_job(
            "lakehouse/iceberg/jobs/build_gold_marts.py"
        )

    @task
    def validate():
        return run_spark_job(
            "lakehouse/iceberg/jobs/validate_incremental_pipeline.py"
        )

    f = flink_health()
    m = minio_health()
    s = incremental_silver()
    g = gold_marts()
    v = validate()

    [f, m] >> s >> g >> v

dag = retailpulse_iceberg_incremental()
