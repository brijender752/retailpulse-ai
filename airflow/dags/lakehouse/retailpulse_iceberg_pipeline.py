from __future__ import annotations
import sys
from datetime import timedelta
import pendulum

sys.path.insert(0, "/opt/airflow/include")

from airflow.sdk import dag, task
from retailpulse.health import check_flink, check_minio
from retailpulse.minio_validation import validate_streaming_outputs
from retailpulse.iceberg_control import run_spark_job

@dag(
    dag_id="retailpulse_iceberg_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 10, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=60),
    tags=["retailpulse", "iceberg", "lakehouse"],
)
def retailpulse_iceberg_pipeline():

    @task
    def flink_health():
        return check_flink()

    @task
    def minio_health():
        return check_minio()

    @task
    def validate_sources():
        return validate_streaming_outputs()

    @task
    def create_namespaces():
        return run_spark_job(
            "lakehouse/iceberg/jobs/create_namespaces.py"
        )

    @task
    def bootstrap():
        return run_spark_job(
            "lakehouse/iceberg/jobs/bootstrap_minio_to_iceberg.py"
        )

    @task
    def validate_iceberg():
        return run_spark_job(
            "lakehouse/iceberg/jobs/validate_iceberg.py"
        )

    @task
    def build_silver():
        return run_spark_job(
            "lakehouse/iceberg/jobs/build_silver_current_state.py"
        )

    @task
    def validate_silver():
        return run_spark_job(
            "lakehouse/iceberg/jobs/validate_silver.py"
        )

    flink = flink_health()
    minio = minio_health()
    source = validate_sources()
    ns = create_namespaces()
    boot = bootstrap()
    valid = validate_iceberg()
    silver = build_silver()
    silver_ok = validate_silver()

    [flink, minio] >> source
    source >> ns >> boot >> valid >> silver >> silver_ok

dag = retailpulse_iceberg_pipeline()
