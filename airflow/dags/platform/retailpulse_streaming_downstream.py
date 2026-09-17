"""Refresh analytics and validate quality every 15 minutes from existing CDC files.

Initially paused; retailpulse_streaming_end_to_end enables this schedule and
triggers a run after its analytics, quality checks, and docs succeed.
Pause this DAG and let its active run finish before running end-to-end recovery
or another manual pipeline that writes the same Silver and analytics tables.
"""
import sys
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

sys.path.insert(0, "/opt/airflow/include")
from retailpulse.dbt_control import run_dbt_command
from retailpulse.iceberg_control import run_spark_job
from retailpulse.quality_control import run_quality_job


from retailpulse.task_logging import log_task_start

@dag(default_args={"on_execute_callback": log_task_start}, 
    dag_id="retailpulse_streaming_downstream",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 9, 14, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=3),
    tags=["retailpulse", "streaming", "incremental", "dbt", "quality"],
)
def retailpulse_streaming_downstream():
    @task(execution_timeout=timedelta(minutes=45))
    def merge_iceberg_silver():
        return run_spark_job("lakehouse/iceberg/jobs/incremental_cdc_to_silver.py")

    @task(execution_timeout=timedelta(minutes=45))
    def validate_iceberg_silver():
        return run_spark_job("lakehouse/iceberg/jobs/validate_silver.py")

    @task(execution_timeout=timedelta(minutes=45))
    def dbt_build():
        # Build includes the configured dbt tests.
        return run_dbt_command("dbt build --target dev")

    @task(execution_timeout=timedelta(minutes=45))
    def freshness():
        return run_quality_job("quality/check_freshness.py")

    @task(execution_timeout=timedelta(minutes=45))
    def gx_validation():
        return run_quality_job("quality/validate_analytics.py")

    merge_iceberg_silver() >> validate_iceberg_silver() >> dbt_build() >> freshness() >> gx_validation()


dag = retailpulse_streaming_downstream()
