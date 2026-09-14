"""One manual run from source initialization through tested dbt analytics."""
import sys
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task
from airflow.providers.standard.sensors.python import PythonSensor

sys.path.insert(0, "/opt/airflow/include")
from retailpulse.streaming_setup import initialize_source, ensure_bucket, ensure_connector, bronze_files_ready
from retailpulse.health import check_debezium, check_flink, check_kafka
from retailpulse.flink_control import ensure_flink_job, validate_job_checkpoint
from retailpulse.jobs import FLINK_JOBS
from retailpulse.iceberg_control import run_spark_job
from retailpulse.dbt_control import run_dbt_command


@dag(dag_id="retailpulse_streaming_end_to_end", schedule=None,
     start_date=pendulum.datetime(2026, 9, 14, tz="UTC"), catchup=False,
     max_active_runs=1, dagrun_timeout=timedelta(hours=3),
     tags=["retailpulse", "streaming", "dbt", "manual"])
def retailpulse_streaming_end_to_end():
    source = task(initialize_source)()
    bucket = task(ensure_bucket)()
    connector = task(ensure_connector)()
    source >> connector
    connector_ready = task(retries=20, retry_delay=timedelta(seconds=15))(check_debezium)()
    connector >> connector_ready
    flink = task(retries=5, retry_delay=timedelta(seconds=30))(check_flink)()
    kafka = task(retries=5, retry_delay=timedelta(seconds=30))(check_kafka)()

    @task
    def start_job(key):
        return ensure_flink_job(FLINK_JOBS[key])

    bronze = start_job.override(task_id="start_bronze")("bronze_cdc")
    for ready in (connector_ready, bucket, flink, kafka):
        ready >> bronze
    silver = start_job.override(task_id="start_silver")("silver_stream")
    bronze >> silver
    gold_jobs = []
    for key in ("gold_order_summary", "gold_payments", "gold_products", "gold_customer360_recovery"):
        job = start_job.override(task_id=f"start_{key}")(key)
        silver >> job
        gold_jobs.append(job)

    @task(execution_timeout=timedelta(minutes=12))
    def checkpoint(key):
        return validate_job_checkpoint(FLINK_JOBS[key]["job_name"], timeout_seconds=600, poll_interval_seconds=30)

    checkpoints = []
    for key, job in zip(
        ("bronze_cdc", "silver_stream", "gold_order_summary", "gold_payments", "gold_products", "gold_customer360_recovery"),
        (bronze, silver, *gold_jobs),
    ):
        checked = checkpoint.override(task_id=f"checkpoint_{key}")(key)
        job >> checked
        checkpoints.append(checked)

    files = PythonSensor(task_id="wait_for_bronze_files", python_callable=bronze_files_ready,
                         mode="reschedule", poke_interval=30, timeout=1800)
    checkpoints >> files

    @task(execution_timeout=timedelta(minutes=45))
    def spark_job(path):
        return run_spark_job(f"lakehouse/iceberg/jobs/{path}.py")

    bootstrap = spark_job.override(task_id="initialize_missing_iceberg_bronze")("initialize_streaming_bronze")
    merge = spark_job.override(task_id="merge_iceberg_silver")("incremental_cdc_to_silver")
    validate = spark_job.override(task_id="validate_iceberg_silver")("validate_silver")
    namespaces = spark_job.override(task_id="initialize_dbt_namespaces")("init_dbt_namespaces")
    files >> bootstrap >> merge >> validate >> namespaces

    @task(execution_timeout=timedelta(minutes=45))
    def dbt(command):
        return run_dbt_command(command)

    debug = dbt.override(task_id="dbt_debug")("dbt debug --target dev")
    build = dbt.override(task_id="dbt_build")("dbt build --target dev")
    docs = dbt.override(task_id="dbt_docs")("dbt docs generate --target dev")
    namespaces >> debug >> build >> docs


dag = retailpulse_streaming_end_to_end()
