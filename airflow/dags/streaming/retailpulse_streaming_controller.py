from __future__ import annotations

import sys
from datetime import timedelta

import pendulum

sys.path.insert(
    0,
    "/opt/airflow/include",
)

from airflow.sdk import dag, task

from retailpulse.health import (
    check_postgres,
    check_debezium,
    check_kafka,
    check_flink,
    check_minio,
)

from retailpulse.flink_control import (
    ensure_flink_job,
    validate_job_checkpoint,
)

from retailpulse.jobs import (
    FLINK_JOBS,
)

from retailpulse.minio_validation import (
    validate_streaming_outputs,
)


@dag(
    dag_id="retailpulse_streaming_controller",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(
        2026,
        9,
        9,
        tz="UTC",
    ),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(
        minutes=30
    ),
    tags=[
        "retailpulse",
        "streaming",
        "flink",
        "control-plane",
    ],
)
def retailpulse_streaming_controller():

    # ========================================================
    # INFRASTRUCTURE HEALTH
    # ========================================================

    @task
    def postgres_health():
        return check_postgres()

    @task
    def debezium_health():
        return check_debezium()

    @task
    def kafka_health():
        return check_kafka()

    @task
    def flink_health():
        return check_flink()

    @task
    def minio_health():
        return check_minio()

    pg = postgres_health()
    dbz = debezium_health()
    kafka = kafka_health()
    flink = flink_health()
    minio = minio_health()

    # Health checks are independent and can run in parallel.
    infrastructure_ready = [
        pg,
        dbz,
        kafka,
        flink,
        minio,
    ]

    # ========================================================
    # ENSURE STREAMING JOBS
    # ========================================================

    @task
    def ensure_job(job_key: str):
        return ensure_flink_job(
            FLINK_JOBS[job_key]
        )

    bronze = ensure_job(
        "bronze_cdc"
    )

    silver = ensure_job(
        "silver_stream"
    )

    gold_order = ensure_job(
        "gold_order_summary"
    )

    gold_payments = ensure_job(
        "gold_payments"
    )

    gold_products = ensure_job(
        "gold_products"
    )

    customer360 = ensure_job(
        "gold_customer360_recovery"
    )

    for health_task in infrastructure_ready:
        health_task >> bronze

    # Keep flow easy to read.
    bronze >> silver

    silver >> [
        gold_order,
        gold_payments,
        gold_products,
        customer360,
    ]

    # ========================================================
    # CHECK CUSTOMER 360 RECOVERY CHECKPOINT
    # ========================================================

    @task(
        retries=0,
        execution_timeout=timedelta(minutes=11),
    )
    def customer360_checkpoint():
        return validate_job_checkpoint(
            FLINK_JOBS[
                "gold_customer360_recovery"
            ]["job_name"],
            timeout_seconds=600,
            poll_interval_seconds=30,
        )

    checkpoint_ok = (
        customer360_checkpoint()
    )

    customer360 >> checkpoint_ok

    # ========================================================
    # MINIO VALIDATION
    # ========================================================

    @task
    def validate_minio():
        return validate_streaming_outputs()

    outputs_ok = validate_minio()

    [
        gold_order,
        gold_payments,
        gold_products,
        checkpoint_ok,
    ] >> outputs_ok


dag = retailpulse_streaming_controller()
