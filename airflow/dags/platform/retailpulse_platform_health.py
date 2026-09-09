from __future__ import annotations

import sys

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


@dag(
    dag_id="retailpulse_platform_health",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(
        2026,
        9,
        9,
        tz="UTC",
    ),
    catchup=False,
    max_active_runs=1,
    tags=[
        "retailpulse",
        "health",
    ],
)
def retailpulse_platform_health():

    @task
    def postgres():
        return check_postgres()

    @task
    def debezium():
        return check_debezium()

    @task
    def kafka():
        return check_kafka()

    @task
    def flink():
        return check_flink()

    @task
    def minio():
        return check_minio()

    postgres()
    debezium()
    kafka()
    flink()
    minio()


dag = retailpulse_platform_health()
