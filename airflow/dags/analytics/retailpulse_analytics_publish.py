from __future__ import annotations

import sys

from datetime import timedelta

import pendulum


sys.path.insert(
    0,
    "/opt/airflow/include",
)


from airflow.sdk import dag, task

from retailpulse.superset_control import (
    check_superset_health,
)


from retailpulse.task_logging import log_task_start

@dag(default_args={"on_execute_callback": log_task_start}, 
    dag_id="retailpulse_analytics_publish",

    schedule=None,

    start_date=pendulum.datetime(
        2026,
        9,
        15,
        tz="UTC",
    ),

    catchup=False,

    is_paused_upon_creation=False,

    max_active_runs=1,

    dagrun_timeout=timedelta(
        minutes=10
    ),

    tags=[
        "retailpulse",
        "superset",
        "analytics",
    ],
)
def retailpulse_analytics_publish():

    @task
    def superset_health():

        return check_superset_health()

    superset_health()


dag = retailpulse_analytics_publish()
