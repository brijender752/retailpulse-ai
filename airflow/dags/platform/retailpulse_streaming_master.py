"""Parent for continuous streaming; waits for controller readiness, not stream termination."""
from datetime import timedelta

import pendulum
from airflow.sdk import dag
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


import sys
sys.path.insert(0, "/opt/airflow/include")
from retailpulse.task_logging import log_task_start

@dag(default_args={"on_execute_callback": log_task_start}, 
    dag_id="retailpulse_streaming_master",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 9, 11, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=40),
    tags=["retailpulse", "streaming", "parent"],
)
def retailpulse_streaming_master():
    TriggerDagRunOperator(
        task_id="run_streaming_controller",
        trigger_dag_id="retailpulse_streaming_controller",
        trigger_run_id="streaming_master__{{ run_id }}",
        wait_for_completion=True,
        deferrable=True,
        poke_interval=30,
    )


dag = retailpulse_streaming_master()
