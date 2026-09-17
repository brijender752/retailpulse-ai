from __future__ import annotations
import sys, pendulum
from datetime import timedelta
sys.path.insert(0,"/opt/airflow/include")
from airflow.sdk import dag,task
from retailpulse.dbt_control import run_dbt_command
from retailpulse.task_logging import log_task_start

@dag(default_args={"on_execute_callback": log_task_start}, dag_id="retailpulse_dbt_analytics",schedule=None,start_date=pendulum.datetime(2026,9,11,tz="UTC"),catchup=False,max_active_runs=1,dagrun_timeout=timedelta(minutes=45),tags=["retailpulse","dbt","analytics"])
def retailpulse_dbt_analytics():
    @task
    def debug(): return run_dbt_command("dbt debug --target dev")
    @task
    def build(): return run_dbt_command("dbt build --target dev")
    @task
    def docs(): return run_dbt_command("dbt docs generate --target dev")
    debug() >> build() >> docs()
dag=retailpulse_dbt_analytics()
