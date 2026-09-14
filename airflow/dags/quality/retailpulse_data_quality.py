import sys, pendulum
from datetime import timedelta
sys.path.insert(0,"/opt/airflow/include")
from airflow.sdk import dag, task
from retailpulse.dbt_control import run_dbt_command
from retailpulse.quality_control import run_quality_job

@dag(
    dag_id="retailpulse_data_quality",
    schedule=None,
    start_date=pendulum.datetime(2026,9,11,tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=45),
    tags=["retailpulse","quality","great-expectations"],
)
def retailpulse_data_quality():
    @task
    def dbt_tests():
        return run_dbt_command("dbt test --target dev")
    @task
    def freshness():
        return run_quality_job("quality/check_freshness.py")
    @task
    def gx_validation():
        return run_quality_job("quality/validate_analytics.py")

    dbt_tests() >> freshness() >> gx_validation()

dag = retailpulse_data_quality()
