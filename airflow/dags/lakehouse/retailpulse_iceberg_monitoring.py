from datetime import timedelta
import sys
import pendulum
from airflow.sdk import dag, task

sys.path.insert(0, '/opt/airflow/include')
from retailpulse.iceberg_control import run_spark_job


@dag(
    dag_id='retailpulse_iceberg_monitoring',
    schedule='*/15 * * * *',
    start_date=pendulum.datetime(2026, 9, 10, tz='UTC'),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=14),
    tags=['retailpulse', 'iceberg', 'monitoring'],
)
def monitoring():
    @task
    def check():
        return run_spark_job('lakehouse/iceberg/jobs/check_freshness.py')
    check()


dag = monitoring()
