
from __future__ import annotations
import sys
from datetime import timedelta
import pendulum

sys.path.insert(0, "/opt/airflow/include")

from airflow.sdk import dag, task
from retailpulse.iceberg_control import run_spark_job

@dag(
    dag_id="retailpulse_iceberg_maintenance",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 9, 10, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    tags=["retailpulse", "iceberg", "maintenance"],
)
def retailpulse_iceberg_maintenance():

    @task
    def maintain():
        return run_spark_job(
            "lakehouse/iceberg/jobs/iceberg_maintenance.py"
        )

    maintain()

dag = retailpulse_iceberg_maintenance()
