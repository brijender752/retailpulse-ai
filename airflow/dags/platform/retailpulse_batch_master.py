"""Independent bounded snapshot pipeline with isolated batch outputs."""
import sys
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

sys.path.insert(0, "/opt/airflow/include")
from retailpulse.batch_control import run_batch_job
from retailpulse.health import check_postgres, check_minio
from retailpulse.minio_validation import validate_prefix


@dag(
    dag_id="retailpulse_batch_master",
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 11, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    tags=["retailpulse", "batch", "parent", "spark"],
)
def retailpulse_batch_master():
    @task
    def source_health():
        return check_postgres()

    @task
    def storage_health():
        return check_minio()

    @task(retries=0)
    def run_layer(layer: str):
        run_batch_job(layer)
        return {"layer": layer, "status": "completed"}

    @task
    def validate_outputs():
        entities = (
            "customers", "products", "orders", "order_items", "payments",
            "inventory", "website_events", "support_tickets", "marketing_events",
        )
        prefixes = [f"batch/{layer}/{entity}/" for layer in ("bronze", "silver") for entity in entities]
        prefixes += [f"batch/gold/{name}/" for name in ("customer_360", "product_performance", "order_summary")]
        return [validate_prefix(prefix, prefix) for prefix in prefixes]

    bronze = run_layer.override(task_id="batch_bronze")("bronze")
    silver = run_layer.override(task_id="batch_silver")("silver")
    gold = run_layer.override(task_id="batch_gold")("gold")
    [source_health(), storage_health()] >> bronze >> silver >> gold >> validate_outputs()


dag = retailpulse_batch_master()
