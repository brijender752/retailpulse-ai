"""Run the bounded batch modules in the Spark container."""
import docker

from retailpulse.config import POSTGRES, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
from retailpulse.iceberg_control import SPARK_CONTAINER


BATCH_MODULES = {
    "bronze": "spark.jobs.batch.bronze.postgres_to_bronze",
    "silver": "spark.jobs.batch.silver.bronze_to_silver",
    "gold": "spark.jobs.batch.gold.silver_to_gold",
}


def run_batch_job(layer: str) -> str:
    if layer not in BATCH_MODULES:
        raise ValueError(f"Unknown batch layer: {layer}")
    environment = {
        "PYTHONPATH": "/opt/retailpulse",
        "POSTGRES_HOST": POSTGRES["host"],
        "POSTGRES_PORT": str(POSTGRES["port"]),
        "POSTGRES_DB": POSTGRES["database"],
        "POSTGRES_USER": POSTGRES["user"],
        "POSTGRES_PASSWORD": POSTGRES["password"],
        "MINIO_ENDPOINT": MINIO_ENDPOINT,
        "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
        "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
        "HADOOP_AWS_PACKAGE": "org.apache.hadoop:hadoop-aws:3.4.1",
    }
    client = docker.from_env()
    try:
        result = client.containers.get(SPARK_CONTAINER).exec_run(
            [
                "/opt/spark/bin/spark-submit",
                "--driver-class-path", "/opt/retailpulse/spark/jars/postgresql-42.7.12.jar",
                "--jars", "/opt/retailpulse/spark/jars/postgresql-42.7.12.jar",
                "/opt/retailpulse/spark/jobs/batch/run_batch.py", layer,
            ],
            workdir="/opt/retailpulse",
            environment=environment,
            stdout=True,
            stderr=True,
        )
    finally:
        client.close()
    output = result.output.decode("utf-8", errors="replace")
    if result.exit_code != 0:
        raise RuntimeError(f"Batch {layer} failed\n{output}")
    return output
