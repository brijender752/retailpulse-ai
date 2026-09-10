from __future__ import annotations
import os
import docker

SPARK_CONTAINER = os.getenv(
    "RETAILPULSE_SPARK_ICEBERG_CONTAINER",
    "retailpulse-spark-iceberg",
)

def run_spark_job(relative_job_path: str) -> str:
    client = docker.from_env()
    container = client.containers.get(SPARK_CONTAINER)
    cmd = f"/opt/spark/bin/spark-submit /opt/retailpulse/{relative_job_path}"

    result = container.exec_run(
        ["bash", "-lc", cmd],
        stdout=True,
        stderr=True,
    )

    output = result.output.decode("utf-8", errors="replace")
    if result.exit_code != 0:
        raise RuntimeError(
            f"Spark job failed\nCommand: {cmd}\n{output}"
        )
    return output
