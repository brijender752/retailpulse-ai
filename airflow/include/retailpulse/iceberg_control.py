from __future__ import annotations
import os
import shlex
import docker
from retailpulse.task_logging import log_execution

SPARK_CONTAINER = os.getenv(
    "RETAILPULSE_SPARK_ICEBERG_CONTAINER",
    "retailpulse-spark-iceberg",
)

def run_spark_job(relative_job_path: str) -> str:
    log_execution(relative_job_path, SPARK_CONTAINER, f"spark-submit /opt/retailpulse/{relative_job_path}")
    client = docker.from_env()
    container = client.containers.get(SPARK_CONTAINER)
    cmd = "python3 /opt/retailpulse/monitoring/exporters/run_spark_job.py " + shlex.quote(f"/opt/retailpulse/{relative_job_path}")

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
