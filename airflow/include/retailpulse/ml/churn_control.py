from __future__ import annotations

import os

import docker
import requests


SPARK_CONTAINER = os.getenv(
    "RETAILPULSE_SPARK_CONTAINER",
    "retailpulse-spark-iceberg",
)

ML_CONTAINER = os.getenv(
    "RETAILPULSE_ML_CONTAINER",
    "retailpulse-ml",
)

MLFLOW_URL = os.getenv(
    "RETAILPULSE_MLFLOW_URL",
    "http://mlflow:5000",
)

REGISTERED_MODEL_NAME = os.getenv(
    "MLFLOW_REGISTERED_MODEL_NAME",
    "RetailPulseChurnModel",
)

MODEL_ALIAS = os.getenv(
    "MLFLOW_MODEL_ALIAS",
    "champion",
)


def docker_client():
    return docker.from_env()


def run_container_command(
    container_name: str,
    command: list[str],
) -> str:

    client = docker_client()

    container = client.containers.get(
        container_name
    )

    result = container.exec_run(
        command,
        stdout=True,
        stderr=True,
    )

    output = result.output.decode(
        "utf-8",
        errors="replace",
    )

    print(output)

    if result.exit_code != 0:
        raise RuntimeError(
            f"Command failed in "
            f"{container_name}. "
            f"Exit code={result.exit_code}\n"
            f"{output}"
        )

    return output


def run_spark_job(
    script_path: str,
) -> str:

    return run_container_command(
        SPARK_CONTAINER,
        [
            "spark-submit",
            script_path,
        ],
    )


def run_ml_python(
    script_path: str,
) -> str:

    return run_container_command(
        ML_CONTAINER,
        [
            "python",
            script_path,
        ],
    )


def check_mlflow_health() -> dict:

    response = requests.get(
        f"{MLFLOW_URL}/health",
        timeout=15,
    )

    response.raise_for_status()

    return {
        "status_code":
            response.status_code,

        "body":
            response.text,
    }


def validate_sources() -> str:

    return run_spark_job(
        "/opt/retailpulse/ml/"
        "monitoring/validate_churn_sources.py"
    )


def validate_champion_model() -> str:

    return run_ml_python(
        "/opt/retailpulse/ml/"
        "training/test_registry_model.py"
    )


def build_scoring_features() -> str:

    return run_spark_job(
        "/opt/retailpulse/ml/"
        "features/"
        "build_churn_scoring_features.py"
    )


def export_scoring_features() -> str:

    return run_spark_job(
        "/opt/retailpulse/ml/"
        "inference/"
        "export_churn_scoring_features.py"
    )


def run_churn_inference() -> str:

    return run_ml_python(
        "/opt/retailpulse/ml/"
        "inference/predict_churn.py"
    )


def load_predictions() -> str:

    return run_spark_job(
        "/opt/retailpulse/ml/"
        "inference/"
        "load_churn_predictions.py"
    )


def validate_predictions() -> str:

    return run_spark_job(
        "/opt/retailpulse/ml/"
        "monitoring/validate_churn_predictions.py"
    )


def run_prediction_monitoring() -> str:

    return run_spark_job(
        "/opt/retailpulse/ml/"
        "monitoring/"
        "monitor_churn_predictions.py"
    )
