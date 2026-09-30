from __future__ import annotations

import os

import docker


SPARK_CONTAINER = os.getenv(
    "SPARK_CONTAINER",
    "retailpulse-spark-iceberg",
)


def run_container_command(
    container_name: str,
    command: list[str],
):

    print(
        f"Container: {container_name}"
    )

    print(
        "Command:",
        " ".join(command),
    )

    client = docker.from_env()

    try:

        container = client.containers.get(
            container_name
        )

        result = container.exec_run(
            command,
            stdout=True,
            stderr=True,
        )

        output = (
            result.output.decode(
                "utf-8",
                errors="replace",
            )
        )

        print(output)

        if result.exit_code != 0:

            raise RuntimeError(
                f"Container command failed "
                f"with exit code "
                f"{result.exit_code}"
            )

        return output

    finally:

        client.close()


def spark_submit(
    script: str,
):

    return run_container_command(
        SPARK_CONTAINER,
        [
            "spark-submit",
            script,
        ],
    )


def validate_sources():

    return spark_submit(
        "/opt/retailpulse/ml/"
        "recommendation/monitoring/"
        "validate_recommendation_sources.py"
    )


def build_events():

    return spark_submit(
        "/opt/retailpulse/ml/"
        "recommendation/features/"
        "build_customer_product_events.py"
    )


def build_interactions():

    return spark_submit(
        "/opt/retailpulse/ml/"
        "recommendation/features/"
        "build_customer_product_interactions.py"
    )


def build_popularity():

    return spark_submit(
        "/opt/retailpulse/ml/"
        "recommendation/training/"
        "build_product_popularity.py"
    )


def build_similarity():

    return spark_submit(
        "/opt/retailpulse/ml/"
        "recommendation/training/"
        "build_product_similarity.py"
    )


def generate_recommendations():

    return spark_submit(
        "/opt/retailpulse/ml/"
        "recommendation/inference/"
        "generate_production_recommendations.py"
    )


def validate_recommendations():

    return spark_submit(
        "/opt/retailpulse/ml/"
        "recommendation/monitoring/"
        "validate_production_recommendations.py"
    )


def monitor_recommendations():

    return spark_submit(
        "/opt/retailpulse/ml/"
        "recommendation/monitoring/"
        "monitor_recommendations.py"
    )

def validate_champion_config():

    script = """
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ValidateRecommendationChampion")
    .getOrCreate()
)

table = "retailpulse.ml.recommendation_selected_config"

if not spark.catalog.tableExists(table):
    raise RuntimeError(
        f"Champion config table does not exist: {table}"
    )

rows = spark.table(table).collect()

if len(rows) != 1:
    raise RuntimeError(
        f"Expected exactly one champion config; found {len(rows)}"
    )

row = rows[0]

personalized = float(row["personalized_weight"])
popularity = float(row["popularity_weight"])

if personalized < 0 or popularity < 0:
    raise RuntimeError("Recommendation weights cannot be negative")

if abs(
    (personalized + popularity) - 1.0
) > 0.000001:
    raise RuntimeError(
        "Personalized + popularity weights must equal 1.0"
    )

print("Champion config valid")
print("Model:", row["model"])
print("Personalized:", personalized)
print("Popularity:", popularity)

spark.stop()
"""

    return run_container_command(
        SPARK_CONTAINER,
        [
            "python",
            "-c",
            script,
        ],
    )

def validate_champion_config():

    return spark_submit(
        "/opt/retailpulse/ml/"
        "recommendation/monitoring/"
        "validate_champion_config.py"
    )