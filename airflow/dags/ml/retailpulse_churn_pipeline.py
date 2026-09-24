from __future__ import annotations

import sys
from datetime import timedelta

import pendulum


sys.path.insert(
    0,
    "/opt/airflow/include",
)


from airflow.sdk import (
    dag,
    task,
)

from retailpulse.ml.churn_control import (
    build_scoring_features,
    check_mlflow_health,
    export_scoring_features,
    load_predictions,
    run_churn_inference,
    run_prediction_monitoring,
    validate_champion_model,
    validate_predictions,
    validate_sources,
)


@dag(
    dag_id="retailpulse_churn_pipeline",

    # Start manually.
    # Once validated, we'll schedule it.
    schedule=None,

    start_date=pendulum.datetime(
        2026,
        9,
        21,
        tz="UTC",
    ),

    catchup=False,

    max_active_runs=1,

    dagrun_timeout=timedelta(
        minutes=60
    ),

    tags=[
        "retailpulse",
        "ml",
        "churn",
        "mlflow",
    ],
)
def retailpulse_churn_pipeline():

    # ========================================================
    # VALIDATE SOURCES
    # ========================================================

    @task
    def source_validation():

        return validate_sources()

    # ========================================================
    # MLFLOW HEALTH
    # ========================================================

    @task
    def mlflow_health():

        return check_mlflow_health()

    # ========================================================
    # FEATURE ENGINEERING
    # ========================================================

    @task
    def scoring_features():

        return build_scoring_features()

    # ========================================================
    # EXPORT FEATURES
    # ========================================================

    @task
    def export_features():

        return export_scoring_features()

    # ========================================================
    # MODEL REGISTRY
    # ========================================================

    @task
    def champion_model():

        return validate_champion_model()

    # ========================================================
    # INFERENCE
    # ========================================================

    @task
    def inference():

        return run_churn_inference()

    # ========================================================
    # ICEBERG
    # ========================================================

    @task
    def iceberg_predictions():

        return load_predictions()

    # ========================================================
    # VALIDATION
    # ========================================================

    @task
    def prediction_validation():

        return validate_predictions()

    # ========================================================
    # MONITORING
    # ========================================================

    @task
    def monitoring():

        return run_prediction_monitoring()

    sources = source_validation()

    mlflow = mlflow_health()

    features = scoring_features()

    exported = export_features()

    champion = champion_model()

    predicted = inference()

    loaded = iceberg_predictions()

    validated = prediction_validation()

    monitored = monitoring()

    sources >> features

    features >> exported

    mlflow >> champion

    exported >> predicted

    champion >> predicted

    predicted >> loaded

    loaded >> validated

    validated >> monitored


dag = retailpulse_churn_pipeline()