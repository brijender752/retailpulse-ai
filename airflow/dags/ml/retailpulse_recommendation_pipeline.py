from __future__ import annotations

from datetime import datetime, timezone

from airflow.sdk import dag, task

from include.retailpulse.ml.recommendation_control import (
    build_events,
    build_interactions,
    build_popularity,
    build_similarity,
    generate_recommendations,
    monitor_recommendations,
    validate_champion_config,
    validate_recommendations,
    validate_sources,
)


@dag(
    dag_id="retailpulse_recommendation_pipeline",

    schedule=None,

    start_date=datetime(
        2026,
        9,
        1,
        tzinfo=timezone.utc,
    ),

    catchup=False,

    max_active_runs=1,

    tags=[
        "retailpulse",
        "ml",
        "recommendation",
        "production",
    ],
)
def recommendation_pipeline():

    @task
    def source_validation():

        return validate_sources()


    @task
    def customer_product_events():

        return build_events()


    @task
    def customer_product_interactions():

        return build_interactions()


    @task
    def product_popularity():

        return build_popularity()


    @task
    def product_similarity():

        return build_similarity()


    @task
    def champion_config():

        return validate_champion_config()


    @task
    def production_recommendations():

        return generate_recommendations()


    @task
    def recommendation_validation():

        return validate_recommendations()


    @task
    def recommendation_monitoring():

        return monitor_recommendations()


    sources = source_validation()

    events = customer_product_events()

    interactions = (
        customer_product_interactions()
    )

    popularity = product_popularity()

    similarity = product_similarity()

    champion = champion_config()

    recommendations = (
        production_recommendations()
    )

    validation = (
        recommendation_validation()
    )

    monitoring = (
        recommendation_monitoring()
    )


    sources >> events

    events >> interactions

    interactions >> [
        popularity,
        similarity,
    ]

    [
        popularity,
        similarity,
        champion,
    ] >> recommendations

    recommendations >> validation

    validation >> monitoring


recommendation_pipeline()