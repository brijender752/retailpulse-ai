import socket

import psycopg2
import requests
from minio import Minio

from retailpulse.config import (
    POSTGRES,
    DEBEZIUM_URL,
    DEBEZIUM_CONNECTOR,
    KAFKA_HOST,
    KAFKA_PORT,
    FLINK_URL,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_SECURE,
    MINIO_BUCKET,
)


def check_postgres() -> dict:
    conn = psycopg2.connect(
        host=POSTGRES["host"],
        port=POSTGRES["port"],
        dbname=POSTGRES["database"],
        user=POSTGRES["user"],
        password=POSTGRES["password"],
        connect_timeout=10,
    )

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*)
                FROM ecommerce.customers
                """
            )

            customer_count = cur.fetchone()[0]

        return {
            "status": "healthy",
            "customers": customer_count,
        }

    finally:
        conn.close()


def check_debezium() -> dict:
    response = requests.get(
        f"{DEBEZIUM_URL}/connectors/{DEBEZIUM_CONNECTOR}/status",
        timeout=10,
    )
    response.raise_for_status()

    data = response.json()

    connector_state = (
        data.get("connector", {})
        .get("state")
    )

    task_states = [
        task.get("state")
        for task in data.get("tasks", [])
    ]

    if connector_state != "RUNNING":
        raise RuntimeError(
            f"Debezium connector is {connector_state}"
        )

    bad_tasks = [
        state
        for state in task_states
        if state != "RUNNING"
    ]

    if bad_tasks:
        raise RuntimeError(
            f"Debezium tasks unhealthy: {task_states}"
        )

    return {
        "status": "healthy",
        "connector_state": connector_state,
        "task_states": task_states,
    }


def check_kafka() -> dict:
    with socket.create_connection(
        (KAFKA_HOST, KAFKA_PORT),
        timeout=10,
    ):
        pass

    return {
        "status": "healthy",
        "host": KAFKA_HOST,
        "port": KAFKA_PORT,
    }


def check_flink() -> dict:
    response = requests.get(
        f"{FLINK_URL}/overview",
        timeout=10,
    )
    response.raise_for_status()

    data = response.json()

    if data.get("taskmanagers", 0) < 1:
        raise RuntimeError(
            "Flink has no registered TaskManager."
        )

    return {
        "status": "healthy",
        "taskmanagers": data.get("taskmanagers"),
        "slots_total": data.get("slots-total"),
        "slots_available": data.get("slots-available"),
        "jobs_running": data.get("jobs-running"),
    }


def get_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def check_minio() -> dict:
    client = get_minio_client()

    if not client.bucket_exists(MINIO_BUCKET):
        raise RuntimeError(
            f"MinIO bucket not found: {MINIO_BUCKET}"
        )

    return {
        "status": "healthy",
        "bucket": MINIO_BUCKET,
    }
