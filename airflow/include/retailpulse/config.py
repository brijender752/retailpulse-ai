import os


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


POSTGRES = {
    "host": env("RETAILPULSE_POSTGRES_HOST", "postgres"),
    "port": int(env("RETAILPULSE_POSTGRES_PORT", "5432")),
    "database": env("RETAILPULSE_POSTGRES_DB", "retailpulse"),
    "user": env("RETAILPULSE_POSTGRES_USER", "retailpulse"),
    "password": env("RETAILPULSE_POSTGRES_PASSWORD", "retailpulse"),
}

DEBEZIUM_URL = env(
    "RETAILPULSE_DEBEZIUM_URL",
    "http://debezium:8083",
)

DEBEZIUM_CONNECTOR = env(
    "RETAILPULSE_DEBEZIUM_CONNECTOR",
    "retailpulse-postgres-connector",
)

KAFKA_HOST = env(
    "RETAILPULSE_KAFKA_HOST",
    "kafka",
)

KAFKA_PORT = int(
    env("RETAILPULSE_KAFKA_PORT", "9092")
)

FLINK_URL = env(
    "RETAILPULSE_FLINK_URL",
    "http://flink-jobmanager:8081",
)

FLINK_CONTAINER = env(
    "RETAILPULSE_FLINK_CONTAINER",
    "retailpulse-flink-jobmanager",
)

MINIO_ENDPOINT = env(
    "RETAILPULSE_MINIO_ENDPOINT",
    "minio:9000",
)

MINIO_ACCESS_KEY = env(
    "RETAILPULSE_MINIO_ACCESS_KEY",
    "minioadmin",
)

MINIO_SECRET_KEY = env(
    "RETAILPULSE_MINIO_SECRET_KEY",
    "minioadmin",
)

MINIO_SECURE = (
    env("RETAILPULSE_MINIO_SECURE", "false").lower()
    == "true"
)

MINIO_BUCKET = env(
    "RETAILPULSE_MINIO_BUCKET",
    "retailpulse",
)
