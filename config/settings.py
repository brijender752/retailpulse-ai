import os

from dotenv import load_dotenv


load_dotenv()


POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "retailpulse")
POSTGRES_USER = os.getenv("POSTGRES_USER", "retailpulse")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "retailpulse")

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9094",
)

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "localhost:9000",
)

MINIO_ACCESS_KEY = os.getenv(
    "MINIO_ACCESS_KEY",
    "minioadmin",
)

MINIO_SECRET_KEY = os.getenv(
    "MINIO_SECRET_KEY",
    "minioadmin",
)

MINIO_BUCKET = "retailpulse"

MINIO_S3A_ENDPOINT = f"http://{MINIO_ENDPOINT}"


# ============================================================
# Environment
# ============================================================

ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
