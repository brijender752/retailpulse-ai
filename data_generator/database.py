import psycopg2

from config.settings import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)


def get_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )