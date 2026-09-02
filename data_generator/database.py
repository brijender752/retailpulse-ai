# import psycopg2

# from config.settings import (
#     POSTGRES_HOST,
#     POSTGRES_PORT,
#     POSTGRES_DB,
#     POSTGRES_USER,
#     POSTGRES_PASSWORD,
# )


# def get_connection():
#     return psycopg2.connect(
#         host=POSTGRES_HOST,
#         port=POSTGRES_PORT,
#         database=POSTGRES_DB,
#         user=POSTGRES_USER,
#         password=POSTGRES_PASSWORD,
#     )


from contextlib import contextmanager

import psycopg2
from psycopg2.extras import execute_values

from config.settings import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)


@contextmanager
def get_connection():
    connection = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )

    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def bulk_insert(
    connection,
    table: str,
    columns: list[str],
    rows: list[tuple],
    page_size: int = 5000,
):
    if not rows:
        return

    column_list = ", ".join(columns)

    query = f"""
        INSERT INTO {table} ({column_list})
        VALUES %s
    """

    with connection.cursor() as cursor:
        execute_values(
            cursor,
            query,
            rows,
            page_size=page_size,
        )