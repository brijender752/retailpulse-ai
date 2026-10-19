import psycopg2
from psycopg2.extras import RealDictCursor

from app.core.config import settings


def execute_readonly_sql(sql: str) -> list[dict]:
    conn = psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.analyst_db_user,
        password=settings.analyst_db_password,
        connect_timeout=5,
    )

    try:
        conn.set_session(
            readonly=True,
            autocommit=False,
        )

        with conn.cursor(
            cursor_factory=RealDictCursor
        ) as cursor:
            cursor.execute(
                "SET LOCAL statement_timeout = '5000ms'"
            )

            cursor.execute(sql)

            rows = cursor.fetchmany(101)

            return [
                dict(row)
                for row in rows[:100]
            ]

    finally:
        conn.rollback()
        conn.close()