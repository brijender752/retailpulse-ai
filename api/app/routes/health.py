from fastapi import APIRouter

from app.db.postgres import get_connection


router = APIRouter(
    prefix="/health",
    tags=["Health"],
)


@router.get("")
def health():

    database = "DOWN"

    try:

        connection = get_connection()

        cursor = connection.cursor()

        cursor.execute(
            "SELECT 1"
        )

        cursor.fetchone()

        cursor.close()

        connection.close()

        database = "UP"

    except Exception:

        database = "DOWN"

    return {
        "status": "UP",
        "database": database,
        "service": "retailpulse-api",
    }