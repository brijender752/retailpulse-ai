from fastapi import (
    APIRouter,
    HTTPException,
)

from app.db.postgres import get_connection


router = APIRouter(
    prefix="/customers",
    tags=["Customers"],
)


@router.get("/{customer_id}")
def get_customer(
    customer_id: int,
):

    connection = get_connection()

    try:

        cursor = connection.cursor()

        cursor.execute(
            """
            SELECT
                customer_id,
                first_name,
                last_name,
                email,
                country,
                state,
                city,
                customer_segment,
                signup_date
            FROM ecommerce.customers
            WHERE customer_id = %s
            """,
            (
                customer_id,
            ),
        )

        row = cursor.fetchone()

        if row is None:

            raise HTTPException(
                status_code=404,
                detail="Customer not found",
            )

        return {
            "customer_id": row[0],
            "first_name": row[1],
            "last_name": row[2],
            "email": row[3],
            "country": row[4],
            "state": row[5],
            "city": row[6],
            "customer_segment": row[7],
            "signup_date": row[8],
        }

    finally:

        connection.close()