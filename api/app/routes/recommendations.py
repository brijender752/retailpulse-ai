from fastapi import APIRouter

from app.db.postgres import (
    get_connection,
)

from app.schemas.recommendation import (
    Recommendation,
)


router = APIRouter(
    prefix="/customers",
    tags=["Recommendations"],
)


@router.get(
    "/{customer_id}/recommendations",
    response_model=list[Recommendation],
)
def get_recommendations(
    customer_id: int,
    limit: int = 10,
):

    limit = max(
        1,
        min(
            limit,
            50,
        ),
    )

    connection = get_connection()

    try:

        cursor = connection.cursor()

        cursor.execute(
            """
            SELECT
                product_id,
                recommendation_rank,
                recommendation_score,
                recommendation_reason,
                model_name,
                model_version,
                generated_at
            FROM serving.product_recommendations
            WHERE customer_id = %s
            ORDER BY recommendation_rank
            LIMIT %s
            """,
            (
                customer_id,
                limit,
            ),
        )

        rows = cursor.fetchall()

        return [
            {
                "product_id": row[0],
                "rank": row[1],
                "score": row[2],
                "reason": row[3],
                "model_name": row[4],
                "model_version": row[5],
                "generated_at": row[6],
            }
            for row in rows
        ]

    finally:

        connection.close()