from fastapi import (
    APIRouter,
    HTTPException,
)

from app.db.postgres import (
    get_connection,
)

from app.schemas.churn import (
    ChurnPrediction,
)


router = APIRouter(
    prefix="/customers",
    tags=["Churn"],
)


@router.get(
    "/{customer_id}/churn",
    response_model=ChurnPrediction,
)
def get_customer_churn(
    customer_id: int,
):

    connection = get_connection()

    try:

        cursor = connection.cursor()

        cursor.execute(
            """
            SELECT
                customer_id,
                churn_probability,
                churn_prediction,
                risk_level,
                model_version,
                prediction_date
            FROM serving.churn_predictions
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
                detail=(
                    "Churn prediction "
                    "not found"
                ),
            )

        return {
            "customer_id": row[0],
            "churn_probability": row[1],
            "churn_prediction": row[2],
            "risk_level": row[3],
            "model_version": row[4],
            "prediction_date": row[5],
        }

    finally:

        connection.close()