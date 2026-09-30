from fastapi import (
    APIRouter,
    HTTPException,
)

from app.schemas.customer import (
    CustomerIntelligence,
)

from app.db.postgres import (
    get_connection,
)

from app.schemas.customer import (
    Customer360,
)


router = APIRouter(
    prefix="/customers",
    tags=["Customer 360"],
)


@router.get(
    "/{customer_id}/360",
    response_model=Customer360,
)
def get_customer_360(
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
                total_orders,
                total_revenue,
                avg_order_value,
                days_since_last_order,
                total_payments,
                total_website_events,
                total_support_tickets
            FROM serving.customer_360
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
                detail="Customer 360 not found",
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
            "total_orders": row[8],
            "total_revenue": (
                float(row[9])
                if row[9] is not None
                else None
            ),
            "avg_order_value": (
                float(row[10])
                if row[10] is not None
                else None
            ),
            "days_since_last_order": row[11],
            "total_payments": row[12],
            "total_website_events": row[13],
            "total_support_tickets": row[14],
        }

    finally:

        connection.close()


@router.get(
    "/{customer_id}/intelligence",
    response_model=CustomerIntelligence,
)
def get_customer_intelligence(
    customer_id: int,
):

    connection = get_connection()

    try:

        cursor = connection.cursor()

        # ====================================================
        # CUSTOMER
        # ====================================================

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
                total_orders,
                total_revenue,
                avg_order_value,
                days_since_last_order,
                total_payments,
                total_website_events,
                total_support_tickets
            FROM serving.customer_360
            WHERE customer_id = %s
            """,
            (
                customer_id,
            ),
        )

        customer = cursor.fetchone()

        if customer is None:

            raise HTTPException(
                status_code=404,
                detail="Customer not found",
            )

        customer_data = {
            "customer_id": customer[0],
            "first_name": customer[1],
            "last_name": customer[2],
            "email": customer[3],
            "country": customer[4],
            "state": customer[5],
            "city": customer[6],
            "customer_segment": customer[7],
            "total_orders": customer[8],
            "total_revenue": (
                float(customer[9])
                if customer[9] is not None
                else None
            ),
            "avg_order_value": (
                float(customer[10])
                if customer[10] is not None
                else None
            ),
            "days_since_last_order": customer[11],
            "total_payments": customer[12],
            "total_website_events": customer[13],
            "total_support_tickets": customer[14],
        }

        # ====================================================
        # CHURN
        # ====================================================

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

        churn = cursor.fetchone()

        churn_data = None

        if churn:

            churn_data = {
                "customer_id": churn[0],
                "churn_probability": churn[1],
                "churn_prediction": churn[2],
                "risk_level": churn[3],
                "model_version": churn[4],
                "prediction_date": churn[5],
            }

        # ====================================================
        # RECOMMENDATIONS
        # ====================================================

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
            LIMIT 10
            """,
            (
                customer_id,
            ),
        )

        recommendations = [
            {
                "product_id": row[0],
                "rank": row[1],
                "score": row[2],
                "reason": row[3],
                "model_name": row[4],
                "model_version": row[5],
                "generated_at": row[6],
            }
            for row in cursor.fetchall()
        ]

        return {
            "customer": customer_data,
            "churn": churn_data,
            "recommendations": recommendations,
        }

    finally:

        connection.close()