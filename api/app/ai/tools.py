from __future__ import annotations

from app.db.postgres import get_connection


def get_customer_intelligence(
    customer_id: int,
) -> dict:

    connection = get_connection()

    try:

        cursor = connection.cursor()

        # ====================================================
        # CUSTOMER 360
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

            return {
                "found": False,
                "customer_id": customer_id,
            }

        customer_data = {
            "customer_id": customer[0],
            "first_name": customer[1],
            "last_name": customer[2],
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

        # Notice:
        # We intentionally do NOT send email to the LLM.
        #
        # The API may contain it, but the model doesn't need
        # unnecessary PII to explain churn/recommendations.

        # ====================================================
        # CHURN
        # ====================================================

        cursor.execute(
            """
            SELECT
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
                "churn_probability": churn[0],
                "churn_prediction": churn[1],
                "risk_level": churn[2],
                "model_version": churn[3],
                "prediction_date": (
                    churn[4].isoformat()
                    if churn[4]
                    else None
                ),
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
                model_version
            FROM serving.product_recommendations
            WHERE customer_id = %s
            ORDER BY recommendation_rank
            LIMIT 10
            """,
            (
                customer_id,
            ),
        )

        recommendations = []

        for row in cursor.fetchall():

            recommendations.append(
                {
                    "product_id": row[0],
                    "rank": row[1],
                    "score": row[2],
                    "reason": row[3],
                    "model_name": row[4],
                    "model_version": row[5],
                }
            )

        return {
            "found": True,
            "customer": customer_data,
            "churn": churn_data,
            "recommendations": recommendations,
        }

    finally:

        connection.close()