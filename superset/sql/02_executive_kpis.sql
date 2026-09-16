SELECT
    COUNT(DISTINCT order_id)
        AS total_orders,

    ROUND(
        SUM(
            CASE
                WHEN order_status NOT IN (
                    'CANCELLED',
                    'CANCELED'
                )
                THEN total_amount
                ELSE 0
            END
        ),
        2
    ) AS total_revenue,

    ROUND(
        AVG(
            CASE
                WHEN order_status NOT IN (
                    'CANCELLED',
                    'CANCELED'
                )
                THEN total_amount
            END
        ),
        2
    ) AS average_order_value,

    COUNT(DISTINCT customer_id)
        AS purchasing_customers,

    SUM(
        CASE
            WHEN payment_match_status = 'MISMATCH'
            THEN 1
            ELSE 0
        END
    ) AS payment_mismatch_orders

FROM retailpulse.analytics.fact_orders;