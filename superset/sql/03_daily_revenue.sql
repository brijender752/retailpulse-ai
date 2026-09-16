SELECT
    CAST(order_date AS DATE)
        AS order_date,

    COUNT(DISTINCT order_id)
        AS orders,

    ROUND(
        SUM(total_amount),
        2
    ) AS revenue,

    ROUND(
        AVG(total_amount),
        2
    ) AS average_order_value

FROM retailpulse.analytics.fact_orders

WHERE order_status NOT IN (
    'CANCELLED',
    'CANCELED'
)

GROUP BY
    CAST(order_date AS DATE)

ORDER BY
    order_date;