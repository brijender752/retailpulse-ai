SELECT
    payment_match_status,

    COUNT(*)
        AS orders,

    ROUND(
        SUM(total_amount),
        2
    ) AS order_amount,

    ROUND(
        SUM(payment_amount),
        2
    ) AS payment_amount,

    ROUND(
        SUM(payment_variance),
        2
    ) AS payment_variance

FROM retailpulse.analytics.fact_orders

GROUP BY
    payment_match_status

ORDER BY
    orders DESC;