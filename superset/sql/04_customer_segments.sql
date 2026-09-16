SELECT
    customer_value_segment,

    COUNT(*) AS customers,

    ROUND(
        SUM(lifetime_order_value),
        2
    ) AS lifetime_value,

    ROUND(
        AVG(lifetime_order_value),
        2
    ) AS avg_customer_value,

    ROUND(
        AVG(total_orders),
        2
    ) AS avg_orders

FROM retailpulse.analytics.mart_customer_value

GROUP BY
    customer_value_segment

ORDER BY
    lifetime_value DESC;