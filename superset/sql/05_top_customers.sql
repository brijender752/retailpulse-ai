SELECT
    customer_id,
    customer_name,
    country,
    state,
    customer_segment,
    customer_value_segment,

    total_orders,

    ROUND(
        lifetime_order_value,
        2
    ) AS lifetime_order_value,

    ROUND(
        lifetime_payments,
        2
    ) AS lifetime_payments,

    website_event_count,

    marketing_conversions,

    last_order_date

FROM retailpulse.analytics.mart_customer_value

ORDER BY
    lifetime_order_value DESC

LIMIT 100;