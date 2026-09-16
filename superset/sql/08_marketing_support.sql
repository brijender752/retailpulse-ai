SELECT
    c.customer_value_segment,

    COUNT(*)
        AS customers,

    SUM(a.website_event_count)
        AS website_events,

    SUM(a.session_count)
        AS sessions,

    SUM(a.support_ticket_count)
        AS support_tickets,

    SUM(a.open_support_ticket_count)
        AS open_support_tickets,

    SUM(a.marketing_impressions)
        AS impressions,

    SUM(a.marketing_clicks)
        AS clicks,

    SUM(a.marketing_conversions)
        AS conversions,

    ROUND(
        SUM(a.marketing_cost),
        2
    ) AS marketing_cost

FROM retailpulse.analytics.mart_customer_value c

JOIN retailpulse.analytics.fact_customer_activity a
    ON c.customer_id = a.customer_id

GROUP BY
    c.customer_value_segment

ORDER BY
    customers DESC;