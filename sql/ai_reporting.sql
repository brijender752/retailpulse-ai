CREATE SCHEMA IF NOT EXISTS ai_reporting;

CREATE OR REPLACE VIEW ai_reporting.customer_metrics
WITH (security_barrier = true)
AS
SELECT
    c.customer_segment,
    c.country,
    c.total_orders,
    c.total_revenue,
    c.avg_order_value,
    c.days_since_last_order,
    c.total_website_events,
    c.total_support_tickets,
    p.churn_probability,
    p.risk_level
FROM serving.customer_360 c
LEFT JOIN serving.churn_predictions p
    ON c.customer_id = p.customer_id;