SCHEMA_DESCRIPTION = """
Database: PostgreSQL

Allowed view:
ai_reporting.customer_metrics

Columns:
- customer_segment: text
- country: text
- total_orders: bigint
- total_revenue: numeric
- avg_order_value: numeric
- days_since_last_order: bigint
- total_website_events: bigint
- total_support_tickets: bigint
- churn_probability: double precision
- risk_level: text

Rules:
- Only query ai_reporting.customer_metrics.
- This view contains one row per customer.
- No customer IDs or personally identifiable data.
- Use COUNT(*) for customer counts.
- Use SUM(total_revenue) for total customer revenue.
- Use AVG(churn_probability) for average churn risk.
- Use AVG(total_orders) for average number of orders.
- For rounded averages use ROUND(AVG(column)::numeric, 2).
- Preserve a requested LIMIT when it is below 100.
- Use PostgreSQL syntax.
- Do not invent columns.
- Only generate one SELECT statement.
- Do not use joins, CTEs, subqueries or window functions.
- Do not use SELECT *.
- Maximum 100 result rows.

Important:
total_revenue is cumulative customer revenue.
It is not revenue for a specific calendar period.
The view has no transaction date or product-level
sales information.
"""
