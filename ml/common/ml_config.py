from __future__ import annotations


CATALOG = "retailpulse"

ANALYTICS_SCHEMA = "analytics"

ML_SCHEMA = "ml"


CUSTOMERS_TABLE = (
    "retailpulse.analytics.dim_customer"
)

ORDERS_TABLE = (
    "retailpulse.analytics.fact_orders"
)

PAYMENTS_TABLE = (
    "retailpulse.analytics.fact_payments"
)

ACTIVITY_TABLE = (
    "retailpulse.analytics.fact_customer_activity"
)


CUSTOMER_FEATURES_TABLE = (
    "retailpulse.ml.customer_features"
)

CHURN_TRAINING_TABLE = (
    "retailpulse.ml.churn_training"
)


CHURN_HORIZON_DAYS = 60

SILVER_ORDERS_TABLE = "retailpulse.silver.orders"
SILVER_PAYMENTS_TABLE = "retailpulse.silver.payments"
SILVER_WEBSITE_EVENTS_TABLE = "retailpulse.silver.website_events"
SILVER_SUPPORT_TICKETS_TABLE = "retailpulse.silver.support_tickets"
SILVER_MARKETING_EVENTS_TABLE = "retailpulse.silver.marketing_events"

CHURN_TRAINING_TABLE = "retailpulse.ml.churn_training"

CHURN_HORIZON_DAYS = 60

FEATURE_WINDOWS = [30, 90, 180]