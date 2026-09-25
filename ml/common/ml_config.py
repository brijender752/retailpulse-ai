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

CHURN_SCORING_FEATURES_TABLE = (
    "retailpulse.ml.churn_scoring_features"
)

CHURN_PREDICTIONS_TABLE = (
    "retailpulse.ml.churn_predictions"
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

# ============================================================
# RECOMMENDATION ENGINE
# ============================================================

PRODUCTS_TABLE = (
    "retailpulse.silver.products"
)

ORDER_ITEMS_TABLE = (
    "retailpulse.silver.order_items"
)

RECOMMENDATION_INTERACTIONS_TABLE = (
    "retailpulse.ml.customer_product_interactions"
)

PRODUCT_POPULARITY_TABLE = (
    "retailpulse.ml.product_popularity"
)

PRODUCT_SIMILARITY_TABLE = (
    "retailpulse.ml.product_similarity"
)

PRODUCT_RECOMMENDATIONS_TABLE = (
    "retailpulse.ml.product_recommendations"
)


PURCHASE_WEIGHT = 5.0

PRODUCT_VIEW_WEIGHT = 1.0

# ============================================================
# ITEM-ITEM RECOMMENDER
# ============================================================

PRODUCT_SIMILARITY_TABLE = (
    "retailpulse.ml.product_similarity"
)

ITEM_ITEM_RECOMMENDATIONS_TABLE = (
    "retailpulse.ml.item_item_recommendations"
)

MIN_PRODUCT_CUSTOMERS = 2
MIN_CO_INTERACTIONS = 2

TOP_SIMILAR_PRODUCTS = 50
TOP_RECOMMENDATIONS = 10

# ============================================================
# HYBRID RECOMMENDER
# ============================================================

PRODUCT_RECOMMENDATIONS_TABLE = (
    "retailpulse.ml.product_recommendations"
)

HYBRID_PERSONALIZED_WEIGHT = 0.80

HYBRID_POPULARITY_WEIGHT = 0.20

HYBRID_CANDIDATE_LIMIT = 100

TOP_RECOMMENDATIONS = 10


# ============================================================
# RECOMMENDATION EVALUATION
# ============================================================

RECOMMENDATION_EVALUATION_TABLE = (
    "retailpulse.ml.recommendation_evaluation"
)

RECOMMENDATION_METRICS_TABLE = (
    "retailpulse.ml.recommendation_metrics"
)

RECOMMENDATION_EVAL_TOP_K = 10

RECOMMENDATION_TEST_DAYS = 30

RECOMMENDATION_MIN_HISTORY = 2



# ============================================================
# RECOMMENDATION MLFLOW
# ============================================================

RECOMMENDATION_MLFLOW_EXPERIMENT = (
    "retailpulse-recommendation"
)

RECOMMENDATION_REGISTERED_MODEL = (
    "RetailPulseRecommendationModel"
)

RECOMMENDATION_MODEL_ALIAS = (
    "champion"
)

RECOMMENDATION_MODEL_VERSION = "v1"