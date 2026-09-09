# ============================================================
# RetailPulse Kafka Topics
# ============================================================

TOPICS = {

    "customers": {
        "partitions": 3,
        "replication_factor": 1,
    },

    "products": {
        "partitions": 3,
        "replication_factor": 1,
    },

    "orders": {
        "partitions": 3,
        "replication_factor": 1,
    },

    "order_items": {
        "partitions": 3,
        "replication_factor": 1,
    },

    "payments": {
        "partitions": 3,
        "replication_factor": 1,
    },

    "inventory": {
        "partitions": 3,
        "replication_factor": 1,
    },

    "website_events": {
        "partitions": 3,
        "replication_factor": 1,
    },

    "support_tickets": {
        "partitions": 3,
        "replication_factor": 1,
    },

    "marketing_events": {
        "partitions": 3,
        "replication_factor": 1,
    },
}