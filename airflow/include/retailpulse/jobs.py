"""
Registry of existing RetailPulse Flink jobs.

IMPORTANT:
- These paths point to the files that are already mounted in the Flink
  JobManager.
- Airflow does not duplicate business logic.
- Keep `job_name` exactly aligned with the name emitted by the corresponding
  PyFlink application whenever possible.
"""

FLINK_JOBS = {
    "bronze_cdc": {
        "path": "/opt/flink/jobs/streaming/bronze/retailpulse_bronze.py",
        "job_name": "RetailPulse - Multi Table Bronze CDC",
        "restore_from_savepoint": False,
    },

    "silver_stream": {
        "path": "/opt/flink/jobs/streaming/silver/retailpulse_streaming_silver.py",
        "job_name": "RetailPulse - Streaming Silver",
        "restore_from_savepoint": False,
    },

    "gold_order_summary": {
        "path": "/opt/flink/jobs/streaming/gold/retailpulse_streaming_gold.py",
        "job_name": "RetailPulse - Streaming Gold Order Summary",
        "restore_from_savepoint": False,
    },

    "gold_payments": {
        "path": "/opt/flink/jobs/streaming/gold/retailpulse_streaming_gold_payments.py",
        "job_name": "RetailPulse - Streaming Gold Payments",
        "restore_from_savepoint": False,
    },

    "gold_products": {
        "path": "/opt/flink/jobs/streaming/gold/retailpulse_streaming_gold_products.py",
        "job_name": "RetailPulse - Streaming Product Performance",
        "restore_from_savepoint": False,
    },

    "gold_customer360_recovery": {
        "path": "/opt/flink/jobs/streaming/gold/retailpulse_streaming_gold_customer360_recovery.py",
        "job_name": "RetailPulse - Customer 360 Stateful Recovery",
        "restore_from_savepoint": True,
        "savepoint_prefix": "flink-savepoints/customer360-recovery/",
    },
}
