from __future__ import annotations
import json, sys
from datetime import datetime, timezone
from pathlib import Path
import great_expectations as gx
from pyspark.sql.types import *
sys.path.insert(0, "/opt/retailpulse/lakehouse/iceberg/jobs")
from iceberg_session import create_iceberg_spark_session

RESULTS_DIR = Path("/opt/retailpulse/quality/results")
AUDIT_TABLE = "retailpulse.control.data_quality_results"

RULES = {
    "retailpulse.analytics.dim_customer": {
        "not_null": ["customer_id","email"], "unique": ["customer_id"]},
    "retailpulse.analytics.dim_product": {
        "not_null": ["product_id","price"], "unique": ["product_id"],
        "non_negative": ["price","cost","inventory_quantity"]},
    "retailpulse.analytics.dim_date": {
        "not_null": ["date_key","date_day"], "unique": ["date_key","date_day"]},
    "retailpulse.analytics.fact_orders": {
        "not_null": ["order_id","customer_id","date_key"], "unique": ["order_id"],
        "non_negative": ["total_amount","line_count","item_quantity","gross_item_amount"],
        "accepted": {"payment_match_status":["MATCHED","MISMATCH","NO_PAYMENT"]}},
    "retailpulse.analytics.fact_payments": {
        "not_null": ["payment_id","order_id","customer_id"], "unique": ["payment_id"],
        "non_negative": ["amount"]},
    "retailpulse.analytics.fact_customer_activity": {
        "not_null": ["customer_id"], "unique": ["customer_id"],
        "non_negative": ["website_event_count","session_count","support_ticket_count",
                         "open_support_ticket_count","marketing_impressions",
                         "marketing_clicks","marketing_conversions","marketing_cost"]},
    "retailpulse.analytics.mart_customer_value": {
        "not_null": ["customer_id","customer_value_segment"], "unique": ["customer_id"],
        "non_negative": ["total_orders","lifetime_order_value","lifetime_payments"],
        "accepted": {"customer_value_segment":["HIGH_VALUE","MEDIUM_VALUE","LOW_VALUE","PROSPECT"]}}
}

def ensure_audit_table(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS retailpulse.control")
    if spark.catalog.tableExists(AUDIT_TABLE):
        return
    schema = StructType([
        StructField("run_id", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("expectation", StringType(), False),
        StructField("success", BooleanType(), False),
        StructField("checked_at", TimestampType(), False),
    ])
    spark.createDataFrame([], schema).writeTo(AUDIT_TABLE).using("iceberg").create()

def validate_one(context, spark, table_name, rules):
    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(f"Missing table: {table_name}")
    df = spark.table(table_name)
    if df.limit(1).count() == 0:
        raise RuntimeError(f"Empty table: {table_name}")

    safe = table_name.replace(".", "_")
    ds = context.data_sources.add_spark(name=f"ds_{safe}")
    asset = ds.add_dataframe_asset(name=f"asset_{safe}")
    bd = asset.add_batch_definition_whole_dataframe(name=f"batch_{safe}")
    batch = bd.get_batch(batch_parameters={"dataframe": df})

    checks = []
    for c in rules.get("not_null", []):
        checks.append((f"{c}:not_null", gx.expectations.ExpectColumnValuesToNotBeNull(column=c)))
    for c in rules.get("unique", []):
        checks.append((f"{c}:unique", gx.expectations.ExpectColumnValuesToBeUnique(column=c)))
    for c in rules.get("non_negative", []):
        checks.append((f"{c}:non_negative", gx.expectations.ExpectColumnValuesToBeBetween(column=c, min_value=0)))
    for c, values in rules.get("accepted", {}).items():
        checks.append((f"{c}:accepted_values", gx.expectations.ExpectColumnValuesToBeInSet(column=c, value_set=values)))

    out = []
    for name, exp in checks:
        r = batch.validate(exp)
        out.append({"table_name": table_name, "expectation": name, "success": bool(r.success)})
        print(("PASS" if r.success else "FAIL"), table_name, name)
    return out

def main():
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    spark = create_iceberg_spark_session("RetailPulse - GX Quality")
    context = gx.get_context()
    records = []
    try:
        for table, rules in RULES.items():
            records.extend(validate_one(context, spark, table, rules))

        RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        (RESULTS_DIR / f"quality_{run_id}.json").write_text(
            json.dumps({"run_id":run_id,"checks":records}, indent=2), encoding="utf-8"
        )

        ensure_audit_table(spark)
        rows = [(run_id, r["table_name"], r["expectation"], r["success"],
                 datetime.now(timezone.utc).replace(tzinfo=None)) for r in records]
        schema = StructType([
            StructField("run_id", StringType(), False),
            StructField("table_name", StringType(), False),
            StructField("expectation", StringType(), False),
            StructField("success", BooleanType(), False),
            StructField("checked_at", TimestampType(), False),
        ])
        spark.createDataFrame(rows, schema).writeTo(AUDIT_TABLE).append()

        failed = [r for r in records if not r["success"]]
        if failed:
            raise RuntimeError(f"{len(failed)} data-quality checks failed")
        print(f"SUCCESS: {len(records)} checks passed")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
