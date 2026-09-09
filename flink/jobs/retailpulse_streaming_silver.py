"""
RetailPulse AI - Streaming Silver CDC Pipeline

Kafka CDC (Debezium)
        ↓
     PyFlink
        ↓
Streaming Silver
        ↓
MinIO / Parquet

This job:
- Reads Debezium CDC events from Kafka
- Handles r/c/u/d operations
- Preserves the business columns
- Adds CDC metadata
- Writes streaming Silver data to MinIO
- Uses Flink checkpoints for reliable processing

NOTE:
This first Streaming Silver layer intentionally stores CDC change events.
It does NOT yet maintain a physically materialized current-state table.
The Streaming Gold layer will consume these Silver CDC events.
"""

import json
from datetime import datetime, timezone

from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy

from pyflink.datastream import (
    StreamExecutionEnvironment,
)

from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)

from pyflink.table import StreamTableEnvironment

from pyflink.common import Row


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP = "kafka:9092"

SILVER_BASE = "s3://retailpulse/silver_stream"

GROUP_PREFIX = "retailpulse-streaming-silver"


# ============================================================
# KAFKA TOPICS
# ============================================================

TABLES = {
    "customers": "retailpulse.ecommerce.customers",
    "products": "retailpulse.ecommerce.products",
    "orders": "retailpulse.ecommerce.orders",
    "order_items": "retailpulse.ecommerce.order_items",
    "payments": "retailpulse.ecommerce.payments",
    "inventory": "retailpulse.ecommerce.inventory",
    "website_events": "retailpulse.ecommerce.website_events",
    "support_tickets": "retailpulse.ecommerce.support_tickets",
    "marketing_events": "retailpulse.ecommerce.marketing_events",
}


# ============================================================
# TABLE SCHEMAS
# ============================================================

SCHEMAS = {

    "customers": [
        ("customer_id", "long"),
        ("first_name", "string"),
        ("last_name", "string"),
        ("email", "string"),
        ("phone", "string"),
        ("country", "string"),
        ("state", "string"),
        ("city", "string"),
        ("signup_date", "string"),
        ("customer_segment", "string"),
        ("updated_at", "string"),
        ("created_at", "string"),
    ],

    "products": [
        ("product_id", "long"),
        ("product_name", "string"),
        ("category", "string"),
        ("subcategory", "string"),
        ("brand", "string"),
        ("price", "double"),
        ("cost", "double"),
        ("supplier_id", "long"),
        ("inventory_quantity", "long"),
        ("created_at", "string"),
        ("updated_at", "string"),
    ],

    "orders": [
        ("order_id", "long"),
        ("customer_id", "long"),
        ("order_date", "string"),
        ("status", "string"),
        ("payment_method", "string"),
        ("shipping_country", "string"),
        ("shipping_state", "string"),
        ("total_amount", "double"),
        ("discount", "double"),
        ("tax", "double"),
        ("created_at", "string"),
        ("updated_at", "string"),
    ],

    "order_items": [
        ("order_item_id", "long"),
        ("order_id", "long"),
        ("product_id", "long"),
        ("quantity", "long"),
        ("unit_price", "double"),
        ("discount", "double"),
    ],

    "payments": [
        ("payment_id", "long"),
        ("order_id", "long"),
        ("customer_id", "long"),
        ("amount", "double"),
        ("payment_method", "string"),
        ("payment_status", "string"),
        ("transaction_timestamp", "string"),
    ],

    "inventory": [
        ("inventory_id", "long"),
        ("product_id", "long"),
        ("warehouse_id", "long"),
        ("quantity", "long"),
        ("reserved_quantity", "long"),
        ("updated_at", "string"),
    ],

    "website_events": [
        ("event_id", "string"),
        ("customer_id", "long"),
        ("session_id", "string"),
        ("event_type", "string"),
        ("product_id", "long"),
        ("event_timestamp", "string"),
        ("device", "string"),
        ("browser", "string"),
        ("ip_address", "string"),
    ],

    "support_tickets": [
        ("ticket_id", "long"),
        ("customer_id", "long"),
        ("created_at", "string"),
        ("category", "string"),
        ("priority", "string"),
        ("message", "string"),
        ("status", "string"),
        ("resolution_time_minutes", "long"),
    ],

    "marketing_events": [
        ("marketing_event_id", "long"),
        ("campaign_id", "long"),
        ("customer_id", "long"),
        ("campaign", "string"),
        ("channel", "string"),
        ("impression", "long"),
        ("click", "long"),
        ("conversion", "long"),
        ("cost", "double"),
        ("event_timestamp", "string"),
    ],
}


# ============================================================
# METADATA
# ============================================================

SILVER_METADATA_FIELDS = [
    "_op",
    "_ts_ms",
    "_source_table",
    "_silver_processed_at",
]


# ============================================================
# TYPE CONVERSION
# ============================================================

def convert_value(value, data_type):
    """
    Convert Debezium JSON values into the expected Python type.
    """

    if value is None:
        return None

    try:

        if data_type == "long":
            return int(value)

        if data_type == "double":
            return float(value)

        if data_type == "string":
            return str(value)

        return value

    except (TypeError, ValueError):
        return None


# ============================================================
# DEBEZIUM PARSER
# ============================================================

class DebeziumParser:

    def __init__(self, table_name):
        self.table_name = table_name

    def parse(self, message):

        try:

            data = json.loads(message)

            payload = data.get("payload", data)

            op = payload.get("op")

            ts_ms = payload.get("ts_ms")

            before = payload.get("before")

            after = payload.get("after")

            # ------------------------------------------------
            # Debezium operation handling
            # ------------------------------------------------

            if op == "d":

                record = before

            else:

                record = after

            if record is None:

                return None

            result = {}

            # ------------------------------------------------
            # Business columns
            # ------------------------------------------------

            for field_name, field_type in SCHEMAS[self.table_name]:

                value = record.get(field_name)

                result[field_name] = convert_value(
                    value,
                    field_type,
                )

            # ------------------------------------------------
            # CDC metadata
            # ------------------------------------------------

            result["_op"] = str(op) if op is not None else None

            if ts_ms is not None:

                try:
                    result["_ts_ms"] = int(ts_ms)

                except (TypeError, ValueError):
                    result["_ts_ms"] = None

            else:

                result["_ts_ms"] = None

            result["_source_table"] = self.table_name

            result["_silver_processed_at"] = (
                datetime.now(timezone.utc).isoformat()
            )

            return result

        except Exception:

            return None


# ============================================================
# STREAMING SILVER TRANSFORM
# ============================================================

class StreamingSilverTransform:

    def __init__(self, table_name):

        self.table_name = table_name

    def transform(self, record):

        if record is None:
            return None

        op = record.get("_op")

        # ----------------------------------------------------
        # Valid Debezium operations
        # ----------------------------------------------------

        if op not in ("r", "c", "u", "d"):

            return None

        values = []

        # ----------------------------------------------------
        # Business fields
        # ----------------------------------------------------

        for field_name, field_type in SCHEMAS[self.table_name]:

            values.append(
                record.get(field_name)
            )

        # ----------------------------------------------------
        # Metadata
        # ----------------------------------------------------

        values.append(record.get("_op"))

        values.append(record.get("_ts_ms"))

        values.append(
            record.get("_source_table")
        )

        values.append(
            record.get("_silver_processed_at")
        )

        return Row(*values)


# ============================================================
# PYFLINK ROW TYPE
# ============================================================

def get_row_type(table_name):

    fields = []

    for field_name, field_type in SCHEMAS[table_name]:

        if field_type == "long":

            fields.append(
                Types.LONG()
            )

        elif field_type == "double":

            fields.append(
                Types.DOUBLE()
            )

        else:

            fields.append(
                Types.STRING()
            )

    # Metadata

    fields.append(Types.STRING())   # _op
    fields.append(Types.LONG())     # _ts_ms
    fields.append(Types.STRING())   # _source_table
    fields.append(Types.STRING())   # _silver_processed_at

    return Types.ROW_NAMED(
        [
            field_name
            for field_name, _ in SCHEMAS[table_name]
        ]
        + SILVER_METADATA_FIELDS,
        fields,
    )


# ============================================================
# PARQUET SCHEMA
# ============================================================

def get_parquet_schema(table_name):

    fields = []

    for field_name, field_type in SCHEMAS[table_name]:

        if field_type == "long":

            fields.append(
                f"{field_name}: int64"
            )

        elif field_type == "double":

            fields.append(
                f"{field_name}: double"
            )

        else:

            fields.append(
                f"{field_name}: string"
            )

    fields.extend(
        [
            "_op: string",
            "_ts_ms: int64",
            "_source_table: string",
            "_silver_processed_at: string",
        ]
    )

    return fields


# ============================================================
# KAFKA SOURCE
# ============================================================

def create_kafka_source(
    topic,
    table_name,
):

    return (
        KafkaSource.builder()
        .set_bootstrap_servers(
            KAFKA_BOOTSTRAP
        )
        .set_topics(topic)
        .set_group_id(
            f"{GROUP_PREFIX}-{table_name}"
        )
        .set_starting_offsets(
            KafkaOffsetsInitializer.earliest()
        )
        .set_value_only_deserializer(
            SimpleStringSchema()
        )
        .build()
    )


# ============================================================
# PARQUET TABLE SINK
# ============================================================

def create_parquet_sink(
    table_env,
    statement_set,
    table_name,
    view_name,
):

    output_path = (
        f"{SILVER_BASE}/{table_name}"
    )

    columns = []

    for field_name, field_type in SCHEMAS[table_name]:
        flink_type = {
            "long": "BIGINT",
            "double": "DOUBLE",
            "string": "STRING",
        }[field_type]
        columns.append(f"`{field_name}` {flink_type}")

    columns.extend(
        [
            "`_op` STRING",
            "`_ts_ms` BIGINT",
            "`_source_table` STRING",
            "`_silver_processed_at` STRING",
        ]
    )

    sink_name = f"silver_sink_{table_name}"

    table_env.execute_sql(
        f"""
        CREATE TEMPORARY TABLE `{sink_name}` (
            {', '.join(columns)}
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{output_path}',
            'format' = 'parquet'
        )
        """
    )

    statement_set.add_insert_sql(
        f"INSERT INTO `{sink_name}` SELECT * FROM `{view_name}`"
    )


# ============================================================
# BUILD TABLE PIPELINE
# ============================================================

def build_table_pipeline(
    env,
    table_env,
    statement_set,
    table_name,
    topic,
):

    print(
        f"Creating Streaming Silver pipeline: "
        f"{table_name}"
    )

    # --------------------------------------------------------
    # Kafka source
    # --------------------------------------------------------

    source = create_kafka_source(
        topic,
        table_name,
    )

    stream = (
        env.from_source(
            source,
            WatermarkStrategy.no_watermarks(),
            f"Kafka CDC - {table_name}",
        )
    )

    # --------------------------------------------------------
    # Parse Debezium
    # --------------------------------------------------------

    parser = DebeziumParser(
        table_name
    )

    parsed_stream = (
        stream
        .map(
            parser.parse,
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
    )

    # --------------------------------------------------------
    # Transform into Silver Row
    # --------------------------------------------------------

    transformer = StreamingSilverTransform(
        table_name
    )

    row_type = get_row_type(
        table_name
    )

    silver_stream = (
        parsed_stream
        .map(
            transformer.transform,
            output_type=row_type,
        )
        .filter(
            lambda x: x is not None,
        )
    )

    # --------------------------------------------------------
    # Register a Table API Parquet sink.  PyFlink 2.1 does not expose
    # pyflink.formats.parquet, while the JVM filesystem connector does.
    # --------------------------------------------------------

    view_name = f"silver_view_{table_name}"
    table_env.create_temporary_view(
        view_name,
        silver_stream,
    )

    create_parquet_sink(
        table_env,
        statement_set,
        table_name,
        view_name,
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print(
        "=" * 70
    )

    print(
        "RetailPulse AI - Streaming Silver CDC"
    )

    print(
        "=" * 70
    )

    # --------------------------------------------------------
    # Execution environment
    # --------------------------------------------------------

    env = (
        StreamExecutionEnvironment
        .get_execution_environment()
    )

    table_env = StreamTableEnvironment.create(env)
    statement_set = table_env.create_statement_set()

    # --------------------------------------------------------
    # Parallelism
    # --------------------------------------------------------

    env.set_parallelism(2)

    # --------------------------------------------------------
    # Checkpointing
    # --------------------------------------------------------

    env.enable_checkpointing(
        10_000
    )

    checkpoint_config = (
        env.get_checkpoint_config()
    )

    checkpoint_config.set_checkpoint_timeout(
        120_000
    )

    checkpoint_config.set_min_pause_between_checkpoints(
        30_000
    )

    checkpoint_config.set_max_concurrent_checkpoints(
        1
    )

    # --------------------------------------------------------
    # Build all table pipelines
    # --------------------------------------------------------

    for table_name, topic in TABLES.items():

        build_table_pipeline(
            env,
            table_env,
            statement_set,
            table_name,
            topic,
        )

    # --------------------------------------------------------
    # Execute
    # --------------------------------------------------------

    print(
        "Submitting Streaming Silver job..."
    )

    statement_set.execute()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    main()