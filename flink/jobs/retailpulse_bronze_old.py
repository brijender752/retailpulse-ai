import json
import os
import uuid
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

from minio import Minio

from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)
from pyflink.datastream.functions import MapFunction


# ============================================================
# Configuration
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092",
)

KAFKA_GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID",
    "retailpulse-flink-bronze-v2",
)

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "minio:9000",
)

MINIO_ACCESS_KEY = os.getenv(
    "MINIO_ACCESS_KEY",
    "minioadmin",
)

MINIO_SECRET_KEY = os.getenv(
    "MINIO_SECRET_KEY",
    "minioadmin",
)

MINIO_BUCKET = os.getenv(
    "MINIO_BUCKET",
    "retailpulse",
)


# ============================================================
# CDC Topics
# ============================================================

CDC_TOPICS = [
    "retailpulse.ecommerce.customers",
    "retailpulse.ecommerce.products",
    "retailpulse.ecommerce.orders",
    "retailpulse.ecommerce.order_items",
    "retailpulse.ecommerce.payments",
    "retailpulse.ecommerce.inventory",
    "retailpulse.ecommerce.website_events",
    "retailpulse.ecommerce.support_tickets",
    "retailpulse.ecommerce.marketing_events",
]


# ============================================================
# Parse Debezium CDC
# ============================================================

class ParseDebeziumEvent(MapFunction):

    def map(self, value):

        try:

            event = json.loads(value)

            operation_code = event.get("op")

            before = event.get("before")
            after = event.get("after")

            source = event.get("source") or {}

            ts_ms = event.get("ts_ms")

            # ------------------------------------------------
            # Determine operation
            # ------------------------------------------------

            if operation_code == "c":

                operation = "INSERT"
                record = after

            elif operation_code == "u":

                operation = "UPDATE"
                record = after

            elif operation_code == "d":

                operation = "DELETE"
                record = before

            elif operation_code == "r":

                operation = "SNAPSHOT"
                record = after

            else:

                operation = "UNKNOWN"
                record = after

            if record is None:
                record = {}

            # ------------------------------------------------
            # Determine source table
            # ------------------------------------------------

            source_table = source.get(
                "table",
                "unknown",
            )

            source_schema = source.get(
                "schema",
                "unknown",
            )

            # ------------------------------------------------
            # Timestamp
            # ------------------------------------------------

            if ts_ms:

                event_datetime = datetime.fromtimestamp(
                    ts_ms / 1000,
                    tz=timezone.utc,
                ).isoformat()

            else:

                event_datetime = None

            # ------------------------------------------------
            # Normalized CDC record
            # ------------------------------------------------

            return {
                "event_id": str(
                    uuid.uuid4()
                ),

                "source_table": source_table,

                "source_schema": source_schema,

                "operation": operation,

                "operation_code": operation_code,

                "record": json.dumps(
                    record,
                    default=str,
                ),

                "event_timestamp": event_datetime,

                "source_lsn": source.get("lsn"),

                "source_txid": source.get(
                    "txId"
                ),

                "source_snapshot": str(
                    source.get("snapshot")
                ),

                "ingested_at": datetime.now(
                    timezone.utc
                ).isoformat(),
            }

        except Exception as exc:

            return {
                "event_id": str(
                    uuid.uuid4()
                ),

                "source_table": "unknown",

                "source_schema": "unknown",

                "operation": "ERROR",

                "operation_code": None,

                "record": None,

                "event_timestamp": None,

                "source_lsn": None,

                "source_txid": None,

                "source_snapshot": None,

                "ingested_at": datetime.now(
                    timezone.utc
                ).isoformat(),

                "error_message": str(exc),
            }


# ============================================================
# Write Bronze record to MinIO
# ============================================================

class WriteBronzeToMinIO(MapFunction):

    def __init__(self):

        self.client = None

    def open(self, runtime_context):

        self.client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )

        if not self.client.bucket_exists(
            MINIO_BUCKET
        ):

            self.client.make_bucket(
                MINIO_BUCKET
            )

    def map(self, record):

        # ----------------------------------------------------
        # Current UTC time
        # ----------------------------------------------------

        now = datetime.now(
            timezone.utc
        )

        # ----------------------------------------------------
        # Arrow table
        # ----------------------------------------------------

        table = pa.table(
            {
                "event_id": [
                    record.get("event_id")
                ],

                "source_schema": [
                    record.get(
                        "source_schema"
                    )
                ],

                "source_table": [
                    record.get(
                        "source_table"
                    )
                ],

                "operation": [
                    record.get(
                        "operation"
                    )
                ],

                "operation_code": [
                    record.get(
                        "operation_code"
                    )
                ],

                "record": [
                    record.get("record")
                ],

                "event_timestamp": [
                    record.get(
                        "event_timestamp"
                    )
                ],

                "source_lsn": [
                    record.get(
                        "source_lsn"
                    )
                ],

                "source_txid": [
                    record.get(
                        "source_txid"
                    )
                ],

                "source_snapshot": [
                    record.get(
                        "source_snapshot"
                    )
                ],

                "ingested_at": [
                    record.get(
                        "ingested_at"
                    )
                ],
            }
        )

        # ----------------------------------------------------
        # Temporary Parquet file
        # ----------------------------------------------------

        local_file = (
            f"/tmp/"
            f"cdc_{uuid.uuid4().hex}.parquet"
        )

        pq.write_table(
            table,
            local_file,
            compression="snappy",
        )

        # ----------------------------------------------------
        # Table-specific Bronze path
        # ----------------------------------------------------

        source_table = record.get(
            "source_table",
            "unknown",
        )

        object_name = (
            "bronze/"
            f"{source_table}/"
            f"year={now.year}/"
            f"month={now.month:02d}/"
            f"day={now.day:02d}/"
            f"hour={now.hour:02d}/"
            f"cdc_{uuid.uuid4().hex}.parquet"
        )

        # ----------------------------------------------------
        # Upload
        # ----------------------------------------------------

        self.client.fput_object(
            MINIO_BUCKET,
            object_name,
            local_file,
            content_type=(
                "application/octet-stream"
            ),
        )

        # ----------------------------------------------------
        # Cleanup
        # ----------------------------------------------------

        try:

            os.remove(local_file)

        except OSError:

            pass

        print(
            "BRONZE WRITE:",
            f"s3://{MINIO_BUCKET}/{object_name}",
        )

        return record


# ============================================================
# Main
# ============================================================

def main():

    env = (
        StreamExecutionEnvironment
        .get_execution_environment()
    )

    # ========================================================
    # Checkpointing
    # ========================================================

    env.enable_checkpointing(
        60_000
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

    env.set_parallelism(2)

    # --------------------------------------------------------
    # Kafka source
    # --------------------------------------------------------

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(
            KAFKA_BOOTSTRAP_SERVERS
        )
        .set_topics(
            *CDC_TOPICS
        )
        .set_group_id(
            KAFKA_GROUP_ID
        )
        .set_starting_offsets(
            KafkaOffsetsInitializer.earliest()
        )
        .set_value_only_deserializer(
            SimpleStringSchema()
        )
        .build()
    )

    # --------------------------------------------------------
    # Kafka → Flink
    # --------------------------------------------------------

    stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "RetailPulse CDC Kafka Source",
    )

    # --------------------------------------------------------
    # Parse Debezium
    # --------------------------------------------------------

    parsed_stream = stream.map(
        ParseDebeziumEvent()
    )

    # --------------------------------------------------------
    # Write Bronze
    # --------------------------------------------------------

    bronze_stream = parsed_stream.map(
        WriteBronzeToMinIO()
    )

    bronze_stream.print(
        "BRONZE_CDC"
    )

    # --------------------------------------------------------
    # Execute
    # --------------------------------------------------------

    env.execute(
        "RetailPulse PyFlink Multi-Table Bronze"
    )


if __name__ == "__main__":

    main()
