"""Initialization and readiness checks for the manual streaming workflow."""
import json
import logging
import os
from pathlib import Path
import sys

import psycopg2
from psycopg2 import sql
import requests

from retailpulse.config import POSTGRES, DEBEZIUM_URL, DEBEZIUM_CONNECTOR, MINIO_BUCKET
from retailpulse.health import get_minio_client

PROJECT = Path("/opt/retailpulse")
ENTITIES = (
    "customers", "products", "orders", "order_items", "payments",
    "inventory", "website_events", "support_tickets", "marketing_events",
)


def initialize_source():
    """Create missing schema and seed only a completely empty source database."""
    connection = psycopg2.connect(**POSTGRES, connect_timeout=10)
    try:
        with connection:
            with connection.cursor() as cursor:
                for filename in ("01_create_schemas.sql", "02_create_tables.sql"):
                    cursor.execute((PROJECT / "sql" / filename).read_text())
                counts = {}
                for name in ENTITIES:
                    cursor.execute(sql.SQL("SELECT COUNT(*) FROM ecommerce.{}").format(sql.Identifier(name)))
                    counts[name] = cursor.fetchone()[0]
    finally:
        connection.close()
    if all(counts.values()):
        return {"source": "existing", "counts": counts}
    if any(counts.values()):
        raise RuntimeError(f"Source is partially populated; complete the missing entities before retrying: {counts}")

    # Import after supplying container DNS/configuration; dotenv must not select localhost.
    for key, value in POSTGRES.items():
        os.environ[f"POSTGRES_{'DB' if key == 'database' else key.upper()}"] = str(value)
    sys.path.insert(0, str(PROJECT))
    from data_generator.config import DATA_VOLUME
    from data_generator.main import main
    DATA_VOLUME.update(customers=100, products=50, orders=500,
                       events=2000, support_tickets=100, marketing_events=500)
    main()
    return {"source": "seeded", "volumes": DATA_VOLUME}


def ensure_bucket():
    client = get_minio_client()
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
    return MINIO_BUCKET


def ensure_connector():
    response = requests.get(f"{DEBEZIUM_URL}/connectors/{DEBEZIUM_CONNECTOR}/config", timeout=15)
    if response.status_code != 404:
        response.raise_for_status()
        return "Existing connector retained"
    payload = json.loads((PROJECT / "ingestion/debezium/postgres-connector.json").read_text())
    payload["name"] = DEBEZIUM_CONNECTOR
    for key, value in POSTGRES.items():
        name = {"host": "hostname", "database": "dbname"}.get(key, key)
        payload["config"][f"database.{name}"] = str(value)
    response = requests.post(f"{DEBEZIUM_URL}/connectors", json=payload, timeout=30)
    response.raise_for_status()
    return "Created connector with initial snapshot"


def bronze_files_ready():
    """Only finalized, nonempty Parquet files count as readable Bronze data."""
    client = get_minio_client()
    missing = []
    for name in ENTITIES:
        prefix = f"streaming/bronze/{name}/"
        if not any(obj.object_name.endswith(".parquet") and obj.size > 0
                   for obj in client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True)):
            missing.append(name)
    if missing:
        logging.info("Waiting for committed Bronze Parquet for: %s", ", ".join(missing))
    return not missing
