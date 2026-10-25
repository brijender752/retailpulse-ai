import os
import time
import threading
import json
from pathlib import Path

import requests
import psycopg2

from prometheus_client import (
    Gauge,
    Counter,
    start_http_server,
    REGISTRY,
)
from prometheus_client.core import GaugeMetricFamily


class SparkIcebergMetrics:
    """Read atomic status files on each scrape; ages continue growing between checks."""
    def collect(self):
        now = time.time()
        families = {}

        def add(name, help_text, labels, values, value):
            if value is None:
                return
            if name not in families:
                families[name] = GaugeMetricFamily(name, help_text, labels=labels)
            families[name].add_metric(values, value)

        directory = Path('/opt/monitoring-state')
        try:
            payload = json.loads((directory / 'iceberg.json').read_text())
            add('retailpulse_iceberg_check_timestamp_seconds', 'Latest completed table check', [], [], payload['checked_at'])
            for table, record in payload['tables'].items():
                add('retailpulse_iceberg_table_check_success', 'Latest table check succeeded', ['table'], [table], record['success'])
                if not record['success']:
                    continue
                stamp = record.get('snapshot_timestamp')
                add('retailpulse_iceberg_snapshot_age_seconds', 'Age of latest Iceberg snapshot commit', ['table'], [table], None if stamp is None else now - stamp)
                stamp = record.get('data_timestamp')
                add('retailpulse_table_data_timestamp_available', 'A usable data timestamp was found', ['table'], [table], int(stamp is not None))
                add('retailpulse_table_data_age_seconds', 'Age of latest selected source or processing timestamp', ['table', 'timestamp_column'], [table, record.get('timestamp_column', '')], None if stamp is None else now - stamp)
        except (OSError, ValueError, KeyError):
            add('retailpulse_iceberg_check_timestamp_seconds', 'Latest completed table check', [], [], 0)
        for path in (directory / 'jobs').glob('*.json'):
            try:
                record = json.loads(path.read_text())
                job = [record['job']]
                add('retailpulse_spark_job_running', 'Latest submitted job has not reported completion', ['job'], job, record['running'])
                add('retailpulse_spark_job_start_timestamp_seconds', 'Latest job start time', ['job'], job, record['started'])
                if 'completed' in record:
                    add('retailpulse_spark_job_duration_seconds', 'Latest completed job duration', ['job'], job, record['duration'])
                    add('retailpulse_spark_job_success', 'Latest completed job succeeded', ['job'], job, record['success'])
                    add('retailpulse_spark_job_completion_timestamp_seconds', 'Latest job completion', ['job'], job, record['completed'])
            except (OSError, ValueError, KeyError):
                continue
        yield from families.values()


REGISTRY.register(SparkIcebergMetrics())


CHECK_INTERVAL = 60

CONNECT_URL = os.getenv(
    "CONNECT_URL",
    "http://debezium:8083",
)

FLINK_URL = os.getenv(
    "FLINK_URL",
    "http://flink-jobmanager:8081",
)

POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN",
    "",
)


collector_success = Gauge(
    "retailpulse_collector_success",
    "Whether the last collector run succeeded",
    ["stage"],
)

collector_last_success = Gauge(
    "retailpulse_collector_last_success_timestamp_seconds",
    "Unix time of last successful collection",
    ["stage"],
)

connector_running = Gauge(
    "retailpulse_debezium_connector_running",
    "Kafka Connect connector running status",
    ["connector"],
)

connector_tasks_running = Gauge(
    "retailpulse_debezium_tasks_running",
    "Number of running connector tasks",
    ["connector"],
)

connector_tasks_total = Gauge(
    "retailpulse_debezium_tasks_total",
    "Total number of connector tasks",
    ["connector"],
)

flink_running_jobs = Gauge(
    "retailpulse_flink_running_jobs",
    "Number of running Flink jobs",
)

flink_failed_jobs = Gauge(
    "retailpulse_flink_failed_jobs",
    "Number of failed jobs in Flink REST overview",
)

postgres_slot_retained_bytes = Gauge(
    "retailpulse_postgres_slot_retained_bytes",
    "WAL bytes retained by logical replication slot",
    ["slot"],
)

postgres_slot_active = Gauge(
    "retailpulse_postgres_slot_active",
    "Whether a replication slot is active",
    ["slot"],
)

collector_errors = Counter(
    "retailpulse_collector_errors_total",
    "Total collector errors",
    ["stage"],
)


def collect_debezium():
    response = requests.get(
        f"{CONNECT_URL}/connectors",
        timeout=10,
    )
    response.raise_for_status()

    connectors = response.json()

    for connector in connectors:
        response = requests.get(
            f"{CONNECT_URL}/connectors/{connector}/status",
            timeout=10,
        )
        response.raise_for_status()

        status = response.json()

        connector_running.labels(
            connector=connector
        ).set(
            int(
                status.get("connector", {}).get("state")
                == "RUNNING"
            )
        )

        tasks = status.get("tasks", [])

        connector_tasks_total.labels(
            connector=connector
        ).set(len(tasks))

        connector_tasks_running.labels(
            connector=connector
        ).set(
            sum(
                task.get("state") == "RUNNING"
                for task in tasks
            )
        )


def collect_flink():
    response = requests.get(
        f"{FLINK_URL}/jobs/overview",
        timeout=10,
    )
    response.raise_for_status()

    jobs = response.json().get("jobs", [])

    flink_running_jobs.set(
        sum(
            job.get("state") == "RUNNING"
            for job in jobs
        )
    )

    flink_failed_jobs.set(
        sum(
            job.get("state") == "FAILED"
            for job in jobs
        )
    )


def collect_postgres():
    if not POSTGRES_DSN:
        raise RuntimeError("POSTGRES_DSN is missing")

    conn = psycopg2.connect(
        POSTGRES_DSN,
        connect_timeout=5,
    )

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    slot_name,
                    active,
                    pg_wal_lsn_diff(
                        pg_current_wal_lsn(),
                        restart_lsn
                    )
                FROM pg_replication_slots
                WHERE slot_type = 'logical'
            """)

            for slot, active, retained in cur.fetchall():
                postgres_slot_active.labels(
                    slot=slot
                ).set(int(active))

                if retained is not None:
                    postgres_slot_retained_bytes.labels(
                        slot=slot
                    ).set(float(retained))
    finally:
        conn.close()


COLLECTORS = {
    "debezium": collect_debezium,
    "flink": collect_flink,
    "postgres": collect_postgres,
}


def collect_forever():
    while True:
        for stage, collector in COLLECTORS.items():
            try:
                collector()
                collector_success.labels(stage).set(1)
                collector_last_success.labels(stage).set(
                    time.time()
                )
            except Exception as exc:
                collector_success.labels(stage).set(0)
                collector_errors.labels(stage).inc()
                print(
                    f"Collector {stage} failed: {exc}",
                    flush=True,
                )

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    start_http_server(9105)

    worker = threading.Thread(
        target=collect_forever,
        daemon=True,
    )
    worker.start()

    worker.join()
