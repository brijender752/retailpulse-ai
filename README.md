# RetailPulse AI

**Maintainer:** `bj_kush`

An end-to-end real-time data engineering and GenAI platform for e-commerce analytics.

## Local service URLs

Start the stack from the repository root with `docker compose up -d --build`.
These addresses use the host ports in `docker-compose.yml`.

| Service | Local URL | Purpose |
| --- | --- | --- |
| Airflow | [localhost:8090](http://localhost:8090) | DAGs, runs, task logs, and scheduling |
| Superset | [localhost:8088](http://localhost:8088) | Datasets, charts, SQL Lab, and dashboards |
| Kafka UI | [localhost:8080](http://localhost:8080) | Kafka topics, messages, and consumer groups |
| Flink | [localhost:8081](http://localhost:8081) | Streaming jobs, checkpoints, and task managers |
| Debezium / Kafka Connect | [localhost:8083](http://localhost:8083) | Connector REST API |
| Debezium connectors | [localhost:8083/connectors](http://localhost:8083/connectors) | Registered CDC connectors |
| MinIO console | [localhost:9001](http://localhost:9001) | Browse buckets and lakehouse files |
| MinIO S3 API | [localhost:9000](http://localhost:9000) | S3-compatible endpoint for storage clients |

### Health and status endpoints

| Service | Endpoint |
| --- | --- |
| Superset | [Health](http://localhost:8088/health) |
| Airflow | [Component health](http://localhost:8090/api/v2/monitor/health) |
| MinIO | [Live health](http://localhost:9000/minio/health/live) |
| Flink | [Cluster overview](http://localhost:8081/overview) |
| Debezium PostgreSQL connector | [Connector status](http://localhost:8083/connectors/retailpulse-postgres-connector/status) |

### Superset dashboards

These links refer to the dashboards created in the current local Superset database.
IDs can change if dashboards are recreated or imported into another installation.

| Dashboard | Local link |
| --- | --- |
| Executive Overview | [Open dashboard](http://localhost:8088/superset/dashboard/3/) |
| Sales & Revenue | [Open dashboard](http://localhost:8088/superset/dashboard/4/) |
| Customer 360 | [Open dashboard](http://localhost:8088/superset/dashboard/5/) |
| Product Performance | [Open dashboard](http://localhost:8088/superset/dashboard/6/) |
| Payment Reconciliation | [Open dashboard](http://localhost:8088/superset/dashboard/7/) |
| Marketing & Support | [Open dashboard](http://localhost:8088/superset/dashboard/8/) |

### Database and messaging connections

These are client connection addresses, not browser pages.

| Service | Host address | Usage |
| --- | --- | --- |
| PostgreSQL | `localhost:5432` | SQL clients; credentials and database name are in the root `.env` |
| Kafka host listener | `localhost:9094` | Bootstrap address for applications running on your computer |
| Kafka internal listener | `localhost:9092` | Published port, but advertises `kafka:9092`; use port 9094 for host clients |
| Spark Thrift | `localhost:10000` | Hive-compatible SQL clients; SQLAlchemy URI: `hive://superset@localhost:10000/analytics` |

Inside Docker containers, use service names instead of `localhost`, for example
`http://superset:8088`, `http://minio:9000`, `kafka:9092`, and
`hive://superset@spark-thrift:10000/analytics`. Airflow listens on port 8080
inside its container and is exposed on host port 8090.

## Architecture

PostgreSQL
    ↓
Debezium CDC
    ↓
Kafka
    ↓
Flink / PySpark
    ↓
MinIO / Apache Iceberg
    ↓
dbt
    ↓
Analytical Warehouse
    ↓
Superset / Grafana
    ↓
GenAI AI Analyst

## Pipeline organization

- `spark/jobs/batch/` — bounded Spark jobs, grouped into Bronze, Silver, and Gold.
- `flink/jobs/streaming/` — continuous Flink jobs, grouped into Bronze, Silver, and Gold.
- `flink/jobs/archive/` — historical experiments; do not submit these as active jobs.
- `docs/architecture/pipelines.md` — pipeline ownership and run commands.
- `airflow/README_PHASE1.md` — Airflow setup, DAGs, control-plane commands,
  and troubleshooting.

## Technologies

- Python
- PostgreSQL
- Apache Kafka
- Debezium
- Apache Flink
- Apache Spark / PySpark
- Apache Iceberg
- MinIO
- dbt
- Apache Airflow
- Apache Superset
- Grafana
- Prometheus
- Docker
- Machine Learning
- RAG
- LLM
- FastAPI

## Project Goals

1. Build a batch data pipeline.
2. Build a CDC pipeline.
3. Build real-time streaming pipelines.
4. Build a lakehouse.
5. Build an analytics warehouse.
6. Implement data quality.
7. Build ML pipelines.
8. Build a GenAI analytics assistant.
9. Implement monitoring.
10. Deploy the platform using containers.

## Current Architecture

PostgreSQL
    ↓
E-commerce source tables
    ↓
Python data generator
    ↓
Realistic transactional data

Current phase:
PostgreSQL + Data Generation

## Docker commands from Git Bash on Windows

Git Bash/MSYS automatically converts arguments that begin with `/` into Windows
paths. That breaks `docker exec` when the command or file path exists only
inside a Linux container. Use a double leading slash for container paths;
Docker normalizes it to a single slash:

```bash
# Kafka
docker exec retailpulse-kafka //opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Flink
docker compose up -d --build flink-jobmanager flink-taskmanager
docker exec retailpulse-flink-jobmanager sh -lc 'flink run -py /opt/flink/jobs/streaming/bronze/retailpulse_bronze.py'
```

This avoids needing `MSYS_NO_PATHCONV=1` while preserving the normal single-slash
Linux path inside the container. Use normal relative paths such as
`./flink/jobs` for host-side paths in `docker compose` commands and Compose
files.

## Complete runbook: first command to final validation

The following is the complete local-development sequence used for this
project. Commands below are written for Git Bash unless marked PowerShell.

### 1. Enter the project and activate Python

```bash
cd ~/projects/retailpulse-ai
source .venv/Scripts/activate
```

The virtual environment is kept in place; do not delete it. For PowerShell,
use `\.venv\Scripts\Activate.ps1` instead.

### 2. Start the core platform

```bash
docker compose up -d --build
docker compose ps
```

The core stack starts PostgreSQL, MinIO, Kafka, Debezium, the Flink
JobManager/TaskManager, Kafka UI, and the topic initializer. Check Kafka topics
when needed:

```bash
docker exec retailpulse-kafka //opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

### 3. Submit the streaming Bronze job (optional manual start)

```bash
docker exec retailpulse-flink-jobmanager sh -lc \
  'flink run -py //opt/flink/jobs/streaming/bronze/retailpulse_bronze.py'
```

Check the Flink UI at http://localhost:8081. The `jdk.compiler` export messages
printed by Flink are warnings and can be ignored.

### 4. Run bounded Spark batch processing

Run these from the repository root in PowerShell:

```powershell
& .\.venv\Scripts\python.exe -m spark.jobs.batch.bronze.postgres_to_bronze
& .\.venv\Scripts\python.exe -m spark.jobs.batch.silver.bronze_to_silver
& .\.venv\Scripts\python.exe -m spark.jobs.batch.gold.silver_to_gold
```

Use PowerShell for native Windows PySpark. If Spark reports `WinError 2`,
verify Java and `JAVA_HOME`; if it cannot infer a Parquet schema, verify that
the expected Bronze prefix contains Parquet files.

### 5. Configure and start Airflow

```bash
cd ~/projects/retailpulse-ai/airflow
cp .env.example .env            # first run only
docker compose --env-file .env -f docker-compose.airflow.yml up airflow-init
docker compose --env-file .env -f docker-compose.airflow.yml \
  up -d --build --force-recreate
docker compose --env-file .env -f docker-compose.airflow.yml ps
```

Open the Airflow UI at http://localhost:8090. The Airflow 3 task execution URL
must be `http://airflow-apiserver:8080/execution/`; the shared JWT secret and
issuer are configured in `airflow/docker-compose.airflow.yml`.

### 6. Validate and trigger Airflow DAGs

```bash
docker compose --env-file .env -f docker-compose.airflow.yml exec \
  airflow-scheduler airflow dags list-import-errors

docker compose --env-file .env -f docker-compose.airflow.yml exec \
  airflow-scheduler airflow dags list

docker compose --env-file .env -f docker-compose.airflow.yml exec \
  airflow-scheduler airflow dags trigger retailpulse_platform_health

docker compose --env-file .env -f docker-compose.airflow.yml exec \
  airflow-scheduler airflow dags trigger retailpulse_streaming_controller
```

Run `retailpulse_platform_health` first. The streaming controller starts the
registered Flink jobs, waits for Customer 360 checkpoint completion, and then
checks MinIO output.

### 7. Inspect jobs, checkpoints, and outputs

```bash
docker exec retailpulse-flink-jobmanager flink list -r
docker exec retailpulse-flink-jobmanager sh -lc \
  'python3 -m py_compile //opt/flink/jobs/streaming/gold/retailpulse_streaming_gold_customer360_recovery.py'
docker compose --env-file .env -f docker-compose.airflow.yml logs --tail 150 \
  airflow-scheduler airflow-apiserver
```

To submit Customer 360 manually for a Flink-only test, use the same explicit
pipeline name used by Airflow:

```bash
docker exec retailpulse-flink-jobmanager sh -lc \
  "flink run -d -D 'pipeline.name=RetailPulse - Customer 360 Stateful Recovery' -py //opt/flink/jobs/streaming/gold/retailpulse_streaming_gold_customer360_recovery.py"
```

Expected MinIO prefixes are:

```text
s3://retailpulse/bronze/
s3://retailpulse/silver_stream/
s3://retailpulse/gold_stream/
s3://retailpulse/gold_stream/customer_360_recovery/
s3://retailpulse/flink-checkpoints/customer360-recovery/
```

If a task stops after `Pre Execute`, recreate Airflow with `--force-recreate`.
If Customer 360 reports `Flink job not found`, cancel an obsolete autogenerated
`insert-into_*` Customer 360 job and trigger the controller again. The expected
Flink name is `RetailPulse - Customer 360 Stateful Recovery`.

For the Airflow-specific architecture and troubleshooting guide, see
[`airflow/README_PHASE1.md`](airflow/README_PHASE1.md). For pipeline ownership,
see [`docs/architecture/pipelines.md`](docs/architecture/pipelines.md).


# RetailPulse Data Quality Phase

To initialize streaming through dbt with one command from Git Bash:

```bash
bash scripts/start_streaming.sh
```

This starts the containers, installs the Airflow setup dependencies, pauses the
separate streaming/Iceberg/dbt workflows, and triggers
`retailpulse_streaming_end_to_end`. If another pipeline run is still active,
the script stops before recreating services; let that run finish and retry.
Follow the run at [Airflow](http://localhost:8090).

The manual DAG creates missing PostgreSQL schemas/tables, seeds a small demo
dataset only when all source tables are empty, ensures the MinIO bucket and
Debezium connector, starts or reuses the six Flink jobs, waits for checkpoints
and committed Bronze Parquet for all nine entities, initializes missing Iceberg
Bronze tables, merges CDC into Iceberg Silver, validates Silver, and runs dbt
debug, build (models and tests), and docs generation. Existing Bronze Iceberg
tables are retained; ongoing Silver updates read streaming files directly.
Flink remains running after the DAG succeeds.

The six Flink jobs share a TaskManager configured with a 4 GB process budget
and 2 GB task heap. The previous 512 MB heap ran out of memory while replaying
the existing CDC data. The startup script applies this configuration.

For subsequent runs, trigger **retailpulse_streaming_end_to_end** in Airflow.
Keep the separate pipeline DAGs paused while it runs. The standalone incremental
Iceberg DAG is now manual to avoid independent five-minute writes overlapping
this workflow. A fresh start here means
initializing missing resources, not deleting Kafka offsets, Flink state, or data.
If Kafka history has expired and a previously snapshotted connector has no new
events, another source snapshot/replay is needed; the file sensor will not
pretend an empty source is ready.

1. The `quality` service is already included in `docker-compose.yml`.
2. Build/start:
   docker compose build quality
   docker compose up -d --no-deps --force-recreate quality
3. Check GX:
   docker exec retailpulse-quality python3 -c "import great_expectations as gx; print(gx.__version__)"
4. Run freshness:
   docker exec retailpulse-quality spark-submit /opt/retailpulse/quality/check_freshness.py
5. Run GX:
   docker exec retailpulse-quality spark-submit /opt/retailpulse/quality/validate_analytics.py
6. Restart Airflow and trigger `retailpulse_data_quality`.

Results are written to:
- quality/results/*.json
- retailpulse.control.data_quality_results

Note: if you are not generating CDC continuously, the 30-minute freshness check can fail.
Increase MAX_AGE_MINUTES during static development.

If freshness fails with `ClassNotFoundException: IcebergSparkSessionExtensions`
or `Cannot find catalog plugin class`, rebuild and recreate `quality` using step 2
(with the rest of the stack running). The quality image uses Spark 4.0.1 to match
the shared Iceberg runtime and loads Iceberg and Hadoop AWS dependencies through
`spark-defaults.conf` at startup. Setting `spark.jars.packages` only inside the
Python session builder is too late to resolve them when launched by `spark-submit`.

If freshness reports `TABLE_OR_VIEW_NOT_FOUND` for `retailpulse.silver.customers`,
the Iceberg Silver tables are not visible in the configured warehouse. Both
`quality` and `spark-iceberg` must use the same `ICEBERG_WAREHOUSE` and
`MINIO_S3A_ENDPOINT` (the Compose defaults already match).
For an uninitialized lakehouse, run the `retailpulse_iceberg_pipeline` Airflow DAG
after the streaming Bronze source files are available. Its bootstrap step
replaces Bronze Iceberg tables from the source files, so use it for initialization.
If Bronze Iceberg tables already exist and only Silver is missing, run:

```bash
docker exec retailpulse-spark-iceberg spark-submit /opt/retailpulse/lakehouse/iceberg/jobs/build_silver_current_state.py
docker exec retailpulse-spark-iceberg spark-submit /opt/retailpulse/lakehouse/iceberg/jobs/validate_silver.py
docker exec retailpulse-quality spark-submit /opt/retailpulse/quality/check_freshness.py
```

Raw Parquet files in MinIO do not by themselves create these Iceberg catalog
tables. The freshness check reports missing tables as failures; it does not
create them. Quality Python changes are bind-mounted, so no image rebuild is
needed for the improved diagnostics.
