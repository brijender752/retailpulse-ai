# RetailPulse AI

**Maintainer:** `bj_kush`

An end-to-end real-time data engineering and GenAI platform for e-commerce analytics.

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
