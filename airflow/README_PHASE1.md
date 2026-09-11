# RetailPulse AI — Airflow Control Plane

**Maintainer:** `bj_kush`
**Scope:** local Docker development and orchestration

This phase adds Airflow as the control plane without replacing any working
Flink business logic.

RetailPulse AI is an end-to-end e-commerce data platform:

```text
PostgreSQL → Debezium CDC → Kafka → Flink streaming → MinIO Parquet
                                      ↓
                              Airflow control plane
```

Processing is deliberately separated: continuous jobs live under
`flink/jobs/streaming/{bronze,silver,gold}/`, while bounded Spark jobs live
under `spark/jobs/batch/{bronze,silver,gold}/`. Airflow orchestrates and
validates the platform; it does not contain the transformation logic.

## What it controls

1. PostgreSQL health.
2. Debezium connector health.
3. Kafka connectivity.
4. Flink JobManager/TaskManager health.
5. MinIO bucket health.
6. Starts an existing Flink job only when no healthy instance is running.
7. Restores Customer 360 from the latest MinIO savepoint when one exists.
8. Validates Customer 360 checkpoints.
9. Validates streaming data exists in MinIO.

## DAGs

`retailpulse_platform_health` checks PostgreSQL, Debezium, Kafka, Flink, and
MinIO independently. Run this DAG first.

`retailpulse_streaming_controller` starts the registered Flink Bronze, Silver,
and Gold jobs, checks the Customer 360 checkpoint, and validates MinIO output.
The registry is `include/retailpulse/jobs.py`; submitted jobs use the declared
Flink `pipeline.name` so Airflow can find them reliably.

## Important

The local Airflow container mounts `/var/run/docker.sock`.

This is intentionally a LOCAL DEVELOPMENT solution so Airflow can execute
the existing PyFlink scripts inside the already-running Flink JobManager.
Do not use this Docker-socket approach as the production cloud design.

When RetailPulse moves to Kubernetes, replace this part with the Flink
Kubernetes Operator / Kubernetes APIs.

## Existing Flink jobs remain the source of truth

Airflow only references paths under:

    /opt/flink/jobs/...

It does not copy or rewrite the Flink CDC/state/Gold business logic.

## Install

From project root:

    cp airflow/.env.example airflow/.env

Check the current Docker network:

    docker network ls

If needed, change `RETAILPULSE_NETWORK` in `airflow/.env`.

Then:

    cd airflow

    docker compose \
      --env-file .env \
      -f docker-compose.airflow.yml \
      build

    docker compose \
      --env-file .env \
      -f docker-compose.airflow.yml \
      up airflow-init

    docker compose \
      --env-file .env \
      -f docker-compose.airflow.yml \
      up -d

Airflow UI:

    http://localhost:8090

Default local credentials:

   {"airflow": "r7krUKnH36u6hK6R"}

For a clean rebuild after changing Airflow 3 settings, use:

    docker compose --env-file .env -f docker-compose.airflow.yml \
      up -d --build --force-recreate

## Validate Airflow

    docker compose \
      --env-file .env \
      -f docker-compose.airflow.yml \
      ps

Check DAG import errors:

    docker compose \
      --env-file .env \
      -f docker-compose.airflow.yml \
      exec airflow-scheduler \
      airflow dags list-import-errors

List DAGs:

    docker compose \
      --env-file .env \
      -f docker-compose.airflow.yml \
      exec airflow-scheduler \
      airflow dags list

Expected DAGs:

    retailpulse_platform_health
    retailpulse_streaming_controller

## Trigger manually

Run the health DAG first, then the streaming controller:

    docker compose --env-file .env -f docker-compose.airflow.yml \
      exec airflow-scheduler airflow dags trigger retailpulse_platform_health

    docker compose \
      --env-file .env \
      -f docker-compose.airflow.yml \
      exec airflow-scheduler \
      airflow dags trigger retailpulse_streaming_controller

## Verify the Docker socket works from Airflow

    docker compose \
      --env-file .env \
      -f docker-compose.airflow.yml \
      exec airflow-scheduler \
      python -c "import docker; print(docker.from_env().ping())"

Expected:

    True

## Current MinIO datasets expected

    s3://retailpulse/bronze/
    s3://retailpulse/silver_stream/
    s3://retailpulse/gold_stream/
    s3://retailpulse/gold_stream/customer_360_recovery/
    s3://retailpulse/flink-checkpoints/customer360-recovery/

## Configuration and troubleshooting

Airflow 3 task processes use the shared Execution API URL:

    http://airflow-apiserver:8080/execution/

The scheduler, API server, and task processes must share the JWT secret and
issuer in `docker-compose.airflow.yml`. Airflow-to-RetailPulse connections use
the Compose DNS names `postgres`, `debezium`, `kafka`, `flink-jobmanager`, and
`minio`, not `localhost`.

If a task stops after `Pre Execute`, recreate the Airflow services so these
settings are loaded. If Customer 360 reports `Flink job not found`, inspect
jobs with:

    docker exec retailpulse-flink-jobmanager flink list -r

Cancel only an obsolete autogenerated Customer 360 job (`insert-into_*`) and
trigger the controller again. The expected registered name is:

    RetailPulse - Customer 360 Stateful Recovery

The repeated `jdk.compiler specified to --add-exports` messages are harmless
Flink warnings. Scheduler/API diagnostics are available with:

    docker compose --env-file .env -f docker-compose.airflow.yml logs --tail 150 \
      airflow-scheduler airflow-apiserver

## Repository map

    airflow/dags/                 Airflow health and streaming DAGs
    airflow/include/retailpulse/  health, Flink control, and MinIO helpers
    flink/jobs/streaming/         continuous Bronze/Silver/Gold jobs
    spark/jobs/batch/             bounded Bronze/Silver/Gold jobs
    docs/architecture/            pipeline ownership and run commands

## Roadmap

1. Keep the local Airflow health and Flink control plane green.
2. Add Airflow tasks for Spark batch Silver and Gold processing.
3. Add Iceberg, data quality, dbt, warehouse analytics, dashboards, ML, and
   the GenAI analyst.
4. Replace Docker-socket submission with the Flink Kubernetes Operator for
   production deployment.

## Next phase

After this DAG is green:

    Existing MinIO Parquet
            ↓
    Apache Iceberg warehouse
            ↓
    Bronze / Silver / Gold Iceberg tables
            ↓
    dbt
            ↓
    Data quality
            ↓
    ML
            ↓
    GenAI

All of those will be attached to Airflow instead of being independent
manual scripts.

— `bj_kush`


to run it
cd C:\Users\dell\projects\retailpulse-ai\airflow

docker compose --env-file .env -f docker-compose.airflow.yml build

docker compose --env-file .env -f docker-compose.airflow.yml up airflow-init

docker compose --env-file .env -f docker-compose.airflow.yml up -d

docker compose \
  --env-file .env \
  -f docker-compose.airflow.yml \
  restart \
  airflow-dag-processor \
  airflow-scheduler \
  airflow-apiserver