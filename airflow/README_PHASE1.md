# RetailPulse Airflow - Phase 1

This phase adds Airflow as the control plane without replacing any working
Flink business logic.

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
