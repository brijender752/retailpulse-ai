#!/usr/bin/env bash
# Run from Git Bash, WSL, or Linux. Stop immediately if any setup command fails.
set -euo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1

af_compose() { docker compose --profile airflow "$@"; }
af() { af_compose exec -T airflow-scheduler airflow "$@"; }
if af_compose ps --services --status running | grep -qx airflow-scheduler; then
    # Check before pausing: paused DAGs cannot schedule their remaining tasks.
    af_compose exec -T airflow-scheduler python - < scripts/check_streaming_idle.py
    for dag in retailpulse_streaming_master retailpulse_streaming_controller retailpulse_iceberg_pipeline retailpulse_iceberg_incremental retailpulse_iceberg_maintenance retailpulse_dbt_analytics; do
        af dags pause "$dag"
    done
    af_compose exec -T airflow-scheduler python - < scripts/check_streaming_idle.py
fi
docker compose --profile ingestion --profile etl --profile airflow --profile analytics up -d --build
af_compose up -d --wait --wait-timeout 180 airflow-apiserver airflow-scheduler airflow-dag-processor airflow-triggerer

echo "Waiting for Airflow to discover the end-to-end DAG..."
found=false
for attempt in {1..60}; do
    if af dags list --output json | grep -q 'retailpulse_streaming_end_to_end'; then
        found=true
        break
    fi
    sleep 5
done
if [ "$found" != true ]; then
    af dags list-import-errors
    exit 1
fi

# A single manual workflow owns startup and the Iceberg/dbt writes for this run.
for dag in retailpulse_streaming_master retailpulse_streaming_controller retailpulse_iceberg_pipeline retailpulse_iceberg_incremental retailpulse_iceberg_maintenance retailpulse_dbt_analytics; do
    af dags pause "$dag"
done
af_compose exec -T airflow-scheduler python /opt/retailpulse/scripts/check_streaming_idle.py
af dags unpause retailpulse_streaming_end_to_end
af dags trigger retailpulse_streaming_end_to_end
echo "Triggered. Follow retailpulse_streaming_end_to_end at http://localhost:8090"
