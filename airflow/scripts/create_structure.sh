#!/usr/bin/env bash
set -euo pipefail

mkdir -p \
  airflow/dags/platform \
  airflow/dags/streaming \
  airflow/dags/lakehouse \
  airflow/dags/transformations \
  airflow/dags/quality \
  airflow/dags/ml \
  airflow/dags/genai \
  airflow/include/retailpulse \
  airflow/logs \
  airflow/plugins

echo "RetailPulse Airflow structure created."
