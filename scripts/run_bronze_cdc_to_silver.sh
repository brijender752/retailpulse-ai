#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

source .venv/Scripts/activate

PYTHON="$PROJECT_ROOT/.venv/Scripts/python.exe"
SPARK_SUBMIT="$PROJECT_ROOT/.venv/Scripts/spark-submit.cmd"
export SPARK_HOME="$(cygpath -w "$PROJECT_ROOT/.venv/Lib/site-packages/pyspark")"
export HADOOP_HOME="$(cygpath -w "$PROJECT_ROOT/tools/hadoop")"
export HADOOP_HOME_DIR="$HADOOP_HOME"
export SPARK_LOCAL_IP="127.0.0.1"
export PYSPARK_PYTHON="$(cygpath -w "$PYTHON")"
export MSYS_NO_PATHCONV=1

SPARK_SUBMIT_WIN="$(cygpath -w "$SPARK_SUBMIT")"
JOB_WIN="$(cygpath -w "$PROJECT_ROOT/spark/jobs/bronze_cdc_to_silver.py")"

cmd.exe /c call "$SPARK_SUBMIT_WIN" \
	--master 'local[*]' \
	--conf spark.driver.host=127.0.0.1 \
	--conf spark.driver.bindAddress=127.0.0.1 \
	--conf spark.ui.enabled=false \
	"$JOB_WIN"