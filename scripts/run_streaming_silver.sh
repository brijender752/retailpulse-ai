#!/usr/bin/env bash

set -euo pipefail

# Run the DataStream implementation of Streaming Silver in the Flink cluster.
docker exec retailpulse-flink-jobmanager sh -lc \
  'flink run -py /opt/flink/jobs/streaming/silver/retailpulse_streaming_silver_datastream.py'
