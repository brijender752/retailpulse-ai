# Pipeline layout

RetailPulse separates bounded batch workloads from unbounded streaming workloads.

## Batch processing

Spark jobs live in `spark/jobs/batch/` and are grouped by the lakehouse layer
they produce:

- `bronze/` — source extraction into Bronze.
- `silver/` — Bronze cleanup and schema normalization.
- `gold/` — analytical aggregates and business-ready datasets.

Run a batch module from the repository root, for example:

```powershell
& .\.venv\Scripts\python.exe -m spark.jobs.batch.silver.bronze_to_silver
```

## Stream processing

Flink jobs live in `flink/jobs/streaming/` and are grouped by their output
layer:

- `bronze/` — Kafka/Debezium ingestion.
- `silver/` — normalized streaming CDC records.
- `gold/` — stateful and aggregate real-time datasets.

Submit a streaming job through the Flink JobManager:

```bash
docker exec retailpulse-flink-jobmanager sh -lc \
  'flink run -py /opt/flink/jobs/streaming/gold/retailpulse_streaming_gold_customer360_stateful.py'
```

Historical job variants are available in Git history.
