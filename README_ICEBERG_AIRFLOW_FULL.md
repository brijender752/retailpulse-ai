# RetailPulse Iceberg + Airflow

1. Copy these files into the RetailPulse project root.
2. Merge `spark-iceberg` into the existing main docker-compose.yml.
3. Add `spark-iceberg-ivy:` to the existing root volumes section.
4. Build and start:

   docker compose build spark-iceberg
   docker compose up -d spark-iceberg

5. Verify:

   docker exec retailpulse-spark-iceberg spark-submit --version

6. Create namespaces:

   docker exec retailpulse-spark-iceberg spark-submit /opt/retailpulse/lakehouse/iceberg/jobs/create_namespaces.py

7. Bootstrap MinIO -> Iceberg:

   docker exec retailpulse-spark-iceberg spark-submit /opt/retailpulse/lakehouse/iceberg/jobs/bootstrap_minio_to_iceberg.py

8. Validate:

   docker exec retailpulse-spark-iceberg spark-submit /opt/retailpulse/lakehouse/iceberg/jobs/validate_iceberg.py

9. Build Silver current-state tables:

   docker exec retailpulse-spark-iceberg spark-submit /opt/retailpulse/lakehouse/iceberg/jobs/build_silver_current_state.py

10. Validate Silver:

   docker exec retailpulse-spark-iceberg spark-submit /opt/retailpulse/lakehouse/iceberg/jobs/validate_silver.py

11. Restart Airflow services so the DAG is loaded.

12. Trigger DAG:

   airflow dags trigger retailpulse_iceberg_pipeline

The bootstrap is an initial load. Do not schedule it repeatedly yet.
The next phase will split bootstrap from recurring incremental MERGE orchestration.



# RetailPulse Incremental Iceberg Phase

Recurring flow:

Postgres -> Debezium -> Kafka -> Flink -> MinIO Bronze
                                      |
                                      v
                              Airflow every 5 min
                                      |
                                      v
                        Incremental CDC -> Iceberg Silver
                                      |
                                      v
                              Iceberg Gold marts
                                      |
                                      v
                                  Validation

Pause the previous one-time bootstrap DAG after successful initial load:
retailpulse_iceberg_pipeline

New recurring DAG:
retailpulse_iceberg_incremental

New daily maintenance DAG:
retailpulse_iceberg_maintenance

Manual tests:

docker exec retailpulse-spark-iceberg spark-submit \
  /opt/retailpulse/lakehouse/iceberg/jobs/incremental_cdc_to_silver.py

docker exec retailpulse-spark-iceberg spark-submit \
  /opt/retailpulse/lakehouse/iceberg/jobs/build_gold_marts.py

docker exec retailpulse-spark-iceberg spark-submit \
  /opt/retailpulse/lakehouse/iceberg/jobs/validate_incremental_pipeline.py

Restart Airflow:

cd airflow

docker compose --env-file .env -f docker-compose.airflow.yml \
  restart airflow-dag-processor airflow-scheduler airflow-apiserver

Check:

docker compose --env-file .env -f docker-compose.airflow.yml \
  exec airflow-scheduler airflow dags list-import-errors

Trigger:

docker compose --env-file .env -f docker-compose.airflow.yml \
  exec airflow-scheduler airflow dags trigger retailpulse_iceberg_incremental
