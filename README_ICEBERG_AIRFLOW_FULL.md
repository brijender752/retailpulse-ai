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
