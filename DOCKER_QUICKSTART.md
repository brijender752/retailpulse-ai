# Run the complete stack from the repository root

The root `docker-compose.yml` includes the data platform, Airflow, and Superset.
The root `.env` contains the merged local settings. Edit that file for future
configuration changes; the old environment files in subdirectories are unused.

## One-time transition for an existing Airflow installation

Stop the old Airflow Compose project before starting the combined stack:

```bash
docker compose -p airflow -f docker-compose.airflow.yml down
```

This retains the database volume. The combined stack reuses
`airflow_airflow-db-data`, preserving Airflow metadata and run history.
The root `docker-compose.airflow.yml` is retained for this transition and
optional standalone Airflow use; do not run both setups at the same time.

## Start everything

```bash
docker compose up -d --build
```

Airflow services wait for database migration to complete. Superset waits for
its initialization service. The shared network is created by the root stack.

- Airflow: http://localhost:8090
- Superset: http://localhost:8088

## Manage the stack

```bash
docker compose ps
docker compose logs --tail 100 airflow-scheduler superset
docker compose down
```

The merged environment file remains excluded from Git. No separate `--env-file`
or component build commands are needed for the combined stack.
