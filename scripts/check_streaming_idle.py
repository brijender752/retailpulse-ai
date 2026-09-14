"""Prevent startup from overlapping pre-existing pipeline writers."""
import json
import subprocess


def main():
    owners = {
        "retailpulse_streaming_master", "retailpulse_streaming_controller",
        "retailpulse_iceberg_pipeline", "retailpulse_iceberg_incremental",
        "retailpulse_iceberg_maintenance", "retailpulse_dbt_analytics",
        "retailpulse_streaming_end_to_end",
    }
    listed = subprocess.run(["airflow", "dags", "list", "--output", "json"],
                            check=True, capture_output=True, text=True)
    installed = {row["dag_id"] for row in json.loads(listed.stdout)}
    for owner in sorted(owners & installed):
        for state in ("running", "queued"):
            result = subprocess.run(
                ["airflow", "dags", "list-runs", owner, "--state", state, "--output", "json"],
                check=True, capture_output=True, text=True,
            )
            # Airflow prints a plain message rather than JSON when there are no runs.
            output = result.stdout.strip()
            active = [] if output == "No data found" else json.loads(output)
            if active:
                raise SystemExit(f"Existing {state} pipeline runs must finish or be resolved in Airflow before startup (unpause a paused active DAG so it can finish): {active}")


if __name__ == "__main__":
    main()
