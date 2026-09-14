"""spark-submit entry point for the existing package-based batch modules."""
from pathlib import Path
import runpy
import sys

MODULES = {
    "bronze": "spark.jobs.batch.bronze.postgres_to_bronze",
    "silver": "spark.jobs.batch.silver.bronze_to_silver",
    "gold": "spark.jobs.batch.gold.silver_to_gold",
}


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in MODULES:
        raise SystemExit("Usage: spark-submit run_batch.py bronze|silver|gold")
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
    runpy.run_module(MODULES[sys.argv[1]], run_name="__main__")
