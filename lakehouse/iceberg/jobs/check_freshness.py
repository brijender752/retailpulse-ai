"""Read-only metadata/data checks; persist timestamps, not frozen ages."""
import json
import os
from pathlib import Path
import time
from iceberg_session import create_iceberg_spark_session

TABLES = {
    'silver.customers': ('_ingested_at', '_ts_ms'),
    'silver.orders': ('_ingested_at', '_ts_ms', 'order_date'),
    'gold.customer_360': ('_gold_processed_at',),
    'gold.order_summary': ('_gold_processed_at', 'order_date'),
    'gold.product_performance': ('_gold_processed_at',),
}


def main():
    spark = create_iceberg_spark_session('retailpulse-iceberg-freshness')
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    results = {}
    try:
        for table, candidates in TABLES.items():
            result = {'success': 0}
            try:
                name = 'retailpulse.' + table
                result['snapshot_timestamp'] = spark.sql(
                    f'SELECT unix_timestamp(MAX(committed_at)) FROM {name}.snapshots'
                ).first()[0]
                columns = spark.table(name).columns
                column = next((c for c in candidates if c in columns), None)
                if column:
                    expression = f'MAX(`{column}`) / 1000.0' if column == '_ts_ms' else f'unix_timestamp(MAX(CAST(`{column}` AS TIMESTAMP)))'
                    result['data_timestamp'] = spark.sql(f'SELECT {expression} FROM {name}').first()[0]
                    result['timestamp_column'] = column
                result['success'] = 1
            except Exception as exc:
                print(f'Freshness check failed for {table}: {exc}', flush=True)
            results[table] = result
        destination = Path('/opt/monitoring-state/iceberg.json')
        temporary = destination.with_suffix('.tmp')
        temporary.write_text(json.dumps({'checked_at': time.time(), 'tables': results}))
        os.replace(temporary, destination)
    finally:
        spark.stop()
    if not all(value['success'] for value in results.values()):
        raise RuntimeError('One or more Iceberg table checks failed')


if __name__ == '__main__':
    main()
