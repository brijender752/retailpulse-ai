# RetailPulse AI: purpose and implemented flow

Repository review: 2026-09-11. This describes the checked-in code and configured
DAGs, not a verification that every service or scheduled job is currently healthy.

RetailPulse is a local e-commerce data engineering platform that converts
transactional changes into analytical datasets. It combines continuous Flink
processing, a periodically refreshed Iceberg lakehouse, and dbt business models.
It is built to answer questions about sales, payment discrepancies, product
performance, customer engagement, and customer value. The AI analyst, ML,
dashboards, and serving API remain future work: their directories contain no
implementation beyond an empty API package initializer.

The editable architecture diagram is [project-flow.mmd](project-flow.mmd).
The overview image is [retailpulse-project-overview-20260911.png](../images/retailpulse-project-overview-20260911.png).

## Source and ingestion

`data_generator/main.py` populates PostgreSQL with synthetic customers, products,
orders, order items, payments, inventory, website events, support tickets, and
marketing events. `sql/` defines the source schema. The generator is a bounded
loader; CDC subsequently captures database changes.

`ingestion/debezium/postgres-connector.json` configures an initial snapshot and
PostgreSQL logical change capture for those nine tables. Kafka receives topics
named `retailpulse.ecommerce.<table>`. Compose includes topic and connector
initializers, as well as Kafka UI for inspection.

## Three processing paths

1. **Continuous Flink outputs.** The six jobs registered in
   `airflow/include/retailpulse/jobs.py` cover Bronze, Silver, order summary,
   payment summary, product performance, and Customer 360 recovery. These jobs
   consume Kafka directly. Their Bronze/Silver/Gold names identify output layers;
   they do not form a filesystem-to-filesystem chain. They write Parquet to
   MinIO `bronze/`, `silver_stream/`, and `gold_stream/`. Customer 360 uses state,
   checkpoints, and savepoint restoration.
2. **Spark/Iceberg lakehouse.** Manual bootstrap copies Bronze Parquet into
   `retailpulse.bronze.*`, optionally imports streaming Gold, and merges the
   latest event per key into `retailpulse.silver.*`. The scheduled incremental
   job instead reads finalized Parquet directly from MinIO `bronze/`, filters
   with per-table watermarks and a five-minute lookback, deduplicates by key,
   and applies inserts, updates, and deletes to Iceberg Silver. It then rebuilds
   three Spark Gold tables and validates results. Iceberg files and metadata
   live in MinIO `warehouse/`; Iceberg is the table layer, not another storage
   server. Watermarks live in `retailpulse.control.cdc_watermarks`.
3. **Separate Spark batch implementation.** `spark/jobs/batch/` extracts
   PostgreSQL through JDBC and writes Parquet Bronze, Silver, and Gold. This
   path is manually runnable and is separate from the Iceberg jobs. Its Bronze
   overwrite uses the same MinIO prefix as Flink CDC, so the two ingestion
   paths should not be treated as safe concurrent writers to that prefix.

## dbt analytics and business meaning

dbt reads Iceberg Silver through Spark Thrift. Eight ephemeral staging models
normalize identifiers, amounts, timestamps, and marketing flags. Inventory is
declared as a source but has no dedicated staging or mart model. Seven Iceberg
table models provide:

| Model | Grain and purpose |
| --- | --- |
| `dim_customer` | One customer; identity, geography, and segment |
| `dim_product` | One product; category, price, cost, and margin |
| `dim_date` | One day within the observed order-date range |
| `fact_orders` | One order; item totals and payment reconciliation |
| `fact_payments` | One payment; amount, status, and order context |
| `fact_customer_activity` | One customer; web, support, and marketing aggregates |
| `mart_customer_value` | One customer; lifetime order value, payments, activity, and rule-based value segment |

The customer-value mart references `dim_customer` and all three facts. Product
and date dimensions support analysis and relationship checks; they are not
direct SQL inputs to that mart. The segments are rule-based, not ML predictions.
dbt tests check keys, relationships, accepted values, nonnegative amounts, and
CDC timestamp conversion. Tests validate models throughout `dbt build`; they
are not a downstream data-storage layer.

## Orchestration

| DAG | Configured schedule | Work |
| --- | --- | --- |
| `retailpulse_platform_health` | Every 5 minutes | Infrastructure checks |
| `retailpulse_streaming_controller` | Every 5 minutes | Ensure registered Flink jobs run; validate recovery/checkpoints and outputs |
| `retailpulse_iceberg_pipeline` | Manual | Validate sources, bootstrap, build and validate Silver |
| `retailpulse_iceberg_incremental` | Every 5 minutes | CDC merge, Spark Gold rebuild, validation |
| `retailpulse_iceberg_maintenance` | Daily at 02:00 UTC | Rewrite data files and manifests for listed Silver/Gold tables |
| `retailpulse_dbt_analytics` | Manual | dbt debug, build, docs generation |

Airflow executes commands inside existing Docker containers through its Docker
socket integration. Its arrows represent control, not data transfer. The dbt
DAG is not automatically triggered by completion of the incremental Iceberg DAG.
Configured schedules only run when the corresponding DAG is enabled.

## Boundaries to keep visible

- The README roadmap is behind the implementation: Iceberg and dbt exist already.
- Flink streaming Silver is event output; Iceberg Silver represents current state.
- Spark Gold and dbt marts are parallel analytical outputs from Iceberg Silver.
- The dbt profile schema and model custom schemas are both `analytics`. With
  dbt's default schema naming and no override macro in this repository, the
  expected output namespace is `analytics_analytics`; confirm in a generated
  manifest before documenting it as `analytics`.
- A five-minute incremental schedule does not guarantee five-minute end-to-end
  freshness: file finalization, processing duration, and manual dbt execution
  also determine when business models update.

Primary evidence: `docker-compose.yml`, `airflow/dags/`,
`airflow/include/retailpulse/jobs.py`, registered `flink/jobs/streaming/` scripts,
`lakehouse/iceberg/jobs/`, `dbt/models/`, and `data_generator/main.py`.
