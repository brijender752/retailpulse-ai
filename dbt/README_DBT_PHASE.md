# RetailPulse dbt + Star Schema

1. Use the spark-thrift and dbt services in the root docker-compose.yml.
2. Build: docker compose build spark-thrift dbt
3. Start: docker compose up -d spark-thrift dbt
4. Check: docker logs --tail=100 retailpulse-spark-thrift
5. dbt version: docker exec retailpulse-dbt dbt --version
6. Debug: docker exec retailpulse-dbt bash -lc 'cd /opt/retailpulse/dbt && dbt debug'
7. Compile: docker exec retailpulse-dbt bash -lc 'cd /opt/retailpulse/dbt && dbt compile'
8. Build + tests: docker exec retailpulse-dbt bash -lc 'cd /opt/retailpulse/dbt && dbt build'
9. Docs: docker exec retailpulse-dbt bash -lc 'cd /opt/retailpulse/dbt && dbt docs generate'
10. Restart Airflow dag processor/scheduler/api server, then trigger retailpulse_dbt_analytics.

Models created under retailpulse.analytics: dim_customer, dim_product, dim_date, fact_orders, fact_payments, fact_customer_activity, mart_customer_value.

The Thrift startup command runs `init_dbt_namespaces.py` before starting the
server. It creates `retailpulse.default` (needed by PyHive when opening a
connection) and `retailpulse.analytics` with `IF NOT EXISTS`, preserving existing
tables. The dbt image includes Git for the `dbt debug` dependency check.
