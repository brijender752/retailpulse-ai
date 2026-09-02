# RetailPulse AI

An end-to-end real-time data engineering and GenAI platform for e-commerce analytics.

## Architecture

PostgreSQL
    ↓
Debezium CDC
    ↓
Kafka
    ↓
Flink / PySpark
    ↓
MinIO / Apache Iceberg
    ↓
dbt
    ↓
Analytical Warehouse
    ↓
Superset / Grafana
    ↓
GenAI AI Analyst

## Technologies

- Python
- PostgreSQL
- Apache Kafka
- Debezium
- Apache Flink
- Apache Spark / PySpark
- Apache Iceberg
- MinIO
- dbt
- Apache Airflow
- Apache Superset
- Grafana
- Prometheus
- Docker
- Machine Learning
- RAG
- LLM
- FastAPI

## Project Goals

1. Build a batch data pipeline.
2. Build a CDC pipeline.
3. Build real-time streaming pipelines.
4. Build a lakehouse.
5. Build an analytics warehouse.
6. Implement data quality.
7. Build ML pipelines.
8. Build a GenAI analytics assistant.
9. Implement monitoring.
10. Deploy the platform using containers.

## Current Architecture

PostgreSQL
    ↓
E-commerce source tables
    ↓
Python data generator
    ↓
Realistic transactional data

Current phase:
PostgreSQL + Data Generation