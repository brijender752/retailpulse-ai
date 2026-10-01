# RetailPulse Architecture

RetailPulse is an end-to-end data engineering, machine
learning and GenAI platform.

## Source

PostgreSQL stores transactional ecommerce data.

## CDC

Debezium captures database changes from PostgreSQL.

## Event Streaming

CDC events are published to Kafka.

## Stream Processing

PyFlink consumes Kafka CDC events and processes streaming
data.

## Object Storage

MinIO provides S3-compatible object storage.

## Lakehouse

Apache Iceberg provides analytical table management on top
of object storage.

## Analytics

Spark and dbt transform data into analytical models.

## Orchestration

Apache Airflow coordinates batch pipelines and ML
workflows.

## Machine Learning

RetailPulse contains churn prediction and product
recommendation pipelines.

MLflow provides experiment and model tracking.

## API Serving

FastAPI exposes customer intelligence and machine-learning
results.

## GenAI

Ollama runs local language models.

The GenAI assistant combines structured RetailPulse data
with retrieved knowledge.