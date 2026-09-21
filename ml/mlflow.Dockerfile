FROM python:3.12-slim

WORKDIR /opt/mlflow

RUN pip install --no-cache-dir \
    mlflow \
    boto3 \
    psycopg2-binary

EXPOSE 5000