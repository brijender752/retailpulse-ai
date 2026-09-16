FROM apache/superset:6.0.0

USER root

# Install in Superset's runtime environment and use the pure Python SASL client.
RUN uv pip install --python /app/.venv/bin/python --no-cache \
    "pyhive[hive_pure_sasl]==0.7.0" \
    psycopg2-binary \
    gevent

USER superset
