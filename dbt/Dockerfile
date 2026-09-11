FROM python:3.13-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 DBT_PROFILES_DIR=/opt/retailpulse/dbt
RUN apt-get update && apt-get install -y --no-install-recommends build-essential gcc git libsasl2-dev libssl-dev libffi-dev && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir "dbt-core==1.12.2" "dbt-spark[PyHive]==1.11.0"
WORKDIR /opt/retailpulse/dbt
CMD ["bash","-lc","tail -f /dev/null"]
