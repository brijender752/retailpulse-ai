FROM python:3.12-slim

WORKDIR /opt/retailpulse

COPY ml/requirements.txt /tmp/requirements.txt

RUN pip install \
    --no-cache-dir \
    -r /tmp/requirements.txt

COPY ml /opt/retailpulse/ml

ENV PYTHONPATH=/opt/retailpulse

CMD ["python", "--version"]