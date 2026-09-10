FROM apache/spark:4.0.1

USER root

ENV SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3
ENV PATH="/opt/spark/bin:${PATH}"

# Load the Iceberg and S3 dependencies before the Python driver starts.
COPY spark/iceberg/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

WORKDIR /opt/retailpulse
CMD ["bash", "-c", "tail -f /dev/null"]
