FROM apache/spark:4.0.1

USER root

ENV SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3
ENV PATH="/opt/spark/bin:${PATH}"

# Load the Iceberg and S3 dependencies before the Python driver starts.
COPY spark/iceberg/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# Resolve the same pinned packages into a fresh build-only Ivy cache, then put
# them on Spark's startup classpath. S3 client cleanup can run after a Thrift
# session's dynamic classloader has closed, so --packages alone is insufficient.
RUN printf 'print("Spark dependencies resolved")\n' > /tmp/resolve-spark-dependencies.py \
    && /opt/spark/bin/spark-submit \
        --conf spark.jars.ivy=/tmp/iceberg-build-ivy \
        /tmp/resolve-spark-dependencies.py \
    && cp /tmp/iceberg-build-ivy/jars/*.jar /opt/spark/jars/ \
    && python3 -c 'from pathlib import Path; from zipfile import ZipFile; jars = list(Path("/opt/spark/jars").glob("software.amazon.awssdk_bundle-*.jar")); assert len(jars) == 1, jars; archive = ZipFile(jars[0]); archive.read("software/amazon/awssdk/thirdparty/org/apache/http/impl/conn/PoolingHttpClientConnectionManager$2.class")' \
    && rm -rf /tmp/iceberg-build-ivy /tmp/resolve-spark-dependencies.py

WORKDIR /opt/retailpulse
CMD ["bash", "-c", "tail -f /dev/null"]
