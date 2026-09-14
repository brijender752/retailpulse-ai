FROM apache/spark:4.0.1
USER root
ENV SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3
ENV PATH="/opt/spark/bin:${PATH}"

RUN pip3 install --no-cache-dir "pyspark==4.0.1" "great_expectations[spark]==1.22.0" minio==7.2.18 requests==2.32.5

# Match the shared Iceberg session's Spark/Scala/Hadoop versions and resolve
# dependencies before spark-submit starts the Python driver.
COPY spark/iceberg/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
RUN printf 'print("Spark dependencies resolved")\n' > /tmp/resolve-spark-dependencies.py \
    && /opt/spark/bin/spark-submit \
        --conf spark.jars.ivy=/tmp/quality-build-ivy \
        /tmp/resolve-spark-dependencies.py \
    && cp /tmp/quality-build-ivy/jars/*.jar /opt/spark/jars/ \
    && rm -rf /tmp/quality-build-ivy /tmp/resolve-spark-dependencies.py

# The image also supports running directly as its default non-root user.
RUN sed -i 's|spark.jars.ivy /root/.ivy2|spark.jars.ivy /home/spark/.ivy2|' /opt/spark/conf/spark-defaults.conf
RUN mkdir -p /opt/retailpulse/quality/results /home/spark/.ivy2 && chown -R spark:spark /opt/retailpulse /home/spark/.ivy2
USER spark
WORKDIR /opt/retailpulse
CMD ["bash","-lc","tail -f /dev/null"]
