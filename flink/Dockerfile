FROM flink:2.1.0-java17

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-dev python-is-python3 && \
    rm -rf /var/lib/apt/lists/*

# Kafka connector
RUN wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/5.0.0-2.1/flink-connector-kafka-5.0.0-2.1.jar

RUN wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/4.2.0/kafka-clients-4.2.0.jar

    # FileSystem connector
RUN wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-files/2.1.0/flink-connector-files-2.1.0.jar

# Flink Parquet format
RUN wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/flink/flink-parquet/2.1.0/flink-parquet-2.1.0.jar

# `flink-parquet` is not a fat JAR. Use Parquet's bundle so the complete
# Parquet runtime, including ParquetWriter and its transitive dependencies,
# is loaded by Flink's JVM classpath.
RUN wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/parquet/parquet-hadoop-bundle/1.15.2/parquet-hadoop-bundle-1.15.2.jar && \
    python3 -c "import zipfile; assert 'org/apache/parquet/hadoop/ParquetWriter\$Builder.class' in zipfile.ZipFile('/opt/flink/lib/parquet-hadoop-bundle-1.15.2.jar').namelist()"

RUN wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.6/hadoop-common-3.3.6.jar

RUN wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar

RUN wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar

RUN wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-hdfs-client/3.3.6/hadoop-hdfs-client-3.3.6.jar

RUN wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/commons/commons-configuration2/2.8.0/commons-configuration2-2.8.0.jar

RUN wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-mapreduce-client-core/3.3.6/hadoop-mapreduce-client-core-3.3.6.jar

RUN wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/apache/hadoop/thirdparty/hadoop-shaded-guava/1.1.1/hadoop-shaded-guava-1.1.1.jar

RUN wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/com/fasterxml/woodstox/woodstox-core/6.4.0/woodstox-core-6.4.0.jar && \
    wget -P /opt/flink/lib \
    https://repo1.maven.org/maven2/org/codehaus/woodstox/stax2-api/4.2.1/stax2-api-4.2.1.jar

# Flink S3 filesystem plugin
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    wget -P /opt/flink/plugins/s3-fs-hadoop \
    https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/2.1.0/flink-s3-fs-hadoop-2.1.0.jar    

    # PostgreSQL JDBC driver
RUN wget -P /opt/flink/lib \
    https://jdbc.postgresql.org/download/postgresql-42.7.12.jar

# PyFlink
RUN pip3 install --no-cache-dir apache-flink==2.1.0

# MinIO Python client + PyArrow
RUN pip3 install --no-cache-dir \
    minio \
    pyarrow

RUN mkdir -p /tmp/pyflink && \
    chmod 1777 /tmp/pyflink

USER flink
