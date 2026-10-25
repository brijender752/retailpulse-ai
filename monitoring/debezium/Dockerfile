FROM quay.io/debezium/connect:3.3.2.Final

# Create the directory explicitly: COPY-created parent permissions can depend
# on the build context and may prevent the unprivileged kafka user traversing it.
USER root
RUN mkdir -p /opt/monitoring && chmod 0755 /opt/monitoring

# Vendor the agent so startup never depends on a runtime download.
COPY --chmod=0444 jmx_prometheus_javaagent-1.0.1.jar /opt/monitoring/jmx_prometheus_javaagent.jar
COPY --chmod=0444 debezium-jmx.yml /opt/monitoring/debezium-jmx.yml
USER kafka
RUN test -r /opt/monitoring/debezium-jmx.yml
RUN echo '7d61f737fd661610ccc14aea79764faa1ea94a340cbc8f0029b3d2edea3d80c1  /opt/monitoring/jmx_prometheus_javaagent.jar' | sha256sum -c -

EXPOSE 7071
