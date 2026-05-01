FROM apache/spark:3.5.3-java17-python3
USER root

# PostgreSQL JDBC-драйвер - кладём в стандартный classpath Spark.
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.10/postgresql-42.7.10.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/postgresql-42.7.10.jar

# ClickHouse-драйвер (clickhouse4j - HTTP-only, лёгкий, без Apache HttpClient).
# Кладём отдельно, так как подключаем через --jars при spark-submit.
RUN mkdir -p /opt/spark/extra-jars
ADD https://repo1.maven.org/maven2/cc/blynk/clickhouse/clickhouse4j/1.4.4/clickhouse4j-1.4.4.jar /opt/spark/extra-jars/
RUN chmod 644 /opt/spark/extra-jars/clickhouse4j-1.4.4.jar

RUN pip3 install --no-cache-dir pyspark==3.5.3
ENV PATH="/opt/spark/bin:${PATH}"

CMD ["tail", "-f", "/dev/null"]
