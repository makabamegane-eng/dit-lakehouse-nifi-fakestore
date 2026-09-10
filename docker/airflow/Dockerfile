# =============================================================================
#  Image Airflow de la plateforme — Airflow 2.9.3 + JRE 17 + Spark 3.5.1
# -----------------------------------------------------------------------------
#  Pourquoi embarquer Spark dans l'image Airflow ?
#  Le DAG soumet les jobs avec SparkSubmitOperator, qui exécute le binaire
#  `spark-submit` *localement* puis pilote un driver en mode client connecté à
#  spark://spark-master:7077. Il faut donc, côté Airflow :
#     * un JRE compatible (Java 17, identique à l'image Spark) ;
#     * la distribution Spark 3.5.1 (même version que le cluster : le protocole
#       RPC master/worker n'est pas garanti compatible entre versions mineures) ;
#     * les mêmes JARs Iceberg / Nessie / S3A que les workers.
#
#  Alternative écartée : DockerOperator / `docker exec` depuis Airflow, qui
#  impose de monter le socket Docker dans le conteneur (surface de sécurité
#  inutile) et rend les logs Spark plus difficiles à rapatrier dans Airflow.
# =============================================================================
FROM apache/airflow:2.9.3-python3.11

USER root

ARG SPARK_VERSION=3.5.1
ARG HADOOP_PROFILE=3
ARG ICEBERG_VERSION=1.5.2
ARG NESSIE_VERSION=0.77.1
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:/opt/spark/sbin:/usr/lib/jvm/java-17-openjdk-amd64/bin:$PATH

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless curl ca-certificates procps netcat-openbsd; \
    rm -rf /var/lib/apt/lists/*

# --- Distribution Spark ------------------------------------------------------
RUN set -eux; \
    curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_PROFILE}.tgz" \
      -o /tmp/spark.tgz; \
    mkdir -p /opt/spark; \
    tar -xzf /tmp/spark.tgz -C /opt/spark --strip-components=1; \
    rm /tmp/spark.tgz

# --- Mêmes JARs que le cluster (cohérence driver / executors) ---------------
ARG MAVEN=https://repo1.maven.org/maven2
RUN set -eux; \
    cd "${SPARK_HOME}/jars"; \
    curl -fsSLO "${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"; \
    curl -fsSLO "${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    curl -fsSLO "${MAVEN}/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/${NESSIE_VERSION}/nessie-spark-extensions-3.5_2.12-${NESSIE_VERSION}.jar"; \
    curl -fsSLO "${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"; \
    curl -fsSLO "${MAVEN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

COPY scripts/init-airflow.sh /opt/airflow/scripts/init-airflow.sh
RUN chmod +x /opt/airflow/scripts/init-airflow.sh \
 && mkdir -p /opt/spark-jobs \
 && chown -R airflow:root /opt/airflow /opt/spark-jobs

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
