# =============================================================================
#  Image Spark de la plateforme — Spark 3.5.1 + Iceberg 1.5.2 + Nessie 0.77.1
# -----------------------------------------------------------------------------
#  Choix : embarquer les JARs dans l'image plutôt que d'utiliser
#  `--packages` au spark-submit. Raisons :
#    * pas de résolution Maven à chaque exécution (démarrage plus rapide,
#      pipeline déterministe et reproductible) ;
#    * fonctionne sans accès Internet une fois l'image construite ;
#    * versions figées => pas de dérive entre le master, les workers et le
#      driver lancé depuis Airflow.
#
#  Matrice de compatibilité retenue :
#    Spark 3.5.1 (Scala 2.12, Hadoop 3.3.4)
#      + iceberg-spark-runtime-3.5_2.12  1.5.2
#      + iceberg-aws-bundle              1.5.2   (S3FileIO, AWS SDK v2)
#      + nessie-spark-extensions-3.5     0.77.1  (SQL `USE REFERENCE`, branches)
#      + hadoop-aws / aws-java-sdk-bundle        (connecteur s3a:// zone brute)
# =============================================================================
FROM apache/spark:3.5.1-scala2.12-java17-python3-ubuntu

USER root

ARG ICEBERG_VERSION=1.5.2
ARG NESSIE_VERSION=0.77.1
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
ARG SCALA_VERSION=2.12
ARG SPARK_MAJOR=3.5

ENV SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:/opt/spark/sbin:$PATH \
    PYTHONPATH=/opt/spark-jobs:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl ca-certificates procps tini; \
    rm -rf /var/lib/apt/lists/*

# --- JARs du lakehouse -------------------------------------------------------
ARG MAVEN=https://repo1.maven.org/maven2
RUN set -eux; \
    cd "${SPARK_HOME}/jars"; \
    curl -fsSLO "${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR}_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar"; \
    curl -fsSLO "${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    curl -fsSLO "${MAVEN}/org/projectnessie/nessie-integrations/nessie-spark-extensions-${SPARK_MAJOR}_${SCALA_VERSION}/${NESSIE_VERSION}/nessie-spark-extensions-${SPARK_MAJOR}_${SCALA_VERSION}-${NESSIE_VERSION}.jar"; \
    curl -fsSLO "${MAVEN}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"; \
    curl -fsSLO "${MAVEN}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"; \
    ls -1 iceberg-* nessie-* hadoop-aws-* aws-java-sdk-*

# --- Dépendances Python des jobs --------------------------------------------
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

RUN mkdir -p /opt/spark-jobs /opt/spark/work-dir /opt/spark/logs \
 && chmod -R 777 /opt/spark/work-dir /opt/spark/logs

WORKDIR /opt/spark-jobs
USER spark

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/opt/spark/bin/spark-shell"]
