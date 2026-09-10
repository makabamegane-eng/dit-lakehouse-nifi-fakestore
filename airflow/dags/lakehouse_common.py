"""Socle partagé par les DAGs de la plateforme.

Regroupe ce qui serait sinon recopié dans chaque DAG : configuration Spark,
fabrique de tâches `spark-submit`, pilotage de NiFi et inspection de la zone
brute MinIO. Les DAGs eux-mêmes ne décrivent plus que l'enchaînement, ce qui
les rend lisibles d'un coup d'œil dans l'interface Airflow.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Iterable, Sequence

import boto3
import requests
from botocore.client import Config

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

LOG = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
#  Contexte plateforme (injecté par le docker-compose)
# --------------------------------------------------------------------------- #
SPARK_JOBS_DIR = "/opt/spark-jobs"
SPARK_CONN_ID = "spark_default"

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
RAW_BUCKET = os.environ.get("RAW_BUCKET", "lakehouse-raw")
ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "lakehouse")
SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "lakehouse123")
REGION = os.environ.get("AWS_REGION", "us-east-1")

NIFI_BASE_URL = os.environ.get("NIFI_BASE_URL", "http://nifi:8080")
NIFI_INGEST_URL = os.environ.get("NIFI_INGEST_URL", "http://nifi:9095/ingest")

DOMAINS: tuple[str, ...] = ("products", "users", "carts")
HISTORY_MONTHS = int(os.environ.get("HISTORY_MONTHS", "6"))
HISTORY_DAYS = HISTORY_MONTHS * 30

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=45),
}

# Configuration Spark commune à toutes les soumissions.
# Elle double volontairement `spark-defaults.conf` : le driver tourne dans le
# conteneur Airflow, et l'on veut que la configuration du pipeline soit lisible
# dans les logs Airflow sans avoir à ouvrir un fichier du conteneur Spark.
SPARK_CONF = {
    "spark.sql.extensions": (
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
        "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
    ),
    "spark.sql.catalog.nessie": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.nessie.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
    "spark.sql.catalog.nessie.uri": os.environ.get(
        "NESSIE_URI", "http://nessie:19120/api/v2"
    ),
    "spark.sql.catalog.nessie.ref": os.environ.get("NESSIE_REF", "main"),
    "spark.sql.catalog.nessie.authentication.type": "NONE",
    "spark.sql.catalog.nessie.warehouse": (
        f"s3a://{os.environ.get('WAREHOUSE_BUCKET', 'lakehouse-warehouse')}/warehouse"
    ),
    "spark.sql.catalog.nessie.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.nessie.s3.endpoint": MINIO_ENDPOINT,
    "spark.sql.catalog.nessie.s3.path-style-access": "true",
    "spark.sql.catalog.nessie.client.region": REGION,
    "spark.sql.defaultCatalog": "nessie",
    "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
    "spark.hadoop.fs.s3a.access.key": ACCESS_KEY,
    "spark.hadoop.fs.s3a.secret.key": SECRET_KEY,
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    "spark.sql.session.timeZone": "UTC",
    # Le paquet `common` est monté au même chemin dans Airflow et dans les
    # workers Spark : les executors le trouvent via PYTHONPATH.
    "spark.executorEnv.PYTHONPATH": SPARK_JOBS_DIR,
    "spark.driver.extraJavaOptions": "-Duser.timezone=UTC",
    "spark.executor.extraJavaOptions": "-Duser.timezone=UTC",
}

SPARK_ENV = {
    "AWS_ACCESS_KEY_ID": ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": SECRET_KEY,
    "AWS_REGION": REGION,
    "MINIO_ENDPOINT": MINIO_ENDPOINT,
    "RAW_BUCKET": RAW_BUCKET,
    "WAREHOUSE_BUCKET": os.environ.get("WAREHOUSE_BUCKET", "lakehouse-warehouse"),
    "NESSIE_URI": os.environ.get("NESSIE_URI", "http://nessie:19120/api/v2"),
    "NESSIE_REF": os.environ.get("NESSIE_REF", "main"),
    "PYTHONPATH": SPARK_JOBS_DIR,
    "SYNTHETIC_HISTORY_ENABLED": os.environ.get("SYNTHETIC_HISTORY_ENABLED", "true"),
    "SYNTHETIC_HISTORY_SEED": os.environ.get("SYNTHETIC_HISTORY_SEED", "20260909"),
    "HISTORY_MONTHS": str(HISTORY_MONTHS),
}


# --------------------------------------------------------------------------- #
#  Fabrique de tâches Spark
# --------------------------------------------------------------------------- #
def spark_task(
    task_id: str,
    script: str,
    application_args: Sequence[str] | None = None,
    executor_memory: str | None = None,
    **kwargs,
) -> SparkSubmitOperator:
    """Crée une tâche `spark-submit` en mode client vers le cluster standalone."""
    return SparkSubmitOperator(
        task_id=task_id,
        application=f"{SPARK_JOBS_DIR}/{script}",
        conn_id=SPARK_CONN_ID,
        name=f"lakehouse-{task_id}",
        conf=SPARK_CONF,
        env_vars=SPARK_ENV,
        application_args=list(application_args or []),
        deploy_mode="client",
        driver_memory=os.environ.get("SPARK_DRIVER_MEMORY", "1g"),
        executor_memory=executor_memory or os.environ.get("SPARK_EXECUTOR_MEMORY", "2g"),
        total_executor_cores=2,
        verbose=False,
        **kwargs,
    )


# --------------------------------------------------------------------------- #
#  Zone brute MinIO
# --------------------------------------------------------------------------- #
def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def count_raw_objects(domain: str, snapshot_date: str) -> int:
    """Nombre d'objets déposés par NiFi dans une partition donnée."""
    client = s3_client()
    prefix = f"fakestore/{domain}/ingest_date={snapshot_date}/"
    total = 0
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=RAW_BUCKET, Prefix=prefix):
        total += sum(
            1
            for obj in page.get("Contents", [])
            if obj["Key"].endswith(".json")
        )
    return total


def list_raw_dates(domain: str) -> list[str]:
    """Dates de snapshot présentes dans la zone brute pour un domaine."""
    client = s3_client()
    prefix = f"fakestore/{domain}/"
    dates: set[str] = set()
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=RAW_BUCKET, Prefix=prefix, Delimiter="/"):
        for common in page.get("CommonPrefixes", []):
            leaf = common["Prefix"][len(prefix):].strip("/")
            if leaf.startswith("ingest_date="):
                dates.add(leaf.split("=", 1)[1])
    return sorted(dates)


# --------------------------------------------------------------------------- #
#  Pilotage de NiFi
# --------------------------------------------------------------------------- #
def trigger_nifi_ingestion(
    snapshot_date: str,
    domains: Iterable[str] = DOMAINS,
    run_id: str | None = None,
    timeout: int = 30,
) -> dict:
    """Déclenche le flow NiFi pour une date logique donnée.

    Le point d'entrée est un processeur ListenHTTP exposé par NiFi. Ce choix,
    par rapport à un pilotage via l'API REST de NiFi (`run-once` sur un
    processeur), a trois avantages :
      * Airflow n'a pas besoin de connaître les identifiants internes des
        composants NiFi, qui changent à chaque reconstruction du flow ;
      * le contrat d'interface est un simple JSON, versionnable et testable
        avec `curl` — ce qui est démontrable en direct ;
      * NiFi reste maître de sa propre exécution : Airflow envoie une demande,
        il ne manipule pas l'état interne de l'outil d'ingestion.
    """
    payload = {
        "snapshot_date": snapshot_date,
        "domains": list(domains),
        "run_id": run_id or f"airflow-{snapshot_date}",
    }
    response = requests.post(
        NIFI_INGEST_URL,
        data=json.dumps(payload),
        headers={"Content-Type": "application/json"},
        timeout=timeout,
    )
    response.raise_for_status()
    LOG.info("NiFi déclenché pour %s (domaines : %s)", snapshot_date, ", ".join(domains))
    return payload


def wait_for_ingestion(
    snapshot_date: str,
    domains: Iterable[str] = DOMAINS,
    timeout: int = 300,
    poll_interval: int = 5,
) -> dict[str, int]:
    """Attend que NiFi ait déposé au moins un objet par domaine.

    C'est la brique « Option A » du sujet : Airflow ne suppose pas que NiFi a
    terminé, il OBSERVE la zone brute jusqu'à constater le dépôt. Le pipeline
    reste donc correct même si NiFi est lent, saturé, ou rejoue un lot.
    """
    domains = list(domains)
    deadline = time.time() + timeout
    counts: dict[str, int] = {d: 0 for d in domains}

    while time.time() < deadline:
        counts = {d: count_raw_objects(d, snapshot_date) for d in domains}
        if all(v > 0 for v in counts.values()):
            LOG.info("Ingestion constatée pour %s : %s", snapshot_date, counts)
            return counts
        LOG.info("En attente du dépôt NiFi pour %s : %s", snapshot_date, counts)
        time.sleep(poll_interval)

    manquants = [d for d, v in counts.items() if v == 0]
    raise TimeoutError(
        f"Aucun objet déposé par NiFi pour {snapshot_date} sur : {', '.join(manquants)} "
        f"(délai de {timeout}s dépassé). Vérifier que le flow NiFi est démarré."
    )


def nifi_health() -> dict:
    """Contrôle de disponibilité de NiFi avant tout déclenchement."""
    response = requests.get(f"{NIFI_BASE_URL}/nifi-api/system-diagnostics", timeout=15)
    response.raise_for_status()
    stats = response.json()["systemDiagnostics"]["aggregateSnapshot"]
    LOG.info(
        "NiFi opérationnel — heap utilisée %s / %s, %s threads actifs",
        stats.get("usedHeap"),
        stats.get("maxHeap"),
        stats.get("totalThreads"),
    )
    return stats


# --------------------------------------------------------------------------- #
#  Utilitaires de dates
# --------------------------------------------------------------------------- #
def history_dates(end_date: str | None = None, days: int = HISTORY_DAYS) -> list[str]:
    """Fenêtre d'historisation, du plus ancien au plus récent."""
    end = (
        datetime.strptime(end_date, "%Y-%m-%d").date()
        if end_date and end_date != "today"
        else datetime.utcnow().date()
    )
    start = end - timedelta(days=days - 1)
    return [(start + timedelta(days=i)).isoformat() for i in range(days)]
