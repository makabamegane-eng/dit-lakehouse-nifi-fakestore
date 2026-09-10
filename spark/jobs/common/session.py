"""Construction de la SparkSession connectée au catalogue Nessie et à MinIO.

La quasi-totalité de la configuration Iceberg/Nessie/S3A vit dans
`spark/conf/spark-defaults.conf` (montée dans les conteneurs Spark ET présente
dans l'image Airflow). Ce module ne fait que :
  * la re-poser explicitement, pour que les jobs restent exécutables même
    lancés hors de la plateforme (ex. `spark-submit` local pendant le dev) ;
  * garantir la présence des namespaces du médaillon ;
  * fournir un logger homogène.
"""

from __future__ import annotations

import logging
import sys

from pyspark.sql import SparkSession

from common.config import BRONZE_NS, GOLD_NS, SETTINGS, SILVER_NS


def get_logger(name: str) -> logging.Logger:
    """Logger unifié : même format que les logs Spark, lisible dans Airflow."""
    logger = logging.getLogger(f"lakehouse.{name}")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)-7s | %(name)s | %(message)s")
        )
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger


def build_spark(app_name: str) -> SparkSession:
    """Retourne une SparkSession prête à écrire des tables Iceberg via Nessie."""
    s = SETTINGS
    catalog = s.catalog

    builder = (
        SparkSession.builder.appName(f"lakehouse::{app_name}")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
        )
        # --- catalogue Iceberg versionné par Nessie ------------------------- #
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config(f"spark.sql.catalog.{catalog}.uri", s.nessie_uri)
        .config(f"spark.sql.catalog.{catalog}.ref", s.nessie_ref)
        .config(f"spark.sql.catalog.{catalog}.authentication.type", "NONE")
        .config(f"spark.sql.catalog.{catalog}.warehouse", f"s3a://{s.warehouse_bucket}/warehouse")
        .config(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{catalog}.s3.endpoint", s.minio_endpoint)
        .config(f"spark.sql.catalog.{catalog}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{catalog}.client.region", s.region)
        .config("spark.sql.defaultCatalog", catalog)
        # --- s3a:// pour la lecture des JSON de la zone brute ---------------- #
        .config("spark.hadoop.fs.s3a.endpoint", s.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", s.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s.secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # --- exécution ------------------------------------------------------ #
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Les namespaces sont créés à la volée : la plateforme se reconstruit
    # intégralement à partir d'un catalogue vide, sans étape manuelle.
    for namespace in (BRONZE_NS, SILVER_NS, GOLD_NS):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")

    return spark


def table_exists(spark: SparkSession, fq_table: str) -> bool:
    """Existence d'une table Iceberg, sans lever d'exception."""
    try:
        spark.sql(f"DESCRIBE TABLE {fq_table}").collect()
        return True
    except Exception:  # noqa: BLE001 — l'API Nessie remonte des erreurs variées
        return False


def log_table_stats(spark: SparkSession, fq_table: str, logger: logging.Logger) -> None:
    """Trace le volume et le snapshot Iceberg courant — utile en démo vidéo."""
    count = spark.table(fq_table).count()
    snapshot_id = "n/a"
    try:
        snapshots = spark.sql(
            f"SELECT snapshot_id, committed_at FROM {fq_table}.snapshots "
            "ORDER BY committed_at DESC LIMIT 1"
        ).collect()
        if snapshots:
            snapshot_id = f"{snapshots[0][0]} @ {snapshots[0][1]}"
    except Exception:  # noqa: BLE001
        pass
    logger.info("[%s] %s lignes | snapshot courant : %s", fq_table, f"{count:,}", snapshot_id)
