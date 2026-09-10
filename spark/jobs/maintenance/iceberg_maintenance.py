#!/usr/bin/env python3
"""MAINTENANCE — entretien des tables Iceberg du lakehouse.

Une table Iceberg qui n'est jamais entretenue se dégrade de deux façons :
  * multiplication de petits fichiers Parquet (un par écriture, par partition),
    qui fait exploser le temps de planification des requêtes Dremio ;
  * accumulation de snapshots et de fichiers de métadonnées orphelins, qui
    occupent MinIO sans jamais être lus.

Trois procédures Iceberg sont enchaînées :
  1. `rewrite_data_files`     — compaction (bin-pack) vers des fichiers cibles
                                de 64 Mo. Sur un volume de démonstration, cela
                                ramène des centaines de fichiers à quelques-uns.
  2. `rewrite_manifests`      — regroupe les manifestes pour accélérer
                                l'élagage de partitions.
  3. `expire_snapshots`       — purge les snapshots antérieurs à la rétention.

ATTENTION — rétention et time travel : `expire_snapshots` supprime la
possibilité de remonter avant la date de rétention. La valeur par défaut est
volontairement de 7 jours et ce job N'EST PAS branché en automatique dans le
DAG quotidien : il se déclenche à la demande, pour ne pas détruire l'historique
de snapshots qui sert justement à démontrer le time travel.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession  # noqa: E402

from common.config import (  # noqa: E402
    BRONZE_TABLES,
    GOLD_TABLES,
    SETTINGS,
    SILVER_TABLES,
)
from common.session import build_spark, get_logger, table_exists  # noqa: E402

LOG = get_logger("maintenance.iceberg")


def all_tables() -> list[str]:
    tables = [SETTINGS.bronze(t) for t in BRONZE_TABLES.values()]
    tables += [SETTINGS.silver(t) for t in SILVER_TABLES.values()]
    tables += [SETTINGS.gold(t) for t in GOLD_TABLES.values()]
    return tables


def compact(spark: SparkSession, table: str, target_mb: int) -> None:
    LOG.info("Compaction de %s (cible %d Mo/fichier)", table, target_mb)
    result = spark.sql(
        f"""
        CALL {SETTINGS.catalog}.system.rewrite_data_files(
            table => '{table.split('.', 1)[1]}',
            strategy => 'binpack',
            options => map(
                'target-file-size-bytes', '{target_mb * 1024 * 1024}',
                'min-input-files', '3'
            )
        )
        """
    ).collect()
    if result:
        LOG.info(
            "  %s fichier(s) réécrit(s) → %s fichier(s) produit(s)",
            result[0][0],
            result[0][1],
        )


def rewrite_manifests(spark: SparkSession, table: str) -> None:
    spark.sql(
        f"CALL {SETTINGS.catalog}.system.rewrite_manifests("
        f"table => '{table.split('.', 1)[1]}')"
    ).collect()
    LOG.info("Manifestes réécrits pour %s", table)


def expire(spark: SparkSession, table: str, retain_days: int) -> None:
    """Purge les snapshots plus vieux que la rétention, en gardant les 5 derniers."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=retain_days)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    LOG.info("Expiration des snapshots de %s antérieurs au %s", table, cutoff)
    spark.sql(
        f"""
        CALL {SETTINGS.catalog}.system.expire_snapshots(
            table => '{table.split('.', 1)[1]}',
            older_than => TIMESTAMP '{cutoff}',
            retain_last => 5
        )
        """
    ).collect()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Maintenance des tables Iceberg")
    p.add_argument("--tables", default="", help="Tables ciblées (défaut : toutes)")
    p.add_argument("--target-file-size-mb", type=int, default=64)
    p.add_argument("--retain-days", type=int, default=7)
    p.add_argument(
        "--expire-snapshots",
        action="store_true",
        help="Purge les anciens snapshots — DÉTRUIT la capacité de time travel "
        "au-delà de la rétention. Désactivé par défaut.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    spark = build_spark("iceberg-maintenance")

    targets = [t.strip() for t in args.tables.split(",") if t.strip()] or all_tables()
    LOG.info("=== MAINTENANCE ICEBERG | %d table(s) ===", len(targets))

    for table in targets:
        if not table_exists(spark, table):
            LOG.warning("Table absente, ignorée : %s", table)
            continue
        try:
            compact(spark, table, args.target_file_size_mb)
            rewrite_manifests(spark, table)
            if args.expire_snapshots:
                expire(spark, table, args.retain_days)
        except Exception as exc:  # noqa: BLE001
            # La maintenance ne doit jamais faire échouer le pipeline de données.
            LOG.error("Maintenance échouée sur %s : %s", table, exc)

    LOG.info("=== MAINTENANCE terminée ===")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
