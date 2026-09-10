"""Découverte de la zone brute MinIO alimentée par NiFi.

Le pipeline doit être **incrémental et idempotent** : rejouer un DAG run ne
doit ni dupliquer les données ni retraiter six mois d'historique. La stratégie
retenue est volontairement simple et lisible :

  1. lister, via l'API S3, les partitions `ingest_date=YYYY-MM-DD/` réellement
     présentes dans la zone brute pour un domaine ;
  2. lister les `_ingest_date` déjà présentes dans la table Bronze ;
  3. ne traiter que la différence (mode incrémental).

On préfère cette comparaison d'ensembles à un simple « watermark max date » :
elle rattrape correctement une partition arrivée en retard (backfill partiel,
rejeu NiFi d'une journée manquée), ce qu'un watermark monotone raterait.

L'écriture Bronze se fait ensuite en `overwritePartitions()` : même si une date
est retraitée, le résultat est le même — d'où l'idempotence.
"""

from __future__ import annotations

import logging
from typing import Iterable, List, Sequence, Set

import boto3
from botocore.client import Config
from pyspark.sql import SparkSession

from common.config import SETTINGS
from common.session import table_exists

_PARTITION_PREFIX = "ingest_date="


def s3_client():
    """Client S3 pointant sur MinIO (path-style obligatoire)."""
    s = SETTINGS
    return boto3.client(
        "s3",
        endpoint_url=s.minio_endpoint,
        aws_access_key_id=s.access_key,
        aws_secret_access_key=s.secret_key,
        region_name=s.region,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def list_available_dates(domain: str) -> List[str]:
    """Dates de snapshot présentes dans la zone brute pour un domaine.

    Utilise un listing par délimiteur (`CommonPrefixes`) : on ne rapatrie que
    les noms de « répertoires », pas la liste complète des objets — coût
    constant même avec 180 partitions.
    """
    client = s3_client()
    paginator = client.get_paginator("list_objects_v2")
    prefix = SETTINGS.raw_prefix(domain)
    dates: Set[str] = set()

    for page in paginator.paginate(
        Bucket=SETTINGS.raw_bucket, Prefix=prefix, Delimiter="/"
    ):
        for common in page.get("CommonPrefixes", []):
            leaf = common["Prefix"][len(prefix):].strip("/")
            if leaf.startswith(_PARTITION_PREFIX):
                dates.add(leaf[len(_PARTITION_PREFIX):])

    return sorted(dates)


def list_processed_dates(spark: SparkSession, bronze_table: str) -> List[str]:
    """Dates déjà matérialisées dans la table Bronze."""
    if not table_exists(spark, bronze_table):
        return []
    rows = spark.sql(
        f"SELECT DISTINCT _ingest_date FROM {bronze_table} ORDER BY _ingest_date"
    ).collect()
    return [str(r[0]) for r in rows if r[0] is not None]


def resolve_dates(
    spark: SparkSession,
    domain: str,
    bronze_table: str,
    mode: str,
    explicit_dates: Sequence[str] | None,
    logger: logging.Logger,
    limit: int | None = None,
) -> List[str]:
    """Détermine les partitions à traiter pour ce run.

    mode = ``explicit``     : uniquement les dates passées en argument
    mode = ``incremental``  : zone brute moins ce qui est déjà en Bronze
    mode = ``full``         : toutes les dates de la zone brute (rejeu total)
    """
    available = list_available_dates(domain)
    if not available:
        logger.warning("[%s] aucune partition dans la zone brute", domain)
        return []

    if mode == "explicit":
        wanted = [d for d in (explicit_dates or []) if d in available]
        missing = sorted(set(explicit_dates or []) - set(available))
        if missing:
            logger.warning("[%s] dates absentes de la zone brute : %s", domain, missing)
        selected = wanted
    elif mode == "full":
        selected = available
    else:  # incremental
        done = set(list_processed_dates(spark, bronze_table))
        selected = [d for d in available if d not in done]

    if limit is not None and limit > 0:
        selected = selected[:limit]

    logger.info(
        "[%s] mode=%s | %d partition(s) disponibles | %d à traiter%s",
        domain,
        mode,
        len(available),
        len(selected),
        f" ({selected[0]} → {selected[-1]})" if selected else "",
    )
    return selected


def build_input_paths(domain: str, dates: Iterable[str]) -> List[str]:
    """Chemins s3a explicites des partitions à lire.

    Passer la liste exacte plutôt qu'un glob global évite à Spark de lister
    l'intégralité du bucket à chaque run incrémental.
    """
    root = SETTINGS.raw_root
    return [f"{root}/{domain}/{_PARTITION_PREFIX}{d}/" for d in dates]
