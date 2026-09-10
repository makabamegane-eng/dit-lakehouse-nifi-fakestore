#!/usr/bin/env python3
"""COUCHE BRONZE — matérialisation fidèle et horodatée de la zone brute.

------------------------------------------------------------------------------
 CE QUE FAIT (ET NE FAIT PAS) CETTE COUCHE
------------------------------------------------------------------------------
 Bronze est la mémoire du lakehouse. Sa règle : **on ne perd rien, on ne
 corrige rien**. Concrètement :

   * les champs de l'API sont conservés tels quels, structures imbriquées
     comprises (`rating`, `address`, `products`) — aucun aplatissement ;
   * aucune règle métier, aucune jointure, aucun filtre destructif ;
   * les enregistrements non conformes ne sont PAS supprimés : ils sont
     marqués (`_is_valid = false`, `_reject_reason`) et restent visibles.
     Un rejet doit être analysable, pas invisible ;
   * huit colonnes techniques préfixées `_` assurent la traçabilité complète
     jusqu'à l'objet MinIO d'origine.

 Seule entorse assumée à la fidélité : le champ `password` de /users n'est
 jamais persisté en clair, il est remplacé par son SHA-256. Stocker un secret
 en clair dans un lakehouse ne se justifie par aucun argument de fidélité.

------------------------------------------------------------------------------
 IDEMPOTENCE
------------------------------------------------------------------------------
 La table est partitionnée par `_ingest_date`, et l'écriture se fait en
 `overwritePartitions()`. Retraiter une date remplace exactement sa partition :
 rejouer un DAG run n'introduit ni doublon ni trou.

------------------------------------------------------------------------------
 USAGE
------------------------------------------------------------------------------
   spark-submit bronze/bronze_ingest.py --domain products --mode incremental
   spark-submit bronze/bronze_ingest.py --domain carts --dates 2026-03-01,2026-03-02
   spark-submit bronze/bronze_ingest.py --domain users --mode full
"""

from __future__ import annotations

import argparse
import sys
import uuid
from pathlib import Path

# Rend le paquet `common` importable quel que soit le répertoire de lancement.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common.config import BRONZE_TABLES, DOMAINS, SETTINGS  # noqa: E402
from common.raw_zone import build_input_paths, resolve_dates  # noqa: E402
from common.schemas import CORRUPT_COLUMN, DOMAIN_SCHEMAS  # noqa: E402
from common.session import build_spark, get_logger, log_table_stats, table_exists  # noqa: E402

LOG = get_logger("bronze")

# Convention de nommage produite par NiFi :
#   <domaine>_<date logique>_<epoch ms de l'appel API>_<uuid>.json
# L'epoch permet de distinguer la date LOGIQUE du snapshot (partition) de
# l'instant RÉEL de l'appel HTTP — indispensable pour auditer un backfill.
FILENAME_PATTERN = r".*/([a-z]+)_(\d{4}-\d{2}-\d{2})_(\d+)_([0-9a-fA-F-]+)\.json$"


# --------------------------------------------------------------------------- #
#  Contrôles de conformité — spécifiques à chaque domaine
# --------------------------------------------------------------------------- #
def quality_expressions(domain: str):
    """Retourne (condition de validité, expression du motif de rejet).

    Les règles restent minimales et non destructives : à ce stade on vérifie
    l'intégrité structurelle (clé présente, types exploitables), pas la
    cohérence métier — celle-ci relève de Silver.
    """
    common_invalid = F.col(CORRUPT_COLUMN).isNotNull()

    if domain == "products":
        cond = (
            F.col("id").isNotNull()
            & F.col("title").isNotNull()
            & F.col("price").isNotNull()
            & (F.col("price") >= 0)
        )
        reason = (
            F.when(common_invalid, F.lit("JSON illisible"))
            .when(F.col("id").isNull(), F.lit("id absent"))
            .when(F.col("title").isNull(), F.lit("title absent"))
            .when(F.col("price").isNull(), F.lit("price absent"))
            .when(F.col("price") < 0, F.lit("price negatif"))
            .otherwise(F.lit(None).cast("string"))
        )
    elif domain == "users":
        cond = (
            F.col("id").isNotNull()
            & F.col("email").isNotNull()
            & F.col("email").contains("@")
        )
        reason = (
            F.when(common_invalid, F.lit("JSON illisible"))
            .when(F.col("id").isNull(), F.lit("id absent"))
            .when(F.col("email").isNull(), F.lit("email absent"))
            .when(~F.col("email").contains("@"), F.lit("email non conforme"))
            .otherwise(F.lit(None).cast("string"))
        )
    else:  # carts
        cond = (
            F.col("id").isNotNull()
            & F.col("userId").isNotNull()
            & (F.size(F.coalesce(F.col("products"), F.array())) > 0)
        )
        reason = (
            F.when(common_invalid, F.lit("JSON illisible"))
            .when(F.col("id").isNull(), F.lit("id absent"))
            .when(F.col("userId").isNull(), F.lit("userId absent"))
            .when(
                F.size(F.coalesce(F.col("products"), F.array())) == 0,
                F.lit("panier vide"),
            )
            .otherwise(F.lit(None).cast("string"))
        )

    return cond & ~common_invalid, reason


def business_columns(domain: str) -> list[str]:
    """Colonnes métier retenues pour le calcul de l'empreinte de contenu."""
    schema = DOMAIN_SCHEMAS[domain]
    return [f.name for f in schema.fields if f.name != CORRUPT_COLUMN]


# --------------------------------------------------------------------------- #
#  Lecture de la zone brute
# --------------------------------------------------------------------------- #
def read_raw(spark: SparkSession, domain: str, paths: list[str]) -> DataFrame:
    """Lit les fichiers JSON déposés par NiFi pour les partitions demandées.

    * `multiLine=true` : chaque objet MinIO contient le tableau JSON complet
      renvoyé par l'API, pas une ligne par enregistrement (format NDJSON).
    * `mode=PERMISSIVE` + colonne de rejet : un fichier corrompu ne fait pas
      échouer le batch, il produit des lignes marquées.
    """
    return (
        spark.read.option("multiLine", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COLUMN)
        .schema(DOMAIN_SCHEMAS[domain])
        .json(paths)
    )


def add_technical_columns(df: DataFrame, domain: str, batch_id: str) -> DataFrame:
    """Ajoute les 8 colonnes de traçabilité communes à toutes les tables Bronze."""
    src = F.input_file_name()
    valid_cond, reject_reason = quality_expressions(domain)

    payload = F.to_json(F.struct(*[F.col(c) for c in business_columns(domain)]))

    return (
        df.withColumn("_source_file", src)
        .withColumn(
            "_ingest_date",
            F.to_date(F.regexp_extract(src, FILENAME_PATTERN, 2), "yyyy-MM-dd"),
        )
        .withColumn(
            "_ingest_ts",
            F.to_timestamp(
                F.from_unixtime(
                    F.regexp_extract(src, FILENAME_PATTERN, 3).cast("long") / 1000
                )
            ),
        )
        .withColumn("_bronze_ts", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_record_hash", F.sha2(payload, 256))
        .withColumn("_is_valid", valid_cond)
        .withColumn("_reject_reason", reject_reason)
    )


def anonymize(df: DataFrame, domain: str) -> DataFrame:
    """Neutralise les secrets avant toute écriture sur disque."""
    if domain != "users":
        return df
    return df.withColumn(
        "password", F.sha2(F.coalesce(F.col("password"), F.lit("")), 256)
    ).withColumnRenamed("password", "password_sha256")


# --------------------------------------------------------------------------- #
#  Écriture Iceberg
# --------------------------------------------------------------------------- #
def write_bronze(spark: SparkSession, df: DataFrame, table: str) -> None:
    """Crée la table au premier passage, écrase les partitions ensuite."""
    writer = df.sortWithinPartitions("_ingest_date", "id").writeTo(table).using("iceberg")

    if not table_exists(spark, table):
        LOG.info("Création de la table Iceberg %s", table)
        (
            writer.tableProperty("format-version", "2")
            .tableProperty("write.parquet.compression-codec", "zstd")
            .tableProperty("write.distribution-mode", "hash")
            .tableProperty("write.metadata.delete-after-commit.enabled", "true")
            .tableProperty("write.metadata.previous-versions-max", "20")
            .tableProperty(
                "comment",
                f"Couche Bronze - copie fidele et horodatee de la zone brute ({table})",
            )
            .partitionedBy(F.col("_ingest_date"))
            .create()
        )
    else:
        LOG.info("Écrasement des partitions concernées de %s", table)
        writer.overwritePartitions()


# --------------------------------------------------------------------------- #
#  Point d'entrée
# --------------------------------------------------------------------------- #
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Job Bronze du médaillon")
    parser.add_argument("--domain", required=True, choices=list(DOMAINS))
    parser.add_argument(
        "--mode",
        default="incremental",
        choices=["incremental", "full", "explicit"],
        help="incremental (défaut) : uniquement les partitions absentes de Bronze",
    )
    parser.add_argument(
        "--dates",
        default="",
        help="Liste de dates YYYY-MM-DD séparées par des virgules (mode explicit)",
    )
    parser.add_argument(
        "--max-partitions",
        type=int,
        default=0,
        help="Plafonne le nombre de partitions traitées par run (0 = illimité)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    domain = args.domain
    table = SETTINGS.bronze(BRONZE_TABLES[domain])
    batch_id = f"bronze-{domain}-{uuid.uuid4().hex[:12]}"

    spark = build_spark(f"bronze-{domain}")
    LOG.info("=== BRONZE | domaine=%s | table=%s | batch=%s ===", domain, table, batch_id)

    explicit = [d.strip() for d in args.dates.split(",") if d.strip()]
    mode = "explicit" if explicit else args.mode

    dates = resolve_dates(
        spark,
        domain=domain,
        bronze_table=table,
        mode=mode,
        explicit_dates=explicit,
        logger=LOG,
        limit=args.max_partitions or None,
    )
    if not dates:
        LOG.info("Rien à traiter — Bronze est déjà à jour pour %s", domain)
        spark.stop()
        return 0

    paths = build_input_paths(domain, dates)
    raw = read_raw(spark, domain, paths)
    enriched = anonymize(add_technical_columns(raw, domain, batch_id), domain)

    # Les lignes dont la partition n'a pas pu être déduite du nom de fichier
    # signalent une convention de nommage NiFi non respectée : on les bloque
    # tout de suite plutôt que de polluer la table avec des NULL de partition.
    orphans = enriched.filter(F.col("_ingest_date").isNull()).count()
    if orphans:
        raise RuntimeError(
            f"{orphans} enregistrement(s) sans date de partition déductible : "
            "vérifier la convention de nommage du flow NiFi"
        )

    total = enriched.count()
    invalid = enriched.filter(~F.col("_is_valid")).count()
    LOG.info(
        "Lecture de %d partition(s) : %d enregistrement(s), dont %d non conforme(s)",
        len(dates),
        total,
        invalid,
    )
    if invalid:
        (
            enriched.filter(~F.col("_is_valid"))
            .groupBy("_reject_reason")
            .count()
            .show(truncate=False)
        )

    write_bronze(spark, enriched, table)
    log_table_stats(spark, table, LOG)
    LOG.info("=== BRONZE %s terminé ===", domain)
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
