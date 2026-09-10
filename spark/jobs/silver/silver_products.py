#!/usr/bin/env python3
"""COUCHE SILVER — dimension produit conformée et historisée.

------------------------------------------------------------------------------
 RÈGLES DE TRANSFORMATION BRONZE → SILVER (domaine produits)
------------------------------------------------------------------------------
 1. FILTRE QUALITÉ      — seuls les enregistrements `_is_valid` passent. Les
                          rejets restent consultables en Bronze : Silver est
                          un contrat de qualité, pas une poubelle.
 2. DÉDOUBLONNAGE       — une ligne par (product_id, snapshot_date). Si NiFi a
                          rejoué une journée, plusieurs fichiers coexistent
                          dans la partition : on garde l'appel API le plus
                          récent (`_ingest_ts` max), départage par `_source_file`
                          pour rendre le résultat déterministe.
 3. TYPAGE              — `price` en DECIMAL(10,2) et non DOUBLE : un prix est
                          une valeur monétaire, les erreurs d'arrondi flottant
                          n'y ont pas leur place une fois agrégées.
 4. APLATISSEMENT       — la structure `rating` est éclatée en deux colonnes
                          scalaires, directement requêtables depuis Dremio.
 5. NORMALISATION       — catégorie mise en forme (slug technique + libellé
                          présentable) ; texte nettoyé des espaces parasites.
 6. ENRICHISSEMENT      — segment de prix, longueur de description, indicateur
                          de complétude du catalogue.
 7. HISTORISATION       — application du modèle documenté dans common/history :
                          `unit_price` et `review_count` sont rétro-projetés,
                          la mesure brute reste dans `*_observed`.

 Résultat : une table SCD de type 2 « par snapshot » — chaque jour de la
 fenêtre de 6 mois contient la photographie complète du catalogue, ce qui rend
 triviales les analyses d'évolution en Gold sans logique de fenêtrage complexe.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common.config import BRONZE_TABLES, SETTINGS, SILVER_TABLES  # noqa: E402
from common.history import price_factor_expr, review_growth_expr  # noqa: E402
from common.incremental import anchor_date, pending_dates, sql_date_list  # noqa: E402
from common.session import build_spark, get_logger, log_table_stats, table_exists  # noqa: E402

LOG = get_logger("silver.products")

BRONZE_TABLE = SETTINGS.bronze(BRONZE_TABLES["products"])
SILVER_TABLE = SETTINGS.silver(SILVER_TABLES["products"])


def transform(spark: SparkSession, dates: list[str], end_date: str) -> DataFrame:
    """Construit la dimension produit pour les dates demandées."""
    price_factor = price_factor_expr("id", "_ingest_date", f"date('{end_date}')")
    review_growth = review_growth_expr("_ingest_date", f"date('{end_date}')")

    return spark.sql(
        f"""
        WITH filtre_qualite AS (
            SELECT *
            FROM {BRONZE_TABLE}
            WHERE _ingest_date IN ({sql_date_list(dates)})
              AND _is_valid = true
        ),
        dedoublonnage AS (
            SELECT *,
                   row_number() OVER (
                       PARTITION BY id, _ingest_date
                       ORDER BY _ingest_ts DESC, _source_file DESC
                   ) AS rang
            FROM filtre_qualite
        ),
        base AS (
            SELECT
                id                                            AS product_id,
                _ingest_date                                  AS snapshot_date,
                trim(title)                                   AS title,
                lower(trim(category))                         AS category_slug,
                initcap(regexp_replace(trim(category), "'", ' '))  AS category_label,
                cast(price AS decimal(10,2))                  AS unit_price_observed,
                rating.rate                                   AS rating_score,
                coalesce(rating.count, 0)                     AS review_count_observed,
                trim(description)                             AS description,
                image                                         AS image_url,
                _ingest_ts                                    AS source_ingest_ts,
                _record_hash                                  AS source_record_hash,
                _ingest_date                                  AS _ingest_date
            FROM dedoublonnage
            WHERE rang = 1
        )
        SELECT
            -- Clé de substitution stable : permet de joindre sans dépendre du
            -- couple (id, date) et survit à un changement de source.
            sha2(concat_ws('|', 'product', cast(product_id AS string),
                           cast(snapshot_date AS string)), 256)  AS product_sk,
            product_id,
            snapshot_date,
            title,
            category_slug,
            category_label,

            -- Mesure observée telle que renvoyée par l'API ce jour-là
            unit_price_observed,
            review_count_observed,

            -- Mesures historisées (cf. common/history.py) : identiques aux
            -- mesures observées sur la date la plus récente de la fenêtre.
            cast(round(unit_price_observed * ({price_factor}), 2) AS decimal(10,2))
                                                                 AS unit_price,
            cast(round(review_count_observed * ({review_growth})) AS int)
                                                                 AS review_count,
            rating_score,

            -- Segmentation tarifaire : bornes choisies sur la distribution
            -- réelle du catalogue FakeStore (min 7,95 / max 999,99).
            CASE
                WHEN unit_price_observed < 25  THEN 'entree_de_gamme'
                WHEN unit_price_observed < 100 THEN 'milieu_de_gamme'
                WHEN unit_price_observed < 500 THEN 'premium'
                ELSE 'luxe'
            END                                                  AS price_band,

            length(description)                                  AS description_length,
            image_url,

            -- Complétude de la fiche produit : indicateur de qualité exposé
            -- jusqu'en Gold, il sert de garde-fou à la couche analytique.
            (title IS NOT NULL AND category_slug IS NOT NULL
             AND rating_score IS NOT NULL AND length(description) > 30)
                                                                 AS is_complete,
            {str(SETTINGS.synthetic_history).lower()}            AS is_reconstructed,
            source_ingest_ts,
            source_record_hash,
            current_timestamp()                                  AS silver_ts
        FROM base
        """
    )


def write_silver(spark: SparkSession, df: DataFrame) -> None:
    writer = (
        df.sortWithinPartitions("snapshot_date", "category_slug", "product_id")
        .writeTo(SILVER_TABLE)
        .using("iceberg")
    )
    if not table_exists(spark, SILVER_TABLE):
        LOG.info("Création de %s", SILVER_TABLE)
        (
            writer.tableProperty("format-version", "2")
            .tableProperty("write.parquet.compression-codec", "zstd")
            .tableProperty("write.distribution-mode", "hash")
            .tableProperty(
                "comment",
                "Couche Silver - dimension produit conformee, une photo par jour",
            )
            # Partition mensuelle : 180 partitions journalières pour ~20 produits
            # produiraient des fichiers Parquet minuscules. `months()` regroupe
            # en 6 partitions, taille de fichier saine, élagage encore efficace.
            .partitionedBy(F.expr("months(snapshot_date)"))
            .create()
        )
    else:
        writer.overwritePartitions()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Job Silver — dimension produits")
    p.add_argument("--mode", default="incremental", choices=["incremental", "full", "explicit"])
    p.add_argument("--dates", default="")
    p.add_argument("--end-date", default="", help="Dernier jour de la fenêtre (défaut : aujourd'hui)")
    p.add_argument("--max-partitions", type=int, default=0)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    spark = build_spark("silver-products")
    LOG.info("=== SILVER produits | %s → %s ===", BRONZE_TABLE, SILVER_TABLE)

    explicit = [d.strip() for d in args.dates.split(",") if d.strip()]
    mode = "explicit" if explicit else args.mode

    dates = pending_dates(
        spark,
        source_table=BRONZE_TABLE,
        source_column="_ingest_date",
        target_table=SILVER_TABLE,
        target_column="snapshot_date",
        mode=mode,
        explicit_dates=explicit,
        logger=LOG,
        limit=args.max_partitions or None,
    )
    if not dates:
        LOG.info("Silver produits déjà à jour")
        spark.stop()
        return 0

    # Le modèle d'historisation est ancré sur la date la plus récente présente
    # en Bronze, et non sur `today` : le pipeline rendra le même résultat qu'il
    # soit rejoué aujourd'hui ou dans trois semaines.
    end_date = args.end_date or anchor_date(spark, BRONZE_TABLE, "_ingest_date", dates)
    LOG.info("Ancrage du modèle d'historisation sur %s", end_date)

    df = transform(spark, dates, end_date)
    LOG.info("%d ligne(s) produites", df.count())
    write_silver(spark, df)
    log_table_stats(spark, SILVER_TABLE, LOG)

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
