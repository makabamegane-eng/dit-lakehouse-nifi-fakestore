#!/usr/bin/env python3
"""COUCHE GOLD — indicateurs quotidiens du catalogue produits.

------------------------------------------------------------------------------
 QUESTION MÉTIER À LAQUELLE CETTE TABLE RÉPOND
------------------------------------------------------------------------------
 « Comment le catalogue évolue-t-il, catégorie par catégorie, sur les six
   derniers mois : prix moyen, dispersion tarifaire, satisfaction client,
   dynamique des avis, et qualité des fiches produit ? »

 Grain : une ligne par (snapshot_date, category_slug), plus une ligne
 « TOTAL » par jour pour disposer directement de l'agrégat global sans
 re-somme côté BI — un GROUP BY ROLLUP matérialisé.

 Indicateurs retenus et pourquoi :
   * avg_unit_price / median_unit_price : la médiane résiste aux quelques
     produits à 999 € qui écrasent la moyenne du catalogue FakeStore ;
   * price_spread_ratio : max/min, mesure de l'étendue de gamme d'une
     catégorie — un indicateur de positionnement commercial ;
   * avg_rating_score pondéré par le nombre d'avis : une note de 4,9 sur
     3 avis ne pèse pas comme une note de 4,1 sur 900 avis ;
   * new_reviews : dérivée du cumul d'avis, c'est le seul indicateur de FLUX
     du domaine produits — il montre l'intérêt de l'historisation ;
   * catalog_completeness_pct : la qualité de donnée remonte jusqu'en Gold,
     elle n'est pas cantonnée à un rapport technique.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common.config import GOLD_TABLES, SETTINGS, SILVER_TABLES  # noqa: E402
from common.session import build_spark, get_logger, log_table_stats  # noqa: E402
from common.writer import replace_table  # noqa: E402

LOG = get_logger("gold.catalog_kpi")

SILVER_PRODUCTS = SETTINGS.silver(SILVER_TABLES["products"])
GOLD_TABLE = SETTINGS.gold(GOLD_TABLES["catalog_kpi"])


def transform(spark: SparkSession) -> DataFrame:
    return spark.sql(
        f"""
        WITH par_categorie AS (
            SELECT
                snapshot_date,
                category_slug,
                max(category_label)                                AS category_label,
                count(*)                                           AS nb_products,
                cast(avg(unit_price) AS decimal(10,2))             AS avg_unit_price,
                cast(percentile_approx(unit_price, 0.5) AS decimal(10,2))
                                                                   AS median_unit_price,
                cast(min(unit_price) AS decimal(10,2))             AS min_unit_price,
                cast(max(unit_price) AS decimal(10,2))             AS max_unit_price,
                cast(sum(unit_price) AS decimal(14,2))             AS catalog_value,
                cast(round(avg(rating_score), 3) AS decimal(4,3))  AS avg_rating_simple,
                cast(round(
                    sum(rating_score * review_count) / nullif(sum(review_count), 0), 3
                ) AS decimal(4,3))                                 AS avg_rating_weighted,
                sum(review_count)                                  AS total_reviews,
                sum(CASE WHEN is_complete THEN 1 ELSE 0 END)       AS nb_complete_products,
                max(CASE WHEN is_reconstructed THEN 1 ELSE 0 END) = 1
                                                                   AS is_reconstructed
            FROM {SILVER_PRODUCTS}
            GROUP BY snapshot_date, category_slug
        ),
        -- Ligne d'agrégat global du jour, matérialisée aux côtés du détail
        total_jour AS (
            SELECT
                snapshot_date,
                'TOTAL'                                            AS category_slug,
                'Tous rayons'                                      AS category_label,
                count(*)                                           AS nb_products,
                cast(avg(unit_price) AS decimal(10,2))             AS avg_unit_price,
                cast(percentile_approx(unit_price, 0.5) AS decimal(10,2))
                                                                   AS median_unit_price,
                cast(min(unit_price) AS decimal(10,2))             AS min_unit_price,
                cast(max(unit_price) AS decimal(10,2))             AS max_unit_price,
                cast(sum(unit_price) AS decimal(14,2))             AS catalog_value,
                cast(round(avg(rating_score), 3) AS decimal(4,3))  AS avg_rating_simple,
                cast(round(
                    sum(rating_score * review_count) / nullif(sum(review_count), 0), 3
                ) AS decimal(4,3))                                 AS avg_rating_weighted,
                sum(review_count)                                  AS total_reviews,
                sum(CASE WHEN is_complete THEN 1 ELSE 0 END)       AS nb_complete_products,
                max(CASE WHEN is_reconstructed THEN 1 ELSE 0 END) = 1
                                                                   AS is_reconstructed
            FROM {SILVER_PRODUCTS}
            GROUP BY snapshot_date
        ),
        union_kpi AS (
            SELECT * FROM par_categorie
            UNION ALL
            SELECT * FROM total_jour
        )
        SELECT
            snapshot_date,
            category_slug,
            category_label,
            nb_products,
            avg_unit_price,
            median_unit_price,
            min_unit_price,
            max_unit_price,
            catalog_value,
            cast(round(max_unit_price / nullif(min_unit_price, 0), 2) AS decimal(10,2))
                                                                   AS price_spread_ratio,
            avg_rating_simple,
            avg_rating_weighted,
            total_reviews,

            -- Flux d'avis du jour : différence du cumul avec la veille.
            -- Première date de la fenêtre => NULL (pas de veille connue),
            -- volontairement pas 0 : une absence n'est pas une valeur nulle.
            total_reviews - lag(total_reviews) OVER (
                PARTITION BY category_slug ORDER BY snapshot_date
            )                                                      AS new_reviews,

            nb_complete_products,
            cast(round(100.0 * nb_complete_products / nullif(nb_products, 0), 1)
                 AS decimal(5,1))                                  AS catalog_completeness_pct,
            is_reconstructed,
            current_timestamp()                                    AS gold_ts
        FROM union_kpi
        """
    )


def main() -> int:
    argparse.ArgumentParser(description="Job Gold — KPI catalogue").parse_args()

    spark = build_spark("gold-catalog-kpi")
    LOG.info("=== GOLD KPI catalogue → %s ===", GOLD_TABLE)

    df = transform(spark)
    replace_table(
        df,
        GOLD_TABLE,
        comment="Couche Gold - indicateurs quotidiens du catalogue par categorie",
        logger=LOG,
        partition_by=F.col("snapshot_date"),
        sort_columns=["snapshot_date", "category_slug"],
    )
    log_table_stats(spark, GOLD_TABLE, LOG)

    LOG.info("Aperçu des 5 derniers jours (tous rayons) :")
    spark.sql(
        f"""
        SELECT snapshot_date, nb_products, avg_unit_price, avg_rating_weighted,
               total_reviews, new_reviews, catalog_completeness_pct
        FROM {GOLD_TABLE}
        WHERE category_slug = 'TOTAL'
        ORDER BY snapshot_date DESC
        LIMIT 5
        """
    ).show(truncate=False)

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
