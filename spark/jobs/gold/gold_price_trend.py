#!/usr/bin/env python3
"""COUCHE GOLD — trajectoire tarifaire produit sur la fenêtre d'historisation.

------------------------------------------------------------------------------
 QUESTION MÉTIER
------------------------------------------------------------------------------
 « Quels produits ont vu leur prix bouger sur les six derniers mois, dans quel
   sens, avec quelle volatilité, et à quel moment se situent les promotions ? »

 C'est LA table qui justifie l'effort d'historisation : sans les 180 snapshots
 quotidiens, aucune de ces colonnes n'existerait. Elle est aussi la vitrine
 des fonctions de fenêtrage Spark et le support naturel d'une démonstration de
 time travel Iceberg.

 Grain : une ligne par (product_id, snapshot_date).

 Indicateurs :
   * price_dod_pct        — variation jour/jour, détecte les ruptures ;
   * price_ma7 / price_ma30 — moyennes mobiles, lissent le bruit quotidien et
                            rendent la tendance lisible sur un graphique ;
   * price_vs_ma30_pct    — écart au niveau « normal » : négatif fort = promo ;
   * is_promotion         — règle métier explicite : au moins 5 % sous la
                            moyenne 30 jours ET en baisse par rapport à la
                            veille (les deux conditions évitent de qualifier
                            de promotion une simple baisse tendancielle) ;
   * price_index_100      — prix rebasé à 100 au premier jour de la fenêtre,
                            seule façon de comparer sur un même graphique un
                            produit à 10 € et un produit à 999 € ;
   * cumulative_change_pct — variation depuis le début de la fenêtre.
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

LOG = get_logger("gold.price_trend")

SILVER_PRODUCTS = SETTINGS.silver(SILVER_TABLES["products"])
GOLD_TABLE = SETTINGS.gold(GOLD_TABLES["price_trend"])


def transform(spark: SparkSession) -> DataFrame:
    return spark.sql(
        f"""
        WITH serie AS (
            SELECT
                product_id,
                snapshot_date,
                title,
                category_slug,
                category_label,
                price_band,
                unit_price,
                unit_price_observed,
                rating_score,
                review_count,
                is_reconstructed
            FROM {SILVER_PRODUCTS}
        ),
        fenetres AS (
            SELECT
                s.*,
                lag(unit_price) OVER w_ord                                AS price_prev_day,
                first_value(unit_price) OVER w_full                       AS price_first_day,
                avg(unit_price)  OVER w_7                                 AS price_ma7,
                avg(unit_price)  OVER w_30                                AS price_ma30,
                stddev_samp(unit_price) OVER w_30                         AS price_std30,
                min(unit_price)  OVER w_full                              AS price_min_window,
                max(unit_price)  OVER w_full                              AS price_max_window,
                row_number() OVER w_ord                                   AS day_rank
            FROM serie s
            WINDOW
                w_ord  AS (PARTITION BY product_id ORDER BY snapshot_date),
                w_full AS (PARTITION BY product_id ORDER BY snapshot_date
                           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
                w_7    AS (PARTITION BY product_id ORDER BY snapshot_date
                           ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
                w_30   AS (PARTITION BY product_id ORDER BY snapshot_date
                           ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
        )
        SELECT
            product_id,
            snapshot_date,
            title,
            category_slug,
            category_label,
            price_band,
            unit_price,
            unit_price_observed,
            cast(price_prev_day AS decimal(10,2))                         AS price_prev_day,

            cast(round(100.0 * (unit_price - price_prev_day)
                       / nullif(price_prev_day, 0), 2) AS decimal(8,2))   AS price_dod_pct,

            cast(round(price_ma7, 2)  AS decimal(10,2))                   AS price_ma7,
            cast(round(price_ma30, 2) AS decimal(10,2))                   AS price_ma30,
            cast(round(coalesce(price_std30, 0), 3) AS decimal(10,3))     AS price_volatility_30d,

            cast(round(100.0 * (unit_price - price_ma30)
                       / nullif(price_ma30, 0), 2) AS decimal(8,2))       AS price_vs_ma30_pct,

            -- Règle promotion : sous la moyenne 30j de plus de 5 % ET en recul
            (unit_price < price_ma30 * 0.95
             AND price_prev_day IS NOT NULL
             AND unit_price < price_prev_day)                             AS is_promotion,

            cast(round(100.0 * unit_price / nullif(price_first_day, 0), 2)
                 AS decimal(8,2))                                         AS price_index_100,
            cast(round(100.0 * (unit_price - price_first_day)
                       / nullif(price_first_day, 0), 2) AS decimal(8,2))  AS cumulative_change_pct,

            cast(price_min_window AS decimal(10,2))                       AS price_min_window,
            cast(price_max_window AS decimal(10,2))                       AS price_max_window,
            (unit_price = price_min_window)                               AS is_window_low,

            rating_score,
            review_count,
            day_rank,
            is_reconstructed,
            current_timestamp()                                           AS gold_ts
        FROM fenetres
        """
    )


def main() -> int:
    argparse.ArgumentParser(description="Job Gold — tendance des prix").parse_args()

    spark = build_spark("gold-price-trend")
    LOG.info("=== GOLD tendance des prix → %s ===", GOLD_TABLE)

    df = transform(spark)
    replace_table(
        df,
        GOLD_TABLE,
        comment="Couche Gold - trajectoire tarifaire par produit sur la fenetre historisee",
        logger=LOG,
        partition_by=F.expr("months(snapshot_date)"),
        sort_columns=["product_id", "snapshot_date"],
    )
    log_table_stats(spark, GOLD_TABLE, LOG)

    LOG.info("Produits les plus volatils sur la fenêtre :")
    spark.sql(
        f"""
        SELECT product_id, max(title) AS title,
               round(avg(price_volatility_30d), 3) AS volatilite_moyenne,
               sum(CASE WHEN is_promotion THEN 1 ELSE 0 END) AS jours_en_promotion
        FROM {GOLD_TABLE}
        GROUP BY product_id
        ORDER BY volatilite_moyenne DESC
        LIMIT 5
        """
    ).show(truncate=False)

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
