#!/usr/bin/env python3
"""COUCHE GOLD — chiffre d'affaires quotidien par catégorie (jointure inter-domaines).

------------------------------------------------------------------------------
 QUESTION MÉTIER
------------------------------------------------------------------------------
 « Combien vend-on chaque jour, dans quelle catégorie, avec quel panier moyen,
   et quelle est la tendance sur six mois ? »

 C'est la table qui matérialise la JOINTURE ENTRE DOMAINES exigée par le sujet
 (Partie 5.3) : faits commandes (domaine paniers) × dimension produit (domaine
 produits) × dimension client (domaine clients).

------------------------------------------------------------------------------
 POINT TECHNIQUE IMPORTANT : LA JOINTURE EST TEMPORELLE
------------------------------------------------------------------------------
 La dimension produit contient une photo par jour. Valoriser une commande du
 12 mars avec le prix du catalogue d'aujourd'hui serait une erreur classique
 et grossière. La jointure se fait donc sur (product_id, order_date =
 snapshot_date) : **chaque ligne de commande est valorisée au prix en vigueur
 le jour où elle a été passée**. C'est exactement l'usage d'une dimension à
 évolution lente, et c'est ce que permet l'historisation sur 6 mois.

 Un `LEFT JOIN` est utilisé volontairement : si un produit commandé n'existe
 pas dans le catalogue de ce jour-là, la ligne n'est pas silencieusement
 perdue — elle est comptée dans `nb_unmatched_lines`, un indicateur de
 cohérence référentielle exposé jusqu'en Gold.

 Grain : une ligne par (order_date, category_slug) + une ligne « TOTAL » / jour.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common.config import GOLD_TABLES, SETTINGS, SILVER_TABLES  # noqa: E402
from common.history import build_date_dimension  # noqa: E402
from common.session import build_spark, get_logger, log_table_stats  # noqa: E402
from common.writer import replace_table  # noqa: E402

LOG = get_logger("gold.sales_daily")

SILVER_ORDERS = SETTINGS.silver(SILVER_TABLES["carts"])
SILVER_PRODUCTS = SETTINGS.silver(SILVER_TABLES["products"])
SILVER_CUSTOMERS = SETTINGS.silver(SILVER_TABLES["users"])
GOLD_TABLE = SETTINGS.gold(GOLD_TABLES["sales_daily"])


def transform(spark: SparkSession) -> DataFrame:
    # Dimension calendaire : garantit qu'un jour sans commande apparaît quand
    # même dans la série, avec des zéros, au lieu de disparaître et de fausser
    # les moyennes mobiles côté BI.
    build_date_dimension(spark).createOrReplaceTempView("dim_date")

    return spark.sql(
        f"""
        WITH lignes_valorisees AS (
            SELECT
                o.order_date,
                o.order_id,
                o.customer_id,
                o.product_id,
                o.quantity,
                -- Prix EN VIGUEUR LE JOUR DE LA COMMANDE
                p.unit_price,
                p.category_slug,
                p.category_label,
                p.price_band,
                p.rating_score,
                cast(o.quantity * p.unit_price AS decimal(14,2))   AS line_revenue,
                (p.product_id IS NULL)                             AS is_unmatched
            FROM {SILVER_ORDERS} o
            LEFT JOIN {SILVER_PRODUCTS} p
                   ON p.product_id    = o.product_id
                  AND p.snapshot_date = o.order_date
        ),
        -- Deuxième jointure inter-domaines : rattachement au référentiel client
        lignes_enrichies AS (
            SELECT
                l.*,
                c.city                                             AS customer_city,
                c.email_domain                                     AS customer_email_domain
            FROM lignes_valorisees l
            LEFT JOIN (
                SELECT customer_id, city, email_domain, snapshot_date
                FROM {SILVER_CUSTOMERS}
            ) c
                   ON c.customer_id   = l.customer_id
                  AND c.snapshot_date = l.order_date
        ),
        par_categorie AS (
            SELECT
                order_date,
                coalesce(category_slug, 'inconnu')                  AS category_slug,
                coalesce(max(category_label), 'Non rattache')       AS category_label,
                count(DISTINCT order_id)                            AS nb_orders,
                count(*)                                            AS nb_order_lines,
                sum(quantity)                                       AS nb_items_sold,
                count(DISTINCT customer_id)                         AS nb_active_customers,
                count(DISTINCT product_id)                          AS nb_distinct_products,
                count(DISTINCT customer_city)                       AS nb_cities,
                cast(sum(line_revenue) AS decimal(14,2))            AS revenue,
                cast(round(avg(unit_price), 2) AS decimal(10,2))     AS avg_unit_price,
                cast(round(avg(rating_score), 3) AS decimal(4,3))    AS avg_rating_sold,
                sum(CASE WHEN is_unmatched THEN 1 ELSE 0 END)        AS nb_unmatched_lines
            FROM lignes_enrichies
            GROUP BY order_date, coalesce(category_slug, 'inconnu')
        ),
        total_jour AS (
            SELECT
                order_date,
                'TOTAL'                                             AS category_slug,
                'Toutes categories'                                 AS category_label,
                count(DISTINCT order_id)                            AS nb_orders,
                count(*)                                            AS nb_order_lines,
                sum(quantity)                                       AS nb_items_sold,
                count(DISTINCT customer_id)                         AS nb_active_customers,
                count(DISTINCT product_id)                          AS nb_distinct_products,
                count(DISTINCT customer_city)                       AS nb_cities,
                cast(sum(line_revenue) AS decimal(14,2))            AS revenue,
                cast(round(avg(unit_price), 2) AS decimal(10,2))     AS avg_unit_price,
                cast(round(avg(rating_score), 3) AS decimal(4,3))    AS avg_rating_sold,
                sum(CASE WHEN is_unmatched THEN 1 ELSE 0 END)        AS nb_unmatched_lines
            FROM lignes_enrichies
            GROUP BY order_date
        ),
        agregats AS (
            SELECT * FROM par_categorie
            UNION ALL
            SELECT * FROM total_jour
        ),
        -- Complétion calendaire : chaque couple (jour, catégorie connue) existe
        squelette AS (
            SELECT d.date_key AS order_date, c.category_slug, c.category_label
            FROM dim_date d
            CROSS JOIN (SELECT DISTINCT category_slug, category_label FROM agregats) c
        ),
        serie_complete AS (
            SELECT
                s.order_date,
                s.category_slug,
                s.category_label,
                coalesce(a.nb_orders, 0)             AS nb_orders,
                coalesce(a.nb_order_lines, 0)        AS nb_order_lines,
                coalesce(a.nb_items_sold, 0)         AS nb_items_sold,
                coalesce(a.nb_active_customers, 0)   AS nb_active_customers,
                coalesce(a.nb_distinct_products, 0)  AS nb_distinct_products,
                coalesce(a.nb_cities, 0)             AS nb_cities,
                coalesce(a.revenue, cast(0 AS decimal(14,2)))     AS revenue,
                a.avg_unit_price,
                a.avg_rating_sold,
                coalesce(a.nb_unmatched_lines, 0)    AS nb_unmatched_lines
            FROM squelette s
            LEFT JOIN agregats a
                   ON a.order_date    = s.order_date
                  AND a.category_slug = s.category_slug
        )
        SELECT
            sc.order_date,
            sc.category_slug,
            sc.category_label,
            d.year,
            d.month,
            d.week_of_year,
            d.day_name,
            d.is_weekend,

            sc.nb_orders,
            sc.nb_order_lines,
            sc.nb_items_sold,
            sc.nb_active_customers,
            sc.nb_distinct_products,
            sc.nb_cities,
            sc.revenue,
            sc.avg_unit_price,
            sc.avg_rating_sold,

            -- Panier moyen : indicateur commercial de référence
            cast(round(sc.revenue / nullif(sc.nb_orders, 0), 2) AS decimal(10,2))
                                                                 AS avg_order_value,
            cast(round(sc.nb_items_sold / nullif(sc.nb_orders, 0), 2) AS decimal(8,2))
                                                                 AS avg_items_per_order,

            -- Tendance : moyenne mobile 7 jours et cumul depuis le début du mois
            cast(round(avg(sc.revenue) OVER (
                    PARTITION BY sc.category_slug ORDER BY sc.order_date
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS decimal(14,2))
                                                                 AS revenue_ma7,
            cast(sum(sc.revenue) OVER (
                    PARTITION BY sc.category_slug, d.year, d.month
                    ORDER BY sc.order_date) AS decimal(14,2))     AS revenue_mtd,
            cast(round(100.0 * (sc.revenue - lag(sc.revenue, 7) OVER (
                    PARTITION BY sc.category_slug ORDER BY sc.order_date))
                 / nullif(lag(sc.revenue, 7) OVER (
                    PARTITION BY sc.category_slug ORDER BY sc.order_date), 0), 2)
                 AS decimal(10,2))                               AS revenue_wow_pct,

            sc.nb_unmatched_lines,
            current_timestamp()                                  AS gold_ts
        FROM serie_complete sc
        JOIN dim_date d ON d.date_key = sc.order_date
        """
    )


def main() -> int:
    argparse.ArgumentParser(description="Job Gold — ventes quotidiennes").parse_args()

    spark = build_spark("gold-sales-daily")
    LOG.info("=== GOLD ventes quotidiennes → %s ===", GOLD_TABLE)

    df = transform(spark)
    replace_table(
        df,
        GOLD_TABLE,
        comment="Couche Gold - CA quotidien par categorie (jointure commandes x produits x clients)",
        logger=LOG,
        partition_by=F.expr("months(order_date)"),
        sort_columns=["order_date", "category_slug"],
    )
    log_table_stats(spark, GOLD_TABLE, LOG)

    LOG.info("Chiffre d'affaires des 7 derniers jours :")
    spark.sql(
        f"""
        SELECT order_date, day_name, nb_orders, nb_items_sold, revenue,
               avg_order_value, revenue_ma7, revenue_wow_pct
        FROM {GOLD_TABLE}
        WHERE category_slug = 'TOTAL'
        ORDER BY order_date DESC
        LIMIT 7
        """
    ).show(truncate=False)

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
