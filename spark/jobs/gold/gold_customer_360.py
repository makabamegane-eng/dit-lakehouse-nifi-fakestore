#!/usr/bin/env python3
"""COUCHE GOLD — vue client 360°, segmentation RFM sur la fenêtre historisée.

------------------------------------------------------------------------------
 QUESTION MÉTIER
------------------------------------------------------------------------------
 « Qui sont mes clients, combien valent-ils, que consomment-ils, et lesquels
   sont en train de me filer entre les doigts ? »

 Deuxième table à jointure inter-domaines : référentiel client (domaine
 clients) × faits commandes (domaine paniers) × dimension produit (domaine
 produits). Grain : une ligne par client — la table de destination naturelle
 d'un outil CRM ou d'un dashboard.

------------------------------------------------------------------------------
 CHOIX MÉTHODOLOGIQUES
------------------------------------------------------------------------------
 * RÉFÉRENTIEL AU DERNIER ÉTAT CONNU — un client est décrit par son snapshot
   le plus récent, pas par une moyenne de ses snapshots. On prend donc la
   photo la plus récente de dim_customers.
 * VALORISATION AU PRIX DU JOUR — comme pour les ventes, chaque ligne est
   valorisée au prix en vigueur à la date de la commande.
 * SEGMENTATION RFM — Récence / Fréquence / Montant, chaque axe noté de 1 à 4
   par quartile (`ntile`). Le score composite est concaténé (ex. « 434 »),
   puis traduit en segment lisible. On utilise des quartiles plutôt que des
   seuils absolus : la segmentation reste valable quel que soit le volume.
 * CLIENTS SANS COMMANDE — conservés via un LEFT JOIN, avec des zéros. Les
   exclure masquerait exactement la population qu'un service marketing veut
   voir.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402

from common.config import GOLD_TABLES, SETTINGS, SILVER_TABLES  # noqa: E402
from common.session import build_spark, get_logger, log_table_stats  # noqa: E402
from common.writer import replace_table  # noqa: E402

LOG = get_logger("gold.customer_360")

SILVER_ORDERS = SETTINGS.silver(SILVER_TABLES["carts"])
SILVER_PRODUCTS = SETTINGS.silver(SILVER_TABLES["products"])
SILVER_CUSTOMERS = SETTINGS.silver(SILVER_TABLES["users"])
GOLD_TABLE = SETTINGS.gold(GOLD_TABLES["customer_360"])


def transform(spark: SparkSession) -> DataFrame:
    return spark.sql(
        f"""
        WITH derniere_photo_client AS (
            SELECT *
            FROM (
                SELECT c.*,
                       row_number() OVER (
                           PARTITION BY customer_id ORDER BY snapshot_date DESC
                       ) AS rang
                FROM {SILVER_CUSTOMERS} c
            )
            WHERE rang = 1
        ),
        lignes_valorisees AS (
            SELECT
                o.customer_id,
                o.order_id,
                o.order_date,
                o.product_id,
                o.quantity,
                p.category_slug,
                p.category_label,
                p.price_band,
                p.rating_score,
                cast(o.quantity * p.unit_price AS decimal(14,2)) AS line_revenue
            FROM {SILVER_ORDERS} o
            LEFT JOIN {SILVER_PRODUCTS} p
                   ON p.product_id    = o.product_id
                  AND p.snapshot_date = o.order_date
        ),
        agregats_client AS (
            SELECT
                customer_id,
                count(DISTINCT order_id)                            AS nb_orders,
                count(*)                                            AS nb_order_lines,
                sum(quantity)                                       AS nb_items,
                count(DISTINCT product_id)                          AS nb_distinct_products,
                count(DISTINCT category_slug)                       AS nb_distinct_categories,
                cast(sum(line_revenue) AS decimal(14,2))            AS lifetime_value,
                min(order_date)                                     AS first_order_date,
                max(order_date)                                     AS last_order_date,
                cast(round(avg(rating_score), 3) AS decimal(4,3))   AS avg_rating_purchased,
                count(DISTINCT date_trunc('MONTH', order_date))     AS nb_active_months
            FROM lignes_valorisees
            GROUP BY customer_id
        ),
        -- Catégorie préférée : celle qui pèse le plus en chiffre d'affaires
        categorie_preferee AS (
            SELECT customer_id, category_label AS favorite_category, revenue_categorie
            FROM (
                SELECT
                    customer_id,
                    category_label,
                    sum(line_revenue) AS revenue_categorie,
                    row_number() OVER (
                        PARTITION BY customer_id
                        ORDER BY sum(line_revenue) DESC, category_label
                    ) AS rang
                FROM lignes_valorisees
                WHERE category_label IS NOT NULL
                GROUP BY customer_id, category_label
            )
            WHERE rang = 1
        ),
        socle AS (
            SELECT
                c.customer_id,
                c.full_name,
                c.email_masked,
                c.email_domain,
                c.city,
                c.zip_code,
                c.latitude,
                c.longitude,
                c.has_complete_address,
                c.snapshot_date                                     AS profile_snapshot_date,

                coalesce(a.nb_orders, 0)                            AS nb_orders,
                coalesce(a.nb_order_lines, 0)                       AS nb_order_lines,
                coalesce(a.nb_items, 0)                             AS nb_items,
                coalesce(a.nb_distinct_products, 0)                 AS nb_distinct_products,
                coalesce(a.nb_distinct_categories, 0)               AS nb_distinct_categories,
                coalesce(a.lifetime_value, cast(0 AS decimal(14,2))) AS lifetime_value,
                a.first_order_date,
                a.last_order_date,
                a.avg_rating_purchased,
                coalesce(a.nb_active_months, 0)                     AS nb_active_months,
                f.favorite_category,

                cast(round(coalesce(a.lifetime_value, 0)
                           / nullif(a.nb_orders, 0), 2) AS decimal(12,2))
                                                                    AS avg_order_value,
                datediff(
                    (SELECT max(order_date) FROM {SILVER_ORDERS}), a.last_order_date
                )                                                   AS recency_days,
                datediff(a.last_order_date, a.first_order_date)      AS customer_lifespan_days
            FROM derniere_photo_client c
            LEFT JOIN agregats_client   a ON a.customer_id = c.customer_id
            LEFT JOIN categorie_preferee f ON f.customer_id = c.customer_id
        ),
        scores_rfm AS (
            SELECT
                s.*,
                -- Récence : plus le délai est court, meilleur est le score
                CASE WHEN nb_orders = 0 THEN 0
                     ELSE ntile(4) OVER (ORDER BY recency_days DESC) END  AS r_score,
                CASE WHEN nb_orders = 0 THEN 0
                     ELSE ntile(4) OVER (ORDER BY nb_orders ASC) END      AS f_score,
                CASE WHEN nb_orders = 0 THEN 0
                     ELSE ntile(4) OVER (ORDER BY lifetime_value ASC) END AS m_score
            FROM socle s
        )
        SELECT
            customer_id,
            full_name,
            email_masked,
            email_domain,
            city,
            zip_code,
            latitude,
            longitude,
            has_complete_address,
            profile_snapshot_date,

            nb_orders,
            nb_order_lines,
            nb_items,
            nb_distinct_products,
            nb_distinct_categories,
            favorite_category,
            lifetime_value,
            avg_order_value,
            avg_rating_purchased,
            first_order_date,
            last_order_date,
            recency_days,
            customer_lifespan_days,
            nb_active_months,

            r_score,
            f_score,
            m_score,
            concat(cast(r_score AS string), cast(f_score AS string),
                   cast(m_score AS string))                         AS rfm_score,

            CASE
                WHEN nb_orders = 0                              THEN 'jamais_acheteur'
                WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN 'champion'
                WHEN r_score >= 3 AND m_score >= 3              THEN 'fidele_a_forte_valeur'
                WHEN r_score >= 3                              THEN 'client_actif'
                WHEN r_score = 2                               THEN 'a_reactiver'
                ELSE                                                'a_risque'
            END                                                     AS customer_segment,

            current_timestamp()                                     AS gold_ts
        FROM scores_rfm
        """
    )


def main() -> int:
    argparse.ArgumentParser(description="Job Gold — vue client 360").parse_args()

    spark = build_spark("gold-customer-360")
    LOG.info("=== GOLD client 360 → %s ===", GOLD_TABLE)

    df = transform(spark)
    # Table de faible cardinalité (un client = une ligne) : la partitionner
    # créerait des fichiers d'une poignée de lignes. On laisse Iceberg gérer.
    replace_table(
        df,
        GOLD_TABLE,
        comment="Couche Gold - vue client 360 et segmentation RFM",
        logger=LOG,
        sort_columns=["customer_id"],
    )
    log_table_stats(spark, GOLD_TABLE, LOG)

    LOG.info("Répartition des segments clients :")
    spark.sql(
        f"""
        SELECT customer_segment, count(*) AS nb_clients,
               cast(round(sum(lifetime_value), 2) AS decimal(14,2)) AS ca_segment,
               cast(round(avg(avg_order_value), 2) AS decimal(12,2)) AS panier_moyen
        FROM {GOLD_TABLE}
        GROUP BY customer_segment
        ORDER BY ca_segment DESC
        """
    ).show(truncate=False)

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
