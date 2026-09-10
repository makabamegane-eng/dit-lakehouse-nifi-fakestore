#!/usr/bin/env python3
"""COUCHE SILVER — table de faits « lignes de commande » (domaine paniers).

------------------------------------------------------------------------------
 POURQUOI CE JOB EST PARTICULIER
------------------------------------------------------------------------------
 /carts est la ressource la plus pauvre de FakeStoreAPI : 7 paniers figés,
 datés de 2019-2020, soit une vingtaine de lignes au total. Telle quelle, elle
 ne permet aucune analyse de flux sur 6 mois. Ce job la transforme en un flux
 de commandes exploitable, en séparant strictement ce qui est réel de ce qui
 est reconstruit (cf. common/history.py, niveaux 2 et 3) :

   RÉEL         — la composition des paniers (quels produits, quelles
                  quantités relatives, quel client) vient intégralement de
                  l'API. Chaque commande générée est la matérialisation d'un
                  panier réellement observé.
   RECONSTRUIT  — la DATE de la commande et sa RÉPÉTITION dans le temps. Un
                  panier « modèle » se matérialise ou non un jour donné selon
                  une intensité hebdomadaire déterministe (creux en semaine,
                  pic le week-end), puis jusqu'à 3 répliques par jour couvrent
                  des clients différents.

 Toutes les lignes portent `is_reconstructed` et `template_cart_id`, qui
 renvoie au panier d'origine : la traçabilité vers la donnée réelle n'est
 jamais rompue.

------------------------------------------------------------------------------
 GRAIN ET CONTENU
------------------------------------------------------------------------------
 Grain : une ligne = un produit dans une commande (order_id, product_id).
 Volontairement SANS prix ni libellé produit : le prix appartient au domaine
 produits. La jointure inter-domaines est faite en Gold, ce qui garde les
 couches Silver strictement mono-domaine et rend la jointure explicite et
 démontrable (exigence Partie 5.3 du sujet).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common.config import BRONZE_TABLES, SETTINGS, SILVER_TABLES  # noqa: E402
from common.history import order_intensity_expr, uniform_expr  # noqa: E402
from common.incremental import pending_dates, sql_date_list  # noqa: E402
from common.session import build_spark, get_logger, log_table_stats, table_exists  # noqa: E402

LOG = get_logger("silver.orders")

BRONZE_TABLE = SETTINGS.bronze(BRONZE_TABLES["carts"])
SILVER_TABLE = SETTINGS.silver(SILVER_TABLES["carts"])

# Nombre maximal de matérialisations d'un même panier modèle sur une journée.
# 7 paniers x 3 répliques x ~55 % d'intensité moyenne => ~11 commandes/jour,
# soit ~2 000 commandes et ~5 000 lignes sur la fenêtre de 6 mois : un volume
# suffisant pour que les agrégats Gold soient statistiquement lisibles.
REPLICAS_PER_DAY = 3

# Identifiants utilisateurs réellement exposés par /users
CUSTOMER_ID_RANGE = 10


def transform(spark: SparkSession, dates: list[str]) -> DataFrame:
    """Génère les lignes de commande des dates demandées."""
    u_occurrence = uniform_expr("concat_ws(':', cart_id, replica)", "order_date", "occur")
    u_customer = uniform_expr("concat_ws(':', cart_id, replica)", "order_date", "cust")
    u_qty = uniform_expr("concat_ws(':', cart_id, replica, product_id)", "order_date", "qty")
    u_hour = uniform_expr("concat_ws(':', cart_id, replica)", "order_date", "hour")
    intensity = order_intensity_expr("order_date")
    synthetic = str(SETTINGS.synthetic_history).lower()

    return spark.sql(
        f"""
        WITH filtre_qualite AS (
            SELECT *
            FROM {BRONZE_TABLE}
            WHERE _ingest_date IN ({sql_date_list(dates)})
              AND _is_valid = true
        ),
        -- Un seul exemplaire de chaque panier par journée de snapshot
        paniers_modeles AS (
            SELECT
                id                   AS cart_id,
                userId               AS source_customer_id,
                `date`               AS source_cart_date,
                products             AS lignes,
                _ingest_date         AS order_date,
                _record_hash         AS source_record_hash,
                row_number() OVER (
                    PARTITION BY id, _ingest_date
                    ORDER BY _ingest_ts DESC, _source_file DESC
                ) AS rang
            FROM filtre_qualite
        ),
        -- Chaque panier modèle est décliné en N répliques candidates par jour
        candidats AS (
            SELECT p.*, r.replica
            FROM paniers_modeles p
            LATERAL VIEW explode(sequence(0, {REPLICAS_PER_DAY - 1})) r AS replica
            WHERE p.rang = 1
        ),
        -- Filtre d'intensité : décide si la commande a lieu ce jour-là
        commandes AS (
            SELECT
                cart_id,
                replica,
                order_date,
                source_customer_id,
                source_cart_date,
                source_record_hash,
                lignes,
                -- Réplique 0 = client réel du panier ; les répliques suivantes
                -- sont attribuées à d'autres clients du référentiel.
                CASE WHEN replica = 0
                     THEN source_customer_id
                     ELSE 1 + cast(pmod(cast({u_customer} * 1000000 AS bigint),
                                        {CUSTOMER_ID_RANGE}) AS int)
                END                                        AS customer_id,
                -- Horodatage : heure de la journée déterministe, plage 8h-21h
                (cast(order_date AS timestamp)
                 + make_interval(0, 0, 0, 0,
                                 cast(8 + floor({u_hour} * 13) AS int),
                                 cast(floor({u_hour} * 60) AS int), 0))
                                                           AS order_ts
            FROM candidats
            WHERE {u_occurrence} < ({intensity})
        ),
        lignes_commande AS (
            SELECT
                c.*,
                item.productId  AS product_id,
                item.quantity   AS quantity_source
            FROM commandes c
            LATERAL VIEW explode(c.lignes) t AS item
        )
        SELECT
            -- Clé de commande : stable, lisible, rejouable à l'identique
            concat_ws('-', 'ORD', date_format(order_date, 'yyyyMMdd'),
                      lpad(cast(cart_id AS string), 3, '0'),
                      cast(replica AS string))              AS order_id,
            concat_ws('-', 'ORD', date_format(order_date, 'yyyyMMdd'),
                      lpad(cast(cart_id AS string), 3, '0'),
                      cast(replica AS string),
                      lpad(cast(product_id AS string), 3, '0'))
                                                            AS order_line_id,
            order_date                                      AS snapshot_date,
            order_date,
            order_ts,
            customer_id,
            product_id,

            -- Quantité : la quantité réelle du panier, modulée de -30 % à +30 %
            greatest(1, cast(round(quantity_source * (0.7 + 0.6 * ({u_qty}))) AS int))
                                                            AS quantity,
            quantity_source,

            -- Traçabilité vers la donnée réelle
            cart_id                                         AS template_cart_id,
            replica                                         AS template_replica,
            source_customer_id,
            try_to_timestamp(source_cart_date)              AS source_cart_ts,
            source_record_hash,

            {synthetic}                                     AS is_reconstructed,
            current_timestamp()                             AS silver_ts
        FROM lignes_commande
        """
    )


def write_silver(spark: SparkSession, df: DataFrame) -> None:
    writer = (
        df.sortWithinPartitions("order_date", "customer_id", "order_id")
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
                "Couche Silver - faits lignes de commande (grain: commande x produit)",
            )
            .partitionedBy(F.expr("months(order_date)"))
            .create()
        )
    else:
        writer.overwritePartitions()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Job Silver — lignes de commande")
    p.add_argument("--mode", default="incremental", choices=["incremental", "full", "explicit"])
    p.add_argument("--dates", default="")
    p.add_argument("--max-partitions", type=int, default=0)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    spark = build_spark("silver-orders")
    LOG.info("=== SILVER commandes | %s → %s ===", BRONZE_TABLE, SILVER_TABLE)

    explicit = [d.strip() for d in args.dates.split(",") if d.strip()]
    mode = "explicit" if explicit else args.mode

    dates = pending_dates(
        spark,
        source_table=BRONZE_TABLE,
        source_column="_ingest_date",
        target_table=SILVER_TABLE,
        target_column="order_date",
        mode=mode,
        explicit_dates=explicit,
        logger=LOG,
        limit=args.max_partitions or None,
    )
    if not dates:
        LOG.info("Silver commandes déjà à jour")
        spark.stop()
        return 0

    df = transform(spark, dates).cache()
    lines = df.count()
    orders = df.select("order_id").distinct().count()
    LOG.info("%d commande(s) / %d ligne(s) générées sur %d jour(s)", orders, lines, len(dates))

    write_silver(spark, df)
    log_table_stats(spark, SILVER_TABLE, LOG)

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
