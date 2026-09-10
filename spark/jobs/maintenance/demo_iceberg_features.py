#!/usr/bin/env python3
"""BONUS — démonstration des fonctionnalités avancées Iceberg + Nessie.

Support de la partie 7 du sujet (« démontrer une fonctionnalité Iceberg
avancée »). Le job est scénarisé pour être joué tel quel pendant la vidéo :
chaque étape s'affiche avec son intitulé, et le résultat SQL est montré à
l'écran juste après.

  ÉTAPE 1 — HISTORIQUE DES SNAPSHOTS
            La table de métadonnées `.snapshots` d'Iceberg, qui est le
            mécanisme sous-jacent du time travel.

  ÉTAPE 2 — TIME TRAVEL PAR VERSION ET PAR DATE
            `VERSION AS OF <snapshot_id>` et `TIMESTAMP AS OF <ts>` sur une
            table Gold. On compare le contenu d'hier et celui d'aujourd'hui.

  ÉTAPE 3 — SCHEMA EVOLUTION
            Ajout d'une colonne sur une table Silver déjà peuplée. Iceberg ne
            réécrit AUCUN fichier de données : il ajoute simplement un champ au
            schéma, avec un identifiant de colonne stable. Les anciens
            snapshots restent lisibles avec leur schéma d'origine.

  ÉTAPE 4 — BRANCHES NESSIE
            Création d'une branche `experimentation`, écriture dessus,
            vérification que `main` n'a pas bougé, puis suppression. C'est le
            « git pour les données » : on peut recalculer un agrégat sur une
            branche, le contrôler, et ne fusionner que si le résultat convient.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession  # noqa: E402

from common.config import GOLD_TABLES, SETTINGS, SILVER_TABLES  # noqa: E402
from common.session import build_spark, get_logger, table_exists  # noqa: E402

LOG = get_logger("demo.iceberg")

GOLD_TABLE = SETTINGS.gold(GOLD_TABLES["catalog_kpi"])
SILVER_TABLE = SETTINGS.silver(SILVER_TABLES["products"])
BRANCH = "experimentation"


def banner(step: str, title: str) -> None:
    LOG.info("")
    LOG.info("=" * 78)
    LOG.info("  ÉTAPE %s — %s", step, title)
    LOG.info("=" * 78)


def show_snapshots(spark: SparkSession) -> list:
    banner("1", "Historique des snapshots Iceberg")
    df = spark.sql(
        f"""
        SELECT snapshot_id, committed_at, operation,
               summary['added-records']   AS lignes_ajoutees,
               summary['total-records']   AS lignes_totales
        FROM {GOLD_TABLE}.snapshots
        ORDER BY committed_at
        """
    )
    df.show(20, truncate=False)
    return df.collect()


def time_travel(spark: SparkSession, snapshots: list) -> None:
    banner("2", "Time travel — VERSION AS OF / TIMESTAMP AS OF")
    if len(snapshots) < 2:
        LOG.warning(
            "Un seul snapshot : relancer le pipeline une seconde fois pour "
            "disposer de deux versions comparables."
        )
        return

    premier = snapshots[0]["snapshot_id"]
    dernier = snapshots[-1]["snapshot_id"]

    LOG.info("Contenu du PREMIER snapshot (%s) :", premier)
    spark.sql(
        f"""
        SELECT count(*) AS nb_lignes,
               min(snapshot_date) AS debut,
               max(snapshot_date) AS fin
        FROM {GOLD_TABLE} VERSION AS OF {premier}
        """
    ).show(truncate=False)

    LOG.info("Contenu du DERNIER snapshot (%s) :", dernier)
    spark.sql(
        f"""
        SELECT count(*) AS nb_lignes,
               min(snapshot_date) AS debut,
               max(snapshot_date) AS fin
        FROM {GOLD_TABLE} VERSION AS OF {dernier}
        """
    ).show(truncate=False)

    horodatage = snapshots[-1]["committed_at"]
    LOG.info("Même lecture, exprimée par la DATE (%s) :", horodatage)
    spark.sql(
        f"SELECT count(*) AS nb_lignes FROM {GOLD_TABLE} "
        f"TIMESTAMP AS OF '{horodatage}'"
    ).show(truncate=False)

    LOG.info("Écart entre les deux versions, catégorie par catégorie :")
    spark.sql(
        f"""
        SELECT coalesce(a.category_slug, b.category_slug) AS categorie,
               a.nb AS lignes_version_initiale,
               b.nb AS lignes_version_courante
        FROM (SELECT category_slug, count(*) AS nb
              FROM {GOLD_TABLE} VERSION AS OF {premier} GROUP BY category_slug) a
        FULL OUTER JOIN
             (SELECT category_slug, count(*) AS nb
              FROM {GOLD_TABLE} VERSION AS OF {dernier} GROUP BY category_slug) b
          ON a.category_slug = b.category_slug
        ORDER BY categorie
        """
    ).show(truncate=False)


def schema_evolution(spark: SparkSession) -> None:
    banner("3", "Schema evolution — ajout de colonne sans réécriture")

    avant = spark.sql(f"DESCRIBE TABLE {SILVER_TABLE}").count()
    LOG.info("Schéma actuel : %d colonne(s)", avant)

    spark.sql(
        f"""
        ALTER TABLE {SILVER_TABLE}
        ADD COLUMNS (
            margin_estimate_pct double
                COMMENT 'Marge estimee - colonne ajoutee a chaud pour demonstration'
        )
        """
    )
    LOG.info("Colonne `margin_estimate_pct` ajoutée. Nouveau schéma :")
    spark.sql(f"DESCRIBE TABLE {SILVER_TABLE}").show(50, truncate=False)

    LOG.info(
        "Les lignes existantes exposent la nouvelle colonne à NULL, "
        "sans qu'aucun fichier Parquet n'ait été réécrit :"
    )
    spark.sql(
        f"SELECT product_id, snapshot_date, unit_price, margin_estimate_pct "
        f"FROM {SILVER_TABLE} LIMIT 5"
    ).show(truncate=False)

    LOG.info("Retrait de la colonne pour laisser la plateforme dans son état initial.")
    spark.sql(f"ALTER TABLE {SILVER_TABLE} DROP COLUMN margin_estimate_pct")


def nessie_branching(spark: SparkSession) -> None:
    banner("4", "Branches Nessie — « git pour les données »")
    catalog = SETTINGS.catalog

    spark.sql(f"DROP BRANCH IF EXISTS {BRANCH} IN {catalog}")
    spark.sql(f"CREATE BRANCH {BRANCH} IN {catalog} FROM {SETTINGS.nessie_ref}")
    LOG.info("Références disponibles dans le catalogue :")
    spark.sql(f"LIST REFERENCES IN {catalog}").show(truncate=False)

    reference_main = spark.sql(f"SELECT count(*) AS n FROM {GOLD_TABLE}").collect()[0]["n"]

    spark.sql(f"USE REFERENCE {BRANCH} IN {catalog}")
    LOG.info("Basculé sur la branche `%s`. Suppression d'une catégorie…", BRANCH)
    spark.sql(f"DELETE FROM {GOLD_TABLE} WHERE category_slug = 'TOTAL'")
    sur_branche = spark.sql(f"SELECT count(*) AS n FROM {GOLD_TABLE}").collect()[0]["n"]

    spark.sql(f"USE REFERENCE {SETTINGS.nessie_ref} IN {catalog}")
    apres_main = spark.sql(f"SELECT count(*) AS n FROM {GOLD_TABLE}").collect()[0]["n"]

    LOG.info("main avant       : %d ligne(s)", reference_main)
    LOG.info("branche %-9s: %d ligne(s)  <- modification isolée", BRANCH, sur_branche)
    LOG.info("main après       : %d ligne(s)  <- inchangé", apres_main)

    if apres_main != reference_main:
        raise RuntimeError("La branche a fui sur main — configuration Nessie à revoir")

    spark.sql(f"DROP BRANCH {BRANCH} IN {catalog}")
    LOG.info("Branche `%s` supprimée, expérimentation abandonnée sans trace.", BRANCH)


def main() -> int:
    parser = argparse.ArgumentParser(description="Démonstration Iceberg / Nessie")
    parser.add_argument("--skip-branching", action="store_true")
    parser.add_argument("--skip-schema-evolution", action="store_true")
    args = parser.parse_args()

    spark = build_spark("demo-iceberg-features")

    if not table_exists(spark, GOLD_TABLE):
        LOG.error("Table %s absente — exécuter le pipeline complet d'abord", GOLD_TABLE)
        spark.stop()
        return 1

    snapshots = show_snapshots(spark)
    time_travel(spark, snapshots)
    if not args.skip_schema_evolution:
        schema_evolution(spark)
    if not args.skip_branching:
        nessie_branching(spark)

    LOG.info("")
    LOG.info("=== Démonstration terminée ===")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
