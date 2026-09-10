#!/usr/bin/env python3
"""QUALITÉ — porte de contrôle exécutée en fin de pipeline.

Ce job est la dernière tâche du DAG. Son rôle : refuser de déclarer le pipeline
« vert » si les données publiées ne tiennent pas la route. Un pipeline qui
réussit techniquement mais publie des agrégats faux est pire qu'un pipeline en
échec — il est cru sur parole.

Deux niveaux :
  * BLOQUANT (`hard`)   — la tâche Airflow échoue, l'alerte part.
  * AVERTISSEMENT (`soft`) — consigné dans les logs, ne bloque pas.

Le contrôle le plus intéressant est le dernier : une RÉCONCILIATION CROISÉE
entre deux tables Gold construites par deux jobs indépendants
(`gold_sales_by_category_daily` et `gold_customer_360`). Elles agrègent la même
matière première selon deux axes différents ; leur total de chiffre d'affaires
doit coïncider. Si ce n'est pas le cas, c'est qu'une règle de jointure a divergé
quelque part — un contrôle qu'aucune vérification intra-table ne détecterait.
"""

from __future__ import annotations

import argparse
import sys
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

LOG = get_logger("quality")


class Report:
    """Accumulateur de résultats de contrôle."""

    def __init__(self) -> None:
        self.rows: list[tuple[str, str, bool, str]] = []

    def add(self, name: str, severity: str, passed: bool, detail: str) -> None:
        self.rows.append((name, severity, passed, detail))
        icon = "OK  " if passed else ("ECHEC" if severity == "hard" else "ALERTE")
        LOG.info("[%-6s] %-42s %s", icon, name, detail)

    @property
    def has_blocking_failure(self) -> bool:
        return any(not ok and sev == "hard" for _, sev, ok, _ in self.rows)

    def summary(self) -> str:
        total = len(self.rows)
        ko = sum(1 for _, _, ok, _ in self.rows if not ok)
        return f"{total - ko}/{total} contrôle(s) au vert"


def scalar(spark: SparkSession, query: str):
    rows = spark.sql(query).collect()
    return rows[0][0] if rows else None


def run_checks(spark: SparkSession, expected_days: int) -> Report:
    report = Report()

    # --- 1. Présence et volume des couches ---------------------------------- #
    for domain, table_name in BRONZE_TABLES.items():
        table = SETTINGS.bronze(table_name)
        if not table_exists(spark, table):
            report.add(f"bronze.{domain}.existe", "hard", False, "table absente")
            continue
        n = scalar(spark, f"SELECT count(*) FROM {table}")
        report.add(f"bronze.{domain}.non_vide", "hard", n > 0, f"{n} ligne(s)")

    # --- 2. Unicité du grain en Silver -------------------------------------- #
    unicite = [
        (SETTINGS.silver(SILVER_TABLES["products"]), "product_id, snapshot_date"),
        (SETTINGS.silver(SILVER_TABLES["users"]), "customer_id, snapshot_date"),
        (SETTINGS.silver(SILVER_TABLES["carts"]), "order_line_id"),
    ]
    for table, keys in unicite:
        if not table_exists(spark, table):
            report.add(f"{table.split('.')[-1]}.grain", "hard", False, "table absente")
            continue
        dup = scalar(
            spark,
            f"SELECT count(*) FROM (SELECT {keys} FROM {table} "
            f"GROUP BY {keys} HAVING count(*) > 1)",
        )
        report.add(
            f"{table.split('.')[-1]}.grain_unique",
            "hard",
            dup == 0,
            f"{dup} clé(s) dupliquée(s) sur ({keys})",
        )

    # --- 3. Domaines de valeurs --------------------------------------------- #
    silver_products = SETTINGS.silver(SILVER_TABLES["products"])
    if table_exists(spark, silver_products):
        neg = scalar(
            spark, f"SELECT count(*) FROM {silver_products} WHERE unit_price <= 0"
        )
        report.add("silver.produits.prix_positif", "hard", neg == 0, f"{neg} prix <= 0")

        incomplets = scalar(
            spark,
            f"SELECT count(*) FROM {silver_products} WHERE NOT is_complete",
        )
        report.add(
            "silver.produits.completude",
            "soft",
            incomplets == 0,
            f"{incomplets} fiche(s) incomplète(s)",
        )

    # --- 4. Profondeur d'historique ----------------------------------------- #
    if table_exists(spark, silver_products):
        jours = scalar(
            spark, f"SELECT count(DISTINCT snapshot_date) FROM {silver_products}"
        )
        report.add(
            "historique.profondeur",
            "soft",
            jours >= expected_days * 0.9,
            f"{jours} jour(s) distincts (cible ~{expected_days})",
        )

    # --- 5. Intégrité référentielle des ventes ------------------------------ #
    gold_sales = SETTINGS.gold(GOLD_TABLES["sales_daily"])
    if table_exists(spark, gold_sales):
        orphelines = scalar(
            spark, f"SELECT coalesce(sum(nb_unmatched_lines), 0) FROM {gold_sales}"
        )
        report.add(
            "gold.ventes.integrite_referentielle",
            "hard",
            orphelines == 0,
            f"{orphelines} ligne(s) de commande sans produit au catalogue du jour",
        )

        negatif = scalar(spark, f"SELECT count(*) FROM {gold_sales} WHERE revenue < 0")
        report.add("gold.ventes.ca_positif", "hard", negatif == 0, f"{negatif} jour(s) à CA négatif")

    # --- 6. Réconciliation croisée entre deux tables Gold ------------------- #
    gold_customers = SETTINGS.gold(GOLD_TABLES["customer_360"])
    if table_exists(spark, gold_sales) and table_exists(spark, gold_customers):
        ca_ventes = scalar(
            spark,
            f"SELECT coalesce(sum(revenue), 0) FROM {gold_sales} "
            "WHERE category_slug = 'TOTAL'",
        )
        ca_clients = scalar(
            spark, f"SELECT coalesce(sum(lifetime_value), 0) FROM {gold_customers}"
        )
        ecart = abs(float(ca_ventes) - float(ca_clients))
        tolerance = max(1.0, float(ca_ventes) * 0.001)  # 0,1 % pour les arrondis
        report.add(
            "gold.reconciliation_ca",
            "hard",
            ecart <= tolerance,
            f"ventes={ca_ventes:.2f} / clients={ca_clients:.2f} / écart={ecart:.2f}",
        )

    return report


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Contrôles qualité du lakehouse")
    p.add_argument("--expected-days", type=int, default=SETTINGS.history_months * 30)
    p.add_argument(
        "--fail-on-error",
        action="store_true",
        default=True,
        help="Sort en code 1 si un contrôle bloquant échoue (défaut)",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    spark = build_spark("data-quality-checks")
    LOG.info("=== CONTRÔLES QUALITÉ ===")

    report = run_checks(spark, args.expected_days)
    LOG.info("--- Bilan : %s ---", report.summary())

    spark.stop()

    if report.has_blocking_failure and args.fail_on_error:
        LOG.error("Au moins un contrôle bloquant a échoué — pipeline en échec")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
