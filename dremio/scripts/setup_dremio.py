#!/usr/bin/env python3
"""Configuration automatique de Dremio OSS — source Nessie + espace de travail.

------------------------------------------------------------------------------
 CE QUE FAIT CE SCRIPT
------------------------------------------------------------------------------
 1. crée le premier utilisateur (Dremio bloque tout tant qu'il n'existe pas) ;
 2. s'authentifie et récupère un jeton ;
 3. déclare la source « lakehouse » de type Nessie, pointée à la fois sur le
    catalogue (métadonnées Iceberg) et sur MinIO (fichiers Parquet) ;
 4. crée un espace `analytics` et y publie trois vues (virtual datasets)
    au-dessus des tables Gold — valorisé par le sujet (Partie 5, dernier §) ;
 5. vérifie que les tables Bronze/Silver/Gold sont bien visibles.

------------------------------------------------------------------------------
 POINT DE CONFIGURATION CRITIQUE
------------------------------------------------------------------------------
 Une source Nessie dans Dremio a besoin des DEUX moitiés de l'information :
   * `nessieEndpoint` — où sont les métadonnées (quelles tables, quels
     snapshots, quels fichiers composent la version courante) ;
   * `awsRootPath` + les propriétés S3 — où sont réellement les octets.
 Oublier la seconde donne une source qui « se connecte » mais dont toutes les
 tables sont vides ou en erreur à la lecture. Les trois propriétés
 `fs.s3a.path.style.access`, `fs.s3a.endpoint` et `dremio.s3.compat` sont ce
 qui rend MinIO utilisable là où Dremio attend un vrai S3.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any, Dict, List

import requests

DREMIO_URL = os.environ.get("DREMIO_URL", "http://localhost:9047")
DREMIO_USER = os.environ.get("DREMIO_USER", "dremio")
DREMIO_PASSWORD = os.environ.get("DREMIO_PASSWORD", "dremio123")

NESSIE_URI = os.environ.get("NESSIE_URI_EXTERNAL", "http://nessie:19120/api/v2")
MINIO_HOST = os.environ.get("MINIO_HOST_INTERNAL", "minio:9000")
MINIO_USER = os.environ.get("MINIO_ROOT_USER", "lakehouse")
MINIO_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "lakehouse123")
WAREHOUSE_BUCKET = os.environ.get("WAREHOUSE_BUCKET", "lakehouse-warehouse")

SOURCE_NAME = "lakehouse"
SPACE_NAME = "analytics"


class Dremio:
    def __init__(self, url: str) -> None:
        self.url = url.rstrip("/")
        self.session = requests.Session()
        self.token: str | None = None

    # ------------------------------------------------------------- Attente #
    def wait_ready(self, attempts: int = 60, delay: int = 5) -> None:
        for i in range(attempts):
            try:
                response = self.session.get(
                    f"{self.url}/apiv2/server_status", timeout=10
                )
                if response.status_code == 200 and "ok" in response.text.lower():
                    print("    Dremio est opérationnel")
                    return
            except requests.RequestException:
                pass
            print(f"    Dremio pas encore prêt ({i + 1}/{attempts})…")
            time.sleep(delay)
        raise RuntimeError("Dremio n'a pas démarré dans le délai imparti")

    # ------------------------------------------------- Premier utilisateur #
    def bootstrap_user(self) -> None:
        payload = {
            "userName": DREMIO_USER,
            "firstName": "DIT",
            "lastName": "Lakehouse",
            "email": "dremio@dit.local",
            "createdAt": int(time.time() * 1000),
            "password": DREMIO_PASSWORD,
        }
        response = self.session.put(
            f"{self.url}/apiv2/bootstrap/firstuser",
            json=payload,
            headers={"Authorization": "_dremionull", "Content-Type": "application/json"},
            timeout=30,
        )
        if response.status_code in (200, 201):
            print(f"    utilisateur « {DREMIO_USER} » créé")
        else:
            # 4xx attendu si l'utilisateur existe déjà : le script est rejouable.
            print("    utilisateur déjà existant — étape ignorée")

    def login(self) -> None:
        response = self.session.post(
            f"{self.url}/apiv2/login",
            json={"userName": DREMIO_USER, "password": DREMIO_PASSWORD},
            timeout=30,
        )
        response.raise_for_status()
        self.token = response.json()["token"]
        self.session.headers.update(
            {"Authorization": f"_dremio{self.token}", "Content-Type": "application/json"}
        )
        print("    authentification réussie")

    # ----------------------------------------------------------- Catalogue #
    def find_catalog_entry(self, name: str) -> Dict[str, Any] | None:
        catalog = self.session.get(f"{self.url}/api/v3/catalog", timeout=30).json()
        for entry in catalog.get("data", []):
            if entry.get("path", [None])[0] == name:
                return entry
        return None

    def delete_entry(self, entry: Dict[str, Any]) -> None:
        self.session.delete(
            f"{self.url}/api/v3/catalog/{entry['id']}",
            params={"tag": entry.get("tag")},
            timeout=60,
        )

    def create_nessie_source(self, recreate: bool = False) -> None:
        existing = self.find_catalog_entry(SOURCE_NAME)
        if existing:
            if not recreate:
                print(f"    source « {SOURCE_NAME} » déjà déclarée")
                return
            self.delete_entry(existing)
            print(f"    ancienne source « {SOURCE_NAME} » supprimée")

        payload = {
            "entityType": "source",
            "name": SOURCE_NAME,
            "type": "NESSIE",
            "config": {
                # --- moitié « métadonnées » : le catalogue versionné ---------
                "nessieEndpoint": NESSIE_URI,
                "nessieAuthType": "NONE",
                # --- moitié « données » : les fichiers Parquet dans MinIO ----
                "credentialType": "ACCESS_KEY",
                "awsAccessKey": MINIO_USER,
                "awsAccessSecret": MINIO_PASSWORD,
                "awsRootPath": f"{WAREHOUSE_BUCKET}/warehouse",
                "secure": False,
                "propertyList": [
                    # MinIO n'implémente pas le virtual-hosted style d'AWS
                    {"name": "fs.s3a.path.style.access", "value": "true"},
                    {"name": "fs.s3a.endpoint", "value": MINIO_HOST},
                    # Bascule Dremio en mode « S3 compatible » (non-AWS)
                    {"name": "dremio.s3.compat", "value": "true"},
                ],
            },
            "metadataPolicy": {
                # Rafraîchissement court : pendant la démonstration de bout en
                # bout, une table Gold recalculée doit être visible tout de
                # suite. En production, on desserrerait ces valeurs.
                "authTTLMs": 86400000,
                "namesRefreshMs": 60000,
                "datasetRefreshAfterMs": 60000,
                "datasetExpireAfterMs": 180000,
                "datasetUpdateMode": "PREFETCH_QUERIED",
                "deleteUnavailableDatasets": True,
                "autoPromoteDatasets": True,
            },
        }
        response = self.session.post(
            f"{self.url}/api/v3/catalog", json=payload, timeout=120
        )
        if response.status_code not in (200, 201):
            raise RuntimeError(
                f"Création de la source échouée ({response.status_code}) :\n{response.text[:1200]}"
            )
        print(f"    source Nessie « {SOURCE_NAME} » créée")

    def create_space(self) -> None:
        if self.find_catalog_entry(SPACE_NAME):
            print(f"    espace « {SPACE_NAME} » déjà présent")
            return
        response = self.session.post(
            f"{self.url}/api/v3/catalog",
            json={"entityType": "space", "name": SPACE_NAME},
            timeout=30,
        )
        if response.status_code in (200, 201):
            print(f"    espace « {SPACE_NAME} » créé")

    # ---------------------------------------------------------------- SQL  #
    def sql(self, query: str, timeout: int = 180) -> Dict[str, Any]:
        response = self.session.post(
            f"{self.url}/api/v3/sql", json={"sql": query}, timeout=60
        )
        response.raise_for_status()
        job_id = response.json()["id"]

        deadline = time.time() + timeout
        state = "RUNNING"
        while time.time() < deadline:
            job = self.session.get(f"{self.url}/api/v3/job/{job_id}", timeout=30).json()
            state = job.get("jobState")
            if state in ("COMPLETED", "CANCELED", "FAILED"):
                break
            time.sleep(1.5)

        if state != "COMPLETED":
            message = job.get("errorMessage", "(pas de détail)")
            raise RuntimeError(f"Requête en échec [{state}] : {message}\n{query.strip()[:400]}")

        return self.session.get(
            f"{self.url}/api/v3/job/{job_id}/results",
            params={"limit": 100},
            timeout=60,
        ).json()

    def create_view(self, name: str, query: str) -> None:
        self.sql(f'DROP VIEW IF EXISTS {SPACE_NAME}."{name}"')
        self.sql(f'CREATE VIEW {SPACE_NAME}."{name}" AS {query}')
        print(f"    vue analytics.{name} publiée")


# --------------------------------------------------------------------------- #
#  Vues métier publiées au-dessus des tables Gold
# --------------------------------------------------------------------------- #
VIEWS: List[tuple[str, str]] = [
    (
        "v_ventes_quotidiennes",
        f"""
        SELECT order_date, category_label, nb_orders, nb_items_sold, revenue,
               avg_order_value, revenue_ma7, revenue_wow_pct, day_name, is_weekend
        FROM {SOURCE_NAME}.gold.gold_sales_by_category_daily AT BRANCH main
        WHERE category_slug <> 'TOTAL'
        """,
    ),
    (
        "v_synthese_mensuelle",
        f"""
        SELECT
            YEAR(order_date)  AS annee,
            MONTH(order_date) AS mois,
            category_label    AS categorie,
            SUM(nb_orders)    AS commandes,
            SUM(nb_items_sold) AS articles,
            SUM(revenue)      AS chiffre_affaires,
            ROUND(AVG(avg_order_value), 2) AS panier_moyen
        FROM {SOURCE_NAME}.gold.gold_sales_by_category_daily AT BRANCH main
        WHERE category_slug <> 'TOTAL'
        GROUP BY YEAR(order_date), MONTH(order_date), category_label
        """,
    ),
    (
        "v_clients_a_valeur",
        f"""
        SELECT customer_id, full_name, city, customer_segment, rfm_score,
               nb_orders, lifetime_value, avg_order_value, favorite_category,
               recency_days, last_order_date
        FROM {SOURCE_NAME}.gold.gold_customer_360 AT BRANCH main
        WHERE nb_orders > 0
        """,
    ),
]


def main() -> int:
    parser = argparse.ArgumentParser(description="Configuration de Dremio OSS")
    parser.add_argument("--url", default=DREMIO_URL)
    parser.add_argument("--recreate-source", action="store_true")
    parser.add_argument("--skip-views", action="store_true")
    args = parser.parse_args()

    print("=" * 78)
    print("  CONFIGURATION DE DREMIO — source Nessie + vues analytiques")
    print("=" * 78)

    dremio = Dremio(args.url)

    print("\n[1/5] Disponibilité")
    dremio.wait_ready()

    print("\n[2/5] Authentification")
    dremio.bootstrap_user()
    dremio.login()

    print("\n[3/5] Source Nessie")
    dremio.create_nessie_source(recreate=args.recreate_source)
    print("    attente de la découverte des métadonnées…")
    time.sleep(12)

    print("\n[4/5] Vérification de la visibilité des couches")
    for namespace in ("bronze", "silver", "gold"):
        try:
            result = dremio.sql(
                f"SELECT COUNT(*) AS nb FROM INFORMATION_SCHEMA.\"TABLES\" "
                f"WHERE TABLE_SCHEMA LIKE '%{namespace}%'"
            )
            nb = result["rows"][0]["nb"] if result.get("rows") else 0
            print(f"    {namespace:<7} : {nb} table(s) visible(s)")
        except Exception as exc:  # noqa: BLE001
            print(f"    {namespace:<7} : non interrogeable ({exc})")

    print("\n[5/5] Vues analytiques")
    if args.skip_views:
        print("    --skip-views : étape ignorée")
    else:
        dremio.create_space()
        for name, query in VIEWS:
            try:
                dremio.create_view(name, query)
            except Exception as exc:  # noqa: BLE001
                # Les vues dépendent des tables Gold : si le médaillon n'a pas
                # encore tourné, l'échec est attendu et non bloquant.
                print(f"    vue {name} non créée : {str(exc)[:160]}")

    print("\n" + "=" * 78)
    print(f"  DREMIO PRÊT — {args.url}")
    print(f"  Identifiants : {DREMIO_USER} / {DREMIO_PASSWORD}")
    print(f"  Source       : {SOURCE_NAME}  (bronze / silver / gold)")
    print(f"  Espace       : {SPACE_NAME}   (vues métier)")
    print("=" * 78)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
