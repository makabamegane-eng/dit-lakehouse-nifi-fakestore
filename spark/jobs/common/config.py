"""Configuration centralisée de la plateforme, lue depuis l'environnement.

Aucune valeur d'infrastructure n'est codée en dur dans les jobs : tout passe
par ce module, lui-même alimenté par les variables injectées par le
docker-compose (bloc `x-spark-env`). Un même job peut ainsi tourner tel quel
en local, dans le conteneur Spark ou depuis le driver Airflow.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

# --------------------------------------------------------------------------- #
#  Domaines fonctionnels couverts
# --------------------------------------------------------------------------- #
#  Le sujet impose « au moins deux domaines distincts ». On en couvre trois :
#  produits et utilisateurs (obligatoires) + paniers, utilisés comme proxy du
#  domaine commandes (partie bonus 7.3).
DOMAINS = ("products", "users", "carts")

# Namespaces Iceberg dans le catalogue Nessie
BRONZE_NS = "bronze"
SILVER_NS = "silver"
GOLD_NS = "gold"


def _env(key: str, default: str) -> str:
    value = os.environ.get(key)
    return value if value not in (None, "") else default


def _env_bool(key: str, default: bool) -> bool:
    return _env(key, str(default)).strip().lower() in ("1", "true", "yes", "on")


def _env_int(key: str, default: int) -> int:
    try:
        return int(_env(key, str(default)))
    except ValueError:
        return default


@dataclass(frozen=True)
class Settings:
    """Vue immuable de la configuration effective d'une exécution."""

    # --- Stockage objet ---------------------------------------------------- #
    minio_endpoint: str = field(default_factory=lambda: _env("MINIO_ENDPOINT", "http://minio:9000"))
    access_key: str = field(default_factory=lambda: _env("AWS_ACCESS_KEY_ID", "lakehouse"))
    secret_key: str = field(default_factory=lambda: _env("AWS_SECRET_ACCESS_KEY", "lakehouse123"))
    region: str = field(default_factory=lambda: _env("AWS_REGION", "us-east-1"))
    raw_bucket: str = field(default_factory=lambda: _env("RAW_BUCKET", "lakehouse-raw"))
    warehouse_bucket: str = field(default_factory=lambda: _env("WAREHOUSE_BUCKET", "lakehouse-warehouse"))

    # --- Catalogue --------------------------------------------------------- #
    nessie_uri: str = field(default_factory=lambda: _env("NESSIE_URI", "http://nessie:19120/api/v2"))
    nessie_ref: str = field(default_factory=lambda: _env("NESSIE_REF", "main"))
    catalog: str = field(default_factory=lambda: _env("NESSIE_CATALOG_NAME", "nessie"))

    # --- Historisation ----------------------------------------------------- #
    history_months: int = field(default_factory=lambda: _env_int("HISTORY_MONTHS", 6))
    synthetic_history: bool = field(default_factory=lambda: _env_bool("SYNTHETIC_HISTORY_ENABLED", True))
    synthetic_seed: int = field(default_factory=lambda: _env_int("SYNTHETIC_HISTORY_SEED", 20260909))

    # --- Chemins ----------------------------------------------------------- #
    @property
    def raw_root(self) -> str:
        return f"s3a://{self.raw_bucket}/fakestore"

    def raw_domain_glob(self, domain: str) -> str:
        """Glob de lecture d'un domaine, partitions `ingest_date=` comprises."""
        return f"{self.raw_root}/{domain}/ingest_date=*/*.json"

    def raw_domain_base(self, domain: str) -> str:
        """`basePath` à passer à Spark pour reconstituer la colonne de partition."""
        return f"{self.raw_root}/{domain}"

    def raw_prefix(self, domain: str) -> str:
        """Préfixe S3 (sans schéma) utilisé par boto3 pour lister les dates."""
        return f"fakestore/{domain}/"

    # --- Tables ------------------------------------------------------------ #
    def table(self, namespace: str, name: str) -> str:
        return f"{self.catalog}.{namespace}.{name}"

    def bronze(self, name: str) -> str:
        return self.table(BRONZE_NS, name)

    def silver(self, name: str) -> str:
        return self.table(SILVER_NS, name)

    def gold(self, name: str) -> str:
        return self.table(GOLD_NS, name)


SETTINGS = Settings()


# --------------------------------------------------------------------------- #
#  Nommage des tables du médaillon
# --------------------------------------------------------------------------- #
#  Convention : <couche>.<grain>_<sujet>
#    * Bronze : réplique fidèle et horodatée de la source (raw_*)
#    * Silver : entités métier typées, dédoublonnées, conformes (dim_/fct_)
#    * Gold   : indicateurs prêts à l'analyse, agrégés (gold_*)
BRONZE_TABLES = {
    "products": "raw_products",
    "users": "raw_users",
    "carts": "raw_carts",
}

SILVER_TABLES = {
    "products": "dim_products",
    "users": "dim_customers",
    "carts": "fct_order_items",
}

GOLD_TABLES = {
    "catalog_kpi": "gold_catalog_daily_kpi",
    "price_trend": "gold_product_price_trend",
    "sales_daily": "gold_sales_by_category_daily",
    "customer_360": "gold_customer_360",
}
