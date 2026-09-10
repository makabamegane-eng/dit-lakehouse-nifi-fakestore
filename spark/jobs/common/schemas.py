"""Schémas explicites des payloads FakeStoreAPI.

Choix de conception : **aucune inférence de schéma** à la lecture des JSON de
la zone brute. L'inférence oblige Spark à lire deux fois les fichiers, mais
surtout elle rend la couche Bronze instable — un jour où l'API renvoie un
`price` entier plutôt que décimal, le type de colonne changerait tout seul et
casserait Silver. En figeant le schéma ici :

  * les évolutions de l'API sont visibles (colonne manquante => NULL, colonne
    nouvelle => capturée dans `_raw_payload`, pas perdue) ;
  * la couche Bronze reste comparable d'un snapshot à l'autre ;
  * `mode=PERMISSIVE` + `columnNameOfCorruptRecord` isole les enregistrements
    illisibles au lieu de faire échouer tout le batch.
"""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

CORRUPT_COLUMN = "_corrupt_record"


# --------------------------------------------------------------------------- #
#  /products
# --------------------------------------------------------------------------- #
PRODUCTS_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("description", StringType(), True),
        StructField("category", StringType(), True),
        StructField("image", StringType(), True),
        StructField(
            "rating",
            StructType(
                [
                    StructField("rate", DoubleType(), True),
                    StructField("count", IntegerType(), True),
                ]
            ),
            True,
        ),
        StructField(CORRUPT_COLUMN, StringType(), True),
    ]
)


# --------------------------------------------------------------------------- #
#  /users
# --------------------------------------------------------------------------- #
USERS_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("email", StringType(), True),
        StructField("username", StringType(), True),
        # `password` est lu puis immédiatement haché en Bronze (cf.
        # bronze/bronze_ingest.py) : c'est la seule entorse assumée au principe
        # « Bronze = copie fidèle », pour ne jamais persister un secret en clair.
        StructField("password", StringType(), True),
        StructField(
            "name",
            StructType(
                [
                    StructField("firstname", StringType(), True),
                    StructField("lastname", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "address",
            StructType(
                [
                    StructField(
                        "geolocation",
                        StructType(
                            [
                                StructField("lat", StringType(), True),
                                StructField("long", StringType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField("city", StringType(), True),
                    StructField("street", StringType(), True),
                    StructField("number", IntegerType(), True),
                    StructField("zipcode", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("phone", StringType(), True),
        StructField("__v", IntegerType(), True),
        StructField(CORRUPT_COLUMN, StringType(), True),
    ]
)


# --------------------------------------------------------------------------- #
#  /carts  (proxy du domaine « commandes »)
# --------------------------------------------------------------------------- #
CARTS_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("userId", IntegerType(), True),
        StructField("date", StringType(), True),
        StructField(
            "products",
            ArrayType(
                StructType(
                    [
                        StructField("productId", IntegerType(), True),
                        StructField("quantity", IntegerType(), True),
                    ]
                )
            ),
            True,
        ),
        StructField("__v", IntegerType(), True),
        StructField(CORRUPT_COLUMN, StringType(), True),
    ]
)


DOMAIN_SCHEMAS = {
    "products": PRODUCTS_SCHEMA,
    "users": USERS_SCHEMA,
    "carts": CARTS_SCHEMA,
}


# --------------------------------------------------------------------------- #
#  Clés naturelles — utilisées pour le dédoublonnage Silver et les MERGE
# --------------------------------------------------------------------------- #
DOMAIN_BUSINESS_KEYS = {
    "products": ["id"],
    "users": ["id"],
    "carts": ["id"],
}


# Colonnes techniques ajoutées par la couche Bronze à tous les domaines.
# Préfixées par « _ » pour être immédiatement distinguables des champs métier.
BRONZE_TECHNICAL_COLUMNS = [
    "_ingest_date",     # date logique du snapshot (partition, vient de NiFi)
    "_ingest_ts",       # horodatage réel de l'appel API par NiFi
    "_bronze_ts",       # horodatage d'écriture Bronze par Spark
    "_source_file",     # objet MinIO d'origine — traçabilité complète
    "_batch_id",        # identifiant du run Spark
    "_record_hash",     # empreinte du contenu métier (détection de changement)
    "_is_valid",        # résultat du contrôle de conformité
    "_reject_reason",   # motif de rejet le cas échéant
]
