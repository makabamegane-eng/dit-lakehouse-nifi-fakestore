#!/usr/bin/env python3
"""COUCHE SILVER — dimension client conformée.

------------------------------------------------------------------------------
 RÈGLES DE TRANSFORMATION BRONZE → SILVER (domaine clients)
------------------------------------------------------------------------------
 1. FILTRE QUALITÉ    — `_is_valid` uniquement (id présent, email plausible).
 2. DÉDOUBLONNAGE     — une ligne par (customer_id, snapshot_date).
 3. APLATISSEMENT     — `name.*` et `address.*` (deux niveaux d'imbrication)
                        deviennent des colonnes scalaires : Dremio et les
                        outils BI n'ont plus à naviguer dans des structures.
 4. TYPAGE GÉO        — l'API renvoie latitude/longitude en STRING. On les
                        convertit en DOUBLE et on valide les bornes
                        (-90/90, -180/180) : une coordonnée hors bornes est
                        neutralisée plutôt que propagée.
 5. NORMALISATION     — casse harmonisée (villes en Initiales, e-mails en
                        minuscules), téléphone réduit à ses chiffres pour
                        permettre un rapprochement fiable.
 6. MINIMISATION RGPD — le mot de passe (déjà haché en Bronze) est écarté ;
                        une version pseudonymisée de l'e-mail est fournie pour
                        les usages analytiques qui n'ont pas besoin du réel.
 7. ENRICHISSEMENT    — domaine de messagerie, complétude de l'adresse,
                        segment géographique.

 Note d'historisation : contrairement aux produits, aucune dérive synthétique
 n'est appliquée aux clients. Un référentiel client est par nature stable ; y
 injecter du bruit n'aurait aucun sens métier. La dimension temporelle vient
 uniquement de la répétition réelle des snapshots.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from common.config import BRONZE_TABLES, SETTINGS, SILVER_TABLES  # noqa: E402
from common.incremental import pending_dates, sql_date_list  # noqa: E402
from common.session import build_spark, get_logger, log_table_stats, table_exists  # noqa: E402

LOG = get_logger("silver.customers")

BRONZE_TABLE = SETTINGS.bronze(BRONZE_TABLES["users"])
SILVER_TABLE = SETTINGS.silver(SILVER_TABLES["users"])


def transform(spark: SparkSession, dates: list[str]) -> DataFrame:
    return spark.sql(
        f"""
        WITH filtre_qualite AS (
            SELECT *
            FROM {BRONZE_TABLE}
            WHERE _ingest_date IN ({sql_date_list(dates)})
              AND _is_valid = true
        ),
        dedoublonnage AS (
            SELECT *,
                   row_number() OVER (
                       PARTITION BY id, _ingest_date
                       ORDER BY _ingest_ts DESC, _source_file DESC
                   ) AS rang
            FROM filtre_qualite
        ),
        aplatissement AS (
            SELECT
                id                                        AS customer_id,
                _ingest_date                              AS snapshot_date,
                lower(trim(email))                        AS email,
                lower(trim(username))                     AS username,
                initcap(trim(name.firstname))             AS first_name,
                initcap(trim(name.lastname))              AS last_name,
                initcap(trim(address.city))               AS city,
                initcap(trim(address.street))             AS street,
                address.number                            AS street_number,
                trim(address.zipcode)                     AS zip_code,
                cast(address.geolocation.lat  AS double)  AS latitude_raw,
                cast(address.geolocation.long AS double)  AS longitude_raw,
                regexp_replace(coalesce(phone, ''), '[^0-9]', '')  AS phone_digits,
                phone                                     AS phone_raw,
                _ingest_ts                                AS source_ingest_ts,
                _record_hash                              AS source_record_hash
            FROM dedoublonnage
            WHERE rang = 1
        )
        SELECT
            sha2(concat_ws('|', 'customer', cast(customer_id AS string),
                           cast(snapshot_date AS string)), 256)  AS customer_sk,
            customer_id,
            snapshot_date,

            email,
            -- Pseudonymisation : conserve la structure (initiale + domaine)
            -- sans exposer l'identité. Utilisable en démo ou en exposition BI.
            concat(substring(email, 1, 1), '***@', split(email, '@')[1])
                                                            AS email_masked,
            split(email, '@')[1]                            AS email_domain,
            username,
            first_name,
            last_name,
            concat_ws(' ', first_name, last_name)           AS full_name,

            city,
            street,
            street_number,
            zip_code,
            concat_ws(' ', cast(street_number AS string), street, zip_code, city)
                                                            AS full_address,

            -- Coordonnées validées : hors bornes => NULL + drapeau explicite
            CASE WHEN latitude_raw BETWEEN -90 AND 90
                 THEN latitude_raw END                      AS latitude,
            CASE WHEN longitude_raw BETWEEN -180 AND 180
                 THEN longitude_raw END                     AS longitude,
            (latitude_raw BETWEEN -90 AND 90
             AND longitude_raw BETWEEN -180 AND 180)        AS has_valid_geolocation,

            phone_digits,
            phone_raw,
            (length(phone_digits) >= 10)                    AS has_valid_phone,

            (city IS NOT NULL AND zip_code IS NOT NULL
             AND street IS NOT NULL)                        AS has_complete_address,

            source_ingest_ts,
            source_record_hash,
            current_timestamp()                             AS silver_ts
        FROM aplatissement
        """
    )


def write_silver(spark: SparkSession, df: DataFrame) -> None:
    writer = (
        df.sortWithinPartitions("snapshot_date", "customer_id")
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
                "Couche Silver - dimension client conformee et minimisee (RGPD)",
            )
            .partitionedBy(F.col("snapshot_date"))
            .create()
        )
    else:
        writer.overwritePartitions()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Job Silver — dimension clients")
    p.add_argument("--mode", default="incremental", choices=["incremental", "full", "explicit"])
    p.add_argument("--dates", default="")
    p.add_argument("--max-partitions", type=int, default=0)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    spark = build_spark("silver-customers")
    LOG.info("=== SILVER clients | %s → %s ===", BRONZE_TABLE, SILVER_TABLE)

    explicit = [d.strip() for d in args.dates.split(",") if d.strip()]
    mode = "explicit" if explicit else args.mode

    dates = pending_dates(
        spark,
        source_table=BRONZE_TABLE,
        source_column="_ingest_date",
        target_table=SILVER_TABLE,
        target_column="snapshot_date",
        mode=mode,
        explicit_dates=explicit,
        logger=LOG,
        limit=args.max_partitions or None,
    )
    if not dates:
        LOG.info("Silver clients déjà à jour")
        spark.stop()
        return 0

    df = transform(spark, dates)
    LOG.info("%d ligne(s) produites", df.count())
    write_silver(spark, df)
    log_table_stats(spark, SILVER_TABLE, LOG)

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
