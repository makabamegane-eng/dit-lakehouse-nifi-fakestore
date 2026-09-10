"""Sélection incrémentale des partitions entre deux couches du médaillon.

Le même raisonnement qu'entre la zone brute et Bronze est réappliqué entre
Bronze et Silver, puis entre Silver et Gold : on compare l'ensemble des dates
présentes en amont à celles déjà matérialisées en aval, et on ne retraite que
la différence. Combiné à `overwritePartitions()`, cela donne un pipeline à la
fois incrémental (rapide au quotidien) et rejouable (un `--mode full` suffit à
tout reconstruire à l'identique).
"""

from __future__ import annotations

import logging
from typing import List, Sequence

from pyspark.sql import SparkSession

from common.session import table_exists


def distinct_dates(spark: SparkSession, table: str, column: str) -> List[str]:
    """Dates distinctes présentes dans une table Iceberg (liste vide si absente)."""
    if not table_exists(spark, table):
        return []
    rows = spark.sql(
        f"SELECT DISTINCT {column} AS d FROM {table} WHERE {column} IS NOT NULL ORDER BY d"
    ).collect()
    return [str(r["d"]) for r in rows]


def pending_dates(
    spark: SparkSession,
    source_table: str,
    source_column: str,
    target_table: str,
    target_column: str,
    mode: str,
    explicit_dates: Sequence[str] | None,
    logger: logging.Logger,
    limit: int | None = None,
) -> List[str]:
    """Partitions à traiter pour passer de `source_table` à `target_table`."""
    upstream = distinct_dates(spark, source_table, source_column)
    if not upstream:
        logger.warning("Table amont %s vide ou inexistante", source_table)
        return []

    if mode == "explicit":
        wanted = set(explicit_dates or [])
        selected = [d for d in upstream if d in wanted]
    elif mode == "full":
        selected = upstream
    else:  # incremental
        done = set(distinct_dates(spark, target_table, target_column))
        selected = [d for d in upstream if d not in done]

    if limit is not None and limit > 0:
        selected = selected[:limit]

    logger.info(
        "%s → %s | mode=%s | %d date(s) amont | %d à traiter%s",
        source_table.split(".")[-1],
        target_table.split(".")[-1],
        mode,
        len(upstream),
        len(selected),
        f" ({selected[0]} → {selected[-1]})" if selected else "",
    )
    return selected


def sql_date_list(dates: Sequence[str]) -> str:
    """Formate une liste de dates pour une clause `IN (...)` SQL."""
    return ", ".join(f"date('{d}')" for d in dates)


def anchor_date(spark: SparkSession, table: str, column: str, fallback: Sequence[str]) -> str:
    """Date la plus récente connue en amont — ancre du modèle d'historisation.

    Ancrer sur la donnée et non sur `today` rend le résultat des jobs Silver
    stable dans le temps : rejouer le pipeline trois semaines plus tard
    reproduit exactement les mêmes valeurs historisées.
    """
    if table_exists(spark, table):
        row = spark.sql(f"SELECT max({column}) AS d FROM {table}").collect()
        if row and row[0]["d"] is not None:
            return str(row[0]["d"])
    return max(fallback) if fallback else ""
