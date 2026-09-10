"""Écriture normalisée des tables Iceberg de la couche Gold.

Choix assumé pour Gold : **recalcul complet à chaque exécution**
(`createOrReplace`) plutôt qu'un MERGE incrémental.

Justification :
  * le volume Gold est agrégé — quelques milliers de lignes, quelques secondes
    de calcul. L'incrémental n'apporterait aucun gain mesurable ;
  * un agrégat recalculé intégralement ne peut pas dériver de la vérité Silver,
    alors qu'un MERGE mal ordonné le peut ;
  * Iceberg conserve l'historique : `createOrReplace` produit un NOUVEAU
    snapshot, il n'efface rien. Les requêtes de time travel (`VERSION AS OF`,
    `TIMESTAMP AS OF`) restent parfaitement fonctionnelles — c'est d'ailleurs
    ce mécanisme qui est démontré en partie bonus.

Bronze et Silver, eux, restent incrémentaux (`overwritePartitions`) : c'est là
que se trouve le volume et la valeur d'un traitement partitionné.
"""

from __future__ import annotations

import logging

from pyspark.sql import Column, DataFrame

DEFAULT_PROPERTIES = {
    "format-version": "2",
    "write.parquet.compression-codec": "zstd",
    "write.distribution-mode": "hash",
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "50",
}


def replace_table(
    df: DataFrame,
    table: str,
    comment: str,
    logger: logging.Logger,
    partition_by: Column | None = None,
    sort_columns: list[str] | None = None,
) -> None:
    """Remplace intégralement une table Gold par un nouveau snapshot Iceberg."""
    frame = df.sortWithinPartitions(*sort_columns) if sort_columns else df
    writer = frame.writeTo(table).using("iceberg")

    for key, value in DEFAULT_PROPERTIES.items():
        writer = writer.tableProperty(key, value)
    writer = writer.tableProperty("comment", comment)

    if partition_by is not None:
        writer = writer.partitionedBy(partition_by)

    writer.createOrReplace()
    logger.info("Table Gold %s remplacée (nouveau snapshot Iceberg)", table)
