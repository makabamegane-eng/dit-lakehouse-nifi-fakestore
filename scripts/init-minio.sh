#!/bin/sh
# =============================================================================
#  Initialisation MinIO — création des deux zones du lakehouse.
# -----------------------------------------------------------------------------
#  Contrainte du sujet (3.1.2) : la zone brute alimentée par NiFi doit être
#  « clairement séparée du bucket warehouse Iceberg ».
#  Choix retenu : DEUX BUCKETS distincts plutôt que deux préfixes d'un même
#  bucket, afin de pouvoir appliquer des politiques différentes :
#     * lakehouse-raw       : rétention courte, écriture NiFi, lecture Spark
#     * lakehouse-warehouse : données de référence, versionnement activé
#
#  Convention de nommage des clés dans la zone brute (conçue ici, pas imposée) :
#     fakestore/<domaine>/ingest_date=YYYY-MM-DD/<domaine>_<ts>_<uuid>.json
#  Le style Hive `ingest_date=` permet à Spark de découvrir la partition
#  temporelle directement à la lecture, sans parsing de nom de fichier.
# =============================================================================
set -e

RAW_BUCKET="${RAW_BUCKET:-lakehouse-raw}"
WAREHOUSE_BUCKET="${WAREHOUSE_BUCKET:-lakehouse-warehouse}"

echo "==> [minio-init] connexion au serveur MinIO"
until mc alias set local http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null 2>&1; do
  echo "    MinIO pas encore prêt, nouvelle tentative dans 3s..."
  sleep 3
done

echo "==> [minio-init] création des buckets"
mc mb --ignore-existing "local/${RAW_BUCKET}"
mc mb --ignore-existing "local/${WAREHOUSE_BUCKET}"

echo "==> [minio-init] versionnement du warehouse Iceberg"
# Filet de sécurité au niveau objet, en complément du versionnement logique
# assuré par Nessie au niveau du catalogue.
mc version enable "local/${WAREHOUSE_BUCKET}" || true

echo "==> [minio-init] création de la structure logique de la zone brute"
# Marqueurs de préfixes : rendent la structure visible dans la console MinIO
# avant même la première ingestion NiFi.
for domain in products users carts; do
  echo "zone brute FakeStoreAPI - domaine ${domain}" > /tmp/_README
  mc cp /tmp/_README "local/${RAW_BUCKET}/fakestore/${domain}/_README.txt" >/dev/null
done
echo "flowfiles rejetes par le controle NiFi" > /tmp/_README
mc cp /tmp/_README "local/${RAW_BUCKET}/_dead_letter/_README.txt" >/dev/null
rm -f /tmp/_README

echo "==> [minio-init] rétention de 90 jours sur la zone brute"
# La zone brute est un tampon : une fois la couche Bronze construite (elle-même
# immuable et versionnée par Iceberg/Nessie), conserver les JSON indéfiniment
# n'apporte rien. On garde 90 jours pour pouvoir rejouer une ingestion.
cat > /tmp/lifecycle.json <<'JSON'
{
  "Rules": [
    {
      "ID": "expire-raw-json-after-90-days",
      "Status": "Enabled",
      "Filter": { "Prefix": "fakestore/" },
      "Expiration": { "Days": 90 }
    }
  ]
}
JSON
mc ilm import "local/${RAW_BUCKET}" < /tmp/lifecycle.json || \
  echo "    (politique de cycle de vie non appliquée — non bloquant)"
rm -f /tmp/lifecycle.json

echo "==> [minio-init] état final"
mc ls local/
echo "==> [minio-init] terminé"
