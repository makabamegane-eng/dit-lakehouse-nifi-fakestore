#!/usr/bin/env bash
# =============================================================================
#  Initialisation Airflow — exécutée une seule fois par le service airflow-init
#  (les services webserver/scheduler démarrent ensuite via depends_on:
#   condition: service_completed_successfully).
#
#  Rôle :
#    1. préparer l'arborescence et les droits sur les volumes montés ;
#    2. migrer la base de métadonnées PostgreSQL ;
#    3. créer l'utilisateur admin ;
#    4. déclarer les connexions Airflow utilisées par les DAGs
#       (Spark, MinIO/S3, NiFi, Dremio) — aucune connexion n'est donc à créer
#       à la main dans l'UI : la plateforme est reproductible « from scratch ».
# =============================================================================
set -euo pipefail

echo "==> [airflow-init] préparation des répertoires"
mkdir -p /opt/airflow/{logs,dags,plugins,config}
chown -R "${AIRFLOW_UID:-50000}:0" /opt/airflow/{logs,dags,plugins,config} || true

echo "==> [airflow-init] attente de PostgreSQL"
for i in $(seq 1 60); do
  if nc -z postgres 5432; then echo "    PostgreSQL est joignable"; break; fi
  echo "    tentative $i/60..."; sleep 2
done

echo "==> [airflow-init] migration de la base de métadonnées"
runuser -u airflow -- airflow db migrate

echo "==> [airflow-init] création de l'utilisateur admin"
runuser -u airflow -- airflow users create \
  --username "${_AIRFLOW_WWW_USER_USERNAME:-admin}" \
  --password "${_AIRFLOW_WWW_USER_PASSWORD:-admin}" \
  --firstname DIT --lastname Lakehouse \
  --role Admin --email admin@dit.local || echo "    (utilisateur déjà existant)"

echo "==> [airflow-init] déclaration des connexions"

add_conn () {
  local conn_id="$1"; shift
  runuser -u airflow -- airflow connections delete "${conn_id}" >/dev/null 2>&1 || true
  runuser -u airflow -- airflow connections add "${conn_id}" "$@"
}

# Cluster Spark standalone — utilisé par SparkSubmitOperator
add_conn spark_default \
  --conn-type spark \
  --conn-host "spark://spark-master" \
  --conn-port 7077 \
  --conn-extra '{"queue": "default", "deploy-mode": "client"}'

# MinIO vu comme un endpoint S3 — utilisé par les capteurs de la zone brute
add_conn minio_s3 \
  --conn-type aws \
  --conn-login "${AWS_ACCESS_KEY_ID}" \
  --conn-password "${AWS_SECRET_ACCESS_KEY}" \
  --conn-extra "{\"endpoint_url\": \"${MINIO_ENDPOINT}\", \"region_name\": \"${AWS_REGION:-us-east-1}\"}"

# NiFi — API REST (pilotage du flow) et endpoint ListenHTTP (déclenchement)
add_conn nifi_api \
  --conn-type http \
  --conn-host nifi \
  --conn-port 8080 \
  --conn-schema http

add_conn nifi_ingest \
  --conn-type http \
  --conn-host nifi \
  --conn-port "${NIFI_LISTEN_HTTP_PORT:-9095}" \
  --conn-schema http

# Dremio — API REST (exécution des requêtes de validation en fin de pipeline)
add_conn dremio_api \
  --conn-type http \
  --conn-host dremio \
  --conn-port 9047 \
  --conn-schema http \
  --conn-login "${DREMIO_USER:-dremio}" \
  --conn-password "${DREMIO_PASSWORD:-dremio123}"

echo "==> [airflow-init] terminé"
