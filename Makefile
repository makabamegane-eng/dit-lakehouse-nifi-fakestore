# =============================================================================
#  Plateforme Data Lakehouse — DIT M2 Ingénierie des Données
#  Point d'entrée unique de toutes les opérations sur la plateforme.
#  `make aide` liste les cibles disponibles.
# =============================================================================

SHELL := /bin/bash
COMPOSE := docker compose
PY := python

.DEFAULT_GOAL := aide
.PHONY: aide up down restart build logs ps init nifi-flow nifi-export dremio-setup \
        backfill ingest medaillon dremio-test demo-iceberg maintenance qualite \
        demo-complete urls clean reset spark-shell dremio-sql verifier prerequis

# --------------------------------------------------------------------------- #
aide:  ## Affiche cette aide
	@echo ""
	@echo "  PLATEFORME DATA LAKEHOUSE — commandes disponibles"
	@echo "  ================================================="
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "  Démarrage complet depuis zéro :  make demarrage-complet"
	@echo ""

# ------------------------------------------------------- Cycle de vie ------ #
build:  ## Construit les images custom (Spark et Airflow)
	$(COMPOSE) build

up:  ## Démarre l'ensemble des services
	$(COMPOSE) up -d
	@echo "Services démarrés. `make urls` pour les interfaces."

down:  ## Arrête les services (les volumes sont conservés)
	$(COMPOSE) down

restart:  ## Redémarre les services
	$(COMPOSE) restart

ps:  ## État des conteneurs
	$(COMPOSE) ps

logs:  ## Suit les journaux (make logs S=nifi pour un seul service)
	$(COMPOSE) logs -f $(S)

urls:  ## Rappelle les interfaces web et leurs identifiants
	@echo ""
	@echo "  MinIO console ....... http://localhost:9001   lakehouse / lakehouse123"
	@echo "  Apache NiFi ......... http://localhost:8080/nifi        (HTTP, sans auth)"
	@echo "  Airflow ............. http://localhost:8085   admin / admin"
	@echo "  Spark master ........ http://localhost:8090"
	@echo "  Dremio .............. http://localhost:9047   dremio / dremio123"
	@echo "  Nessie API .......... http://localhost:19120/api/v2/config"
	@echo "  Prometheus .......... http://localhost:9090"
	@echo "  Grafana ............. http://localhost:3001   admin / admin"
	@echo ""

# ------------------------------------------------------ Initialisation ----- #
nifi-flow:  ## Construit le dataflow NiFi via l'API REST
	$(PY) nifi/scripts/build_flow.py

nifi-export:  ## Exporte le flow NiFi en .json (livrable du sujet)
	$(PY) nifi/scripts/export_flow.py

dremio-setup:  ## Déclare la source Nessie et les vues dans Dremio
	$(PY) dremio/scripts/setup_dremio.py

init: nifi-flow dremio-setup  ## Configure NiFi et Dremio après le premier `up`

# ------------------------------------------------------------ Pipeline ----- #
backfill:  ## Reconstitue les 6 mois d'historique puis construit le médaillon
	$(COMPOSE) exec airflow-scheduler airflow dags unpause fakestore_backfill_history
	$(COMPOSE) exec airflow-scheduler airflow dags unpause lakehouse_medallion
	$(COMPOSE) exec airflow-scheduler airflow dags trigger fakestore_backfill_history
	@echo "Backfill déclenché — suivi dans l'interface Airflow (http://localhost:8085)"

ingest:  ## Déclenche une ingestion NiFi pour aujourd'hui
	$(COMPOSE) exec airflow-scheduler airflow dags trigger fakestore_daily_ingestion

medaillon:  ## Reconstruit Bronze -> Silver -> Gold
	$(COMPOSE) exec airflow-scheduler airflow dags trigger lakehouse_medallion

qualite:  ## Rejoue les contrôles qualité seuls
	$(COMPOSE) exec spark-master /opt/spark/bin/spark-submit \
	  --master spark://spark-master:7077 \
	  /opt/spark-jobs/maintenance/data_quality_checks.py

maintenance:  ## Compacte les tables Iceberg (sans purger les snapshots)
	$(COMPOSE) exec spark-master /opt/spark/bin/spark-submit \
	  --master spark://spark-master:7077 \
	  /opt/spark-jobs/maintenance/iceberg_maintenance.py

demo-iceberg:  ## Démonstration time travel / schema evolution / branches Nessie
	$(COMPOSE) exec spark-master /opt/spark/bin/spark-submit \
	  --master spark://spark-master:7077 \
	  /opt/spark-jobs/maintenance/demo_iceberg_features.py

# ------------------------------------------------------------- Requêtes ---- #
dremio-test:  ## Joue les requêtes de validation Dremio (Partie 5 du sujet)
	$(PY) dremio/scripts/run_validation.py

dremio-sql:  ## Joue une requête précise, ex. make dremio-sql Q=Q4
	$(PY) dremio/scripts/run_validation.py --only $(Q) --rows 25

spark-shell:  ## Ouvre un spark-sql connecté au catalogue Nessie
	$(COMPOSE) exec spark-master /opt/spark/bin/spark-sql \
	  --master spark://spark-master:7077

# --------------------------------------------------------- Vérification ---- #
prerequis:  ## Contrôle les prérequis AVANT le premier démarrage
	$(PY) scripts/verifier_prerequis.py

verifier:  ## Contrôle statique du projet (syntaxe Python, YAML, SQL)
	$(PY) scripts/verifier_projet.py

# ------------------------------------------------------------- Nettoyage --- #
clean:  ## Arrête et supprime les conteneurs (volumes conservés)
	$(COMPOSE) down --remove-orphans

reset:  ## DESTRUCTIF — supprime aussi les volumes (données perdues)
	$(COMPOSE) down -v --remove-orphans
	@echo "Plateforme remise à zéro. `make demarrage-complet` pour repartir."

# ------------------------------------------------------ Parcours guidé ----- #
.PHONY: demarrage-complet
demarrage-complet:  ## Enchaîne build -> up -> init -> backfill (première mise en route)
	@$(MAKE) prerequis
	@$(MAKE) build
	@$(MAKE) up
	@echo "Attente de la disponibilité des services (90 s)…"
	@sleep 90
	@$(MAKE) nifi-flow
	@$(MAKE) dremio-setup
	@$(MAKE) backfill
	@$(MAKE) urls
