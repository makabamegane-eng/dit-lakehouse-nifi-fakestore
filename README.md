# Plateforme Data Lakehouse — FakeStoreAPI → NiFi → MinIO → Iceberg → Dremio

> **DIT — Dakar Institute of Technologies**
> Master 2 Ingénierie des Données / Big Data
> Module « Architectures Data Lakehouse & Ingestion de données » — M. PENE
> Examen individuel — travail à domicile

Plateforme lakehouse conteneurisée complète : ingestion Apache NiFi depuis une
API publique vers MinIO, médaillon Bronze/Silver/Gold en tables Apache Iceberg
cataloguées par Project Nessie, orchestration Apache Airflow, requêtage SQL
Dremio, supervision Prometheus/Grafana.

| Document | Contenu |
|---|---|
| **Ce README** | comment lancer la plateforme, pas à pas |
| [`docs/architecture.md`](docs/architecture.md) | **pourquoi** chaque choix technique — le fond de la soutenance |
| [`docs/scenario_video.md`](docs/scenario_video.md) | conducteur de la vidéo de 15 minutes |
| `slides/soutenance_lakehouse_nifi.pptx` | support projeté, avec notes de présentateur |

---

# PARTIE I — LANCER LA PLATEFORME

## 0. Ce qu'il faut, et ce qu'il ne faut PAS installer

**Aucun cloud, aucun compte, aucun abonnement.** Tout tourne sur votre poste.

### À installer (une seule fois)

| Outil | Version | Pourquoi |
|---|---|---|
| **Docker Desktop** | 24+ avec Compose v2 | fait tourner les 16 conteneurs |
| **Python** | 3.9+ | scripts d'administration (NiFi, Dremio, vérifications) |

```bash
pip install requests pyyaml python-pptx
```

### À NE PAS installer

NiFi, Spark, Iceberg, Nessie, Dremio, Airflow, MinIO, PostgreSQL, Java, Scala,
Hadoop — **rien de tout cela ne s'installe sur votre machine**. Ce sont des
images Docker que `docker compose` télécharge et isole. Votre système reste
propre, et `make reset` efface tout sans laisser de trace.

### Dimensionnement

| Ressource | Minimum | Confortable |
|---|---|---|
| RAM allouée à Docker | 9 Gio | **14 à 20 Gio** |
| Processeurs | 4 | 6 à 8 |
| Disque libre | 25 Gio | 40 Gio |

Le seul service réellement gourmand est Dremio (~4 Gio à lui seul), suivi des
workers Spark.

---

## 1. Vérifier les prérequis

Avant toute chose. Onze conteneurs qui refusent de démarrer sont pénibles à
diagnostiquer après coup ; ce script dit en dix secondes ce qui manque.

```bash
python scripts/verifier_prerequis.py
```

Il contrôle : Docker installé et **démarré**, mémoire et CPU alloués, espace
disque (sur le volume du projet **et** sur `C:` où vivent les images sous
Windows), modules Python, les 19 ports hôte, et l'accès à FakeStoreAPI.

Tant qu'il affiche un point **BLOQUANT**, ne lancez pas la suite : il indique
la correction exacte.

### Corriger : « Le moteur Docker ne répond pas »

Ouvrez Docker Desktop et attendez que l'icône de la baleine passe au vert
(30 à 60 secondes). Puis relancez le script.

### Corriger : « Mémoire insuffisante » (Windows / WSL2)

C'est le point qui bloque le plus souvent. Docker Desktop sous Windows tourne
dans WSL2, dont la mémoire se règle dans `%USERPROFILE%\.wslconfig` :

```ini
[wsl2]
memory=20GB
processors=8
swap=8GB
```

Puis, dans PowerShell :

```bash
wsl --shutdown
```

Redémarrez ensuite Docker Desktop. `docker info` doit maintenant afficher la
nouvelle valeur.

> Sur macOS ou Linux, le même réglage se fait dans
> *Docker Desktop → Settings → Resources*.

### Corriger : « Port déjà utilisé »

Un autre conteneur ou une autre application occupe un port. Deux options :
arrêter le service concerné, ou changer le port hôte dans
`docker-compose.yml` — seul le nombre **de gauche** dans `"3001:3000"` doit
être modifié.

---

## 2. Construire les images

```bash
docker compose build
```

**Durée : 20 à 35 minutes la première fois.** C'est long, et c'est normal :
deux images sont construites sur mesure.

| Image | Contenu |
|---|---|
| `dit-lakehouse/spark:3.5.1` | Spark 3.5.1 + Iceberg 1.5.2 + extensions Nessie 0.77.1 + connecteur S3A |
| `dit-lakehouse/airflow:2.9.3` | Airflow 2.9.3 + JRE 17 + distribution Spark complète (pour `spark-submit`) |

Les JARs sont **embarqués dans les images** plutôt que résolus par `--packages`
à chaque exécution : le pipeline devient reproductible et fonctionne ensuite
sans accès Internet.

C'est la seule étape qui télécharge massivement (~10 Gio). Tout est ensuite en
cache : les démarrages suivants prennent quelques secondes.

> **Point de contrôle** — `docker images` doit lister `dit-lakehouse/spark` et
> `dit-lakehouse/airflow`.

---

## 3. Démarrer les services

```bash
docker compose up -d
```

Les seize conteneurs démarrent dans un ordre imposé par les `healthcheck` :

```
  postgres ─┬─> nessie ──────┬─> spark-master ──> spark-worker-1
            │                │                    spark-worker-2
            └─> airflow-init │
                    └─> airflow-webserver
                        airflow-scheduler
  minio ────┬─> minio-init
            ├─> nifi
            └─> dremio
  prometheus, grafana, node-exporter, cadvisor
```

**Durée : 2 à 5 minutes** avant que tout soit `healthy`. NiFi est le plus lent
(60 à 90 secondes rien que pour son démarrage interne).

Surveillez la montée en charge :

```bash
docker compose ps
```

> **Point de contrôle** — tous les services affichent `running (healthy)` ou
> `running`. `minio-init` et `airflow-init` doivent afficher `exited (0)` : ce
> sont des conteneurs éphémères, leur travail est terminé.

En cas de doute sur un service :

```bash
docker compose logs -f nifi
```

---

## 4. Construire le flow NiFi

Les services tournent, mais NiFi est vide. Ce script crée l'intégralité du
dataflow via l'API REST : contexte de paramètres, services de contrôle,
12 processeurs, connexions, gestion des erreurs, et la tâche de reporting
Prometheus.

```bash
python nifi/scripts/build_flow.py
```

**Durée : environ 1 minute.** Le script attend lui-même que NiFi soit prêt.

> **Point de contrôle** — ouvrez http://localhost:8080/nifi : un Process Group
> « FakeStoreAPI Ingestion » apparaît, tous ses processeurs sont démarrés sauf
> `10. notifier-airflow`, laissé volontairement à l'arrêt (c'est l'option B du
> sujet, gardée pour la démonstration).

Le flow est reconstructible à l'identique autant de fois que nécessaire : le
script supprime l'ancien groupe avant de recréer le nouveau.

---

## 5. Configurer Dremio

```bash
python dremio/scripts/setup_dremio.py
```

Crée le premier utilisateur, déclare la source Nessie (branchée à la fois sur
le catalogue **et** sur MinIO), et publie trois vues métier.

**Durée : environ 1 minute.**

> **Point de contrôle** — http://localhost:9047, connexion `dremio` /
> `dremio123`. Une source `lakehouse` apparaît dans le panneau de gauche. Elle
> est encore vide : les tables n'existent pas tant que le médaillon n'a pas
> tourné, c'est normal.

---

## 6. Alimenter le lakehouse

C'est ici que la plateforme prend vie. Le backfill reconstitue les 6 mois
d'historique en rejouant l'ingestion NiFi date par date, puis le médaillon
construit les dix tables Iceberg.

```bash
docker compose exec airflow-scheduler airflow dags unpause lakehouse_medallion
```

```bash
docker compose exec airflow-scheduler airflow dags unpause fakestore_backfill_history
```

```bash
docker compose exec airflow-scheduler airflow dags trigger fakestore_backfill_history
```

**Durée : 15 à 25 minutes.** Suivez la progression sur http://localhost:8085
(`admin` / `admin`) :

1. `preparer_fenetre` — calcule les 180 dates à ingérer (quelques secondes)
2. `ingerer_historique` — 180 déclenchements NiFi, progression journalisée tous
   les 10 % (5 à 10 minutes)
3. `verifier_couverture` — contrôle qu'au moins 95 % de la fenêtre est couverte
4. `construire_medaillon` — déclenche `lakehouse_medallion`, qui enchaîne
   3 jobs Bronze, 3 Silver, 4 Gold, puis les contrôles qualité

> **Point de contrôle** — dans la console MinIO (http://localhost:9001,
> `lakehouse` / `lakehouse123`), le bucket `lakehouse-raw` contient
> `fakestore/products/ingest_date=…` avec ~180 partitions, et
> `lakehouse-warehouse` s'est rempli de fichiers Parquet.

### Pour aller plus vite pendant les essais

Le backfill accepte des paramètres au déclenchement (*Trigger DAG w/ config*
dans Airflow). Pour un premier essai en 2 minutes plutôt qu'en 20 :

```json
{ "history_days": 20 }
```

Vous relancerez le DAG complet ensuite : les dates déjà ingérées sont
automatiquement sautées.

---

## 7. Valider que tout fonctionne

```bash
python dremio/scripts/run_validation.py
```

Joue les dix requêtes SQL de la Partie 5 du sujet — une par couche, deux
jointures inter-domaines, deux agrégations, plus le time travel — affiche les
résultats et rend un verdict.

> **Point de contrôle** — `BILAN : 10/10 requête(s) exécutée(s) avec succès`.

À ce stade la plateforme est complète et démontrable.

---

## 8. Tout enchaîner d'une commande

Une fois les prérequis validés, les étapes 2 à 6 se lancent d'un coup :

```bash
make demarrage-complet
```

Sous Windows sans `make` :

```bash
.\scripts\plateforme.ps1 demarrage-complet
```

---

## Récapitulatif du premier démarrage

| # | Commande | Durée | Point de contrôle |
|---|---|---|---|
| 1 | `python scripts/verifier_prerequis.py` | 10 s | aucun point bloquant |
| 2 | `docker compose build` | 20-35 min | 2 images `dit-lakehouse/*` |
| 3 | `docker compose up -d` | 2-5 min | tous les services `healthy` |
| 4 | `python nifi/scripts/build_flow.py` | 1 min | Process Group démarré dans NiFi |
| 5 | `python dremio/scripts/setup_dremio.py` | 1 min | source `lakehouse` visible |
| 6 | `airflow dags trigger fakestore_backfill_history` | 15-25 min | ~180 partitions dans MinIO |
| 7 | `python dremio/scripts/run_validation.py` | 1 min | 10/10 requêtes au vert |

**Total : 45 minutes à 1 heure**, dont l'essentiel en téléchargement. Les
démarrages suivants prennent 3 minutes (`docker compose up -d`), les données
étant conservées dans les volumes.

---

## Interfaces

| Service | URL | Identifiants |
|---|---|---|
| MinIO (console) | http://localhost:9001 | `lakehouse` / `lakehouse123` |
| Apache NiFi | http://localhost:8080/nifi | HTTP simplifié, sans authentification |
| Apache Airflow | http://localhost:8085 | `admin` / `admin` |
| Spark master | http://localhost:8090 | — |
| Dremio | http://localhost:9047 | `dremio` / `dremio123` |
| Nessie (API) | http://localhost:19120/api/v2/config | — |
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3001 | `admin` / `admin` |

`make urls` (ou `.\scripts\plateforme.ps1 urls`) réaffiche cette liste.

Déclencher une ingestion sans passer par Airflow :

```bash
curl -X POST http://localhost:9095/ingest -H "Content-Type: application/json" -d "{\"snapshot_date\":\"2026-09-10\",\"domains\":[\"products\",\"users\",\"carts\"]}"
```

---

## Usage quotidien

| Commande | Effet |
|---|---|
| `make up` / `make down` | démarre / arrête (les données sont conservées) |
| `make ps` | état des conteneurs |
| `make logs S=nifi` | journaux d'un service |
| `make ingest` | une ingestion NiFi pour aujourd'hui |
| `make medaillon` | reconstruit Bronze → Silver → Gold |
| `make dremio-test` | joue les 10 requêtes de validation |
| `make dremio-sql Q=Q4` | joue une requête précise |
| `make demo-iceberg` | time travel, schema evolution, branches Nessie |
| `make qualite` | rejoue les contrôles qualité |
| `make maintenance` | compacte les tables Iceberg |
| `make nifi-export` | exporte le flow NiFi en `.json` (livrable du sujet) |
| `make verifier` | contrôle statique du projet |
| `make slides` | régénère le support de soutenance |
| `make reset` | **destructif** — supprime les volumes et les données |

Sous Windows : `.\scripts\plateforme.ps1 <action>` — mêmes noms d'actions,
`aide` pour la liste. Sans ni `make` ni PowerShell, chaque cible correspond à
une commande lisible directement dans le [`Makefile`](Makefile).

---

## Machine limitée (moins de 12 Gio pour Docker)

La plateforme tourne quand même, en dégradant trois réglages.

**1.** Supprimer le second worker Spark : commentez le bloc `spark-worker-2`
dans `docker-compose.yml`.

**2.** Réduire les allocations mémoire dans `.env` :

```ini
SPARK_WORKER_MEMORY=1500m
SPARK_EXECUTOR_MEMORY=1g
SPARK_DRIVER_MEMORY=768m
NIFI_JVM_HEAP_MAX=1g
```

**3.** Raccourcir la fenêtre d'historisation dans `.env` :

```ini
HISTORY_MONTHS=3
```

Le pipeline reste complet et démontrable ; il est simplement plus lent, et
l'historique couvre 90 jours au lieu de 180.

---

## Dépannage

**Un service reste `unhealthy`.**
`docker compose logs <service>`. Les `healthcheck` indiquent précisément quelle
brique bloque la chaîne — inutile de chercher plus loin dans le graphe.

**`build_flow.py` échoue sur une propriété inconnue.**
Les noms de propriétés des processeurs varient selon la version de NiFi. Le
script les résout dynamiquement à partir des descripteurs du composant ; si un
type manque, c'est que l'extension n'est pas dans l'image. Le message d'erreur
nomme le type introuvable.

**NiFi ne dépose rien dans MinIO.**
Vérifiez d'abord que le Process Group est démarré, puis regardez la file de
rebut : `lakehouse-raw/_dead_letter/`. Le contenu et les attributs de chaque
FlowFile rejeté y sont conservés — c'est exactement ce à quoi elle sert.

**Dremio ne voit aucune table.**
Le médaillon n'a probablement pas encore tourné. Si `lakehouse-warehouse`
contient bien des objets, forcez la redécouverte :

```bash
python dremio/scripts/setup_dremio.py --recreate-source
```

**Spark échoue sur `NoSuchTableException`.**
Nessie n'était pas prêt au premier `CREATE NAMESPACE`. Vérifiez
http://localhost:19120/api/v2/config puis relancez le DAG.

**Un conteneur est tué sans message (`exited 137`).**
C'est l'OOM killer : Docker manque de mémoire. Voir « Machine limitée »
ci-dessus, ou augmentez l'allocation WSL2.

**Le backfill s'interrompt en cours de route.**
Relancez simplement `fakestore_backfill_history` : les dates déjà présentes
dans MinIO sont automatiquement sautées, il reprend où il s'était arrêté.

---

# PARTIE II — RÉFÉRENCE

## Structure du projet

```
.
├── docker-compose.yml              16 services, 1 réseau, 13 volumes
├── .env                            configuration centralisée
├── Makefile                        toutes les opérations courantes
│
├── docker/
│   ├── spark/                      Spark 3.5.1 + Iceberg 1.5.2 + Nessie 0.77.1
│   └── airflow/                    Airflow 2.9.3 + JRE 17 + Spark (spark-submit)
│
├── nifi/
│   ├── scripts/nifi_client.py      client REST minimal (révisions, descripteurs)
│   ├── scripts/build_flow.py       construit le dataflow — 12 processeurs
│   ├── scripts/export_flow.py      exporte le flow en .json (livrable §6.2)
│   └── flow/                       exports produits
│
├── spark/jobs/
│   ├── common/                     session, schémas, historisation, incrémental
│   ├── bronze/bronze_ingest.py     zone brute → Bronze (3 domaines)
│   ├── silver/                     dim_products, dim_customers, fct_order_items
│   ├── gold/                       4 tables analytiques
│   └── maintenance/                qualité, compaction, démo Iceberg/Nessie
│
├── airflow/dags/
│   ├── lakehouse_common.py         socle partagé
│   ├── dag_fakestore_backfill_history.py
│   ├── dag_fakestore_daily_ingestion.py
│   └── dag_lakehouse_medallion.py
│
├── dremio/
│   ├── scripts/setup_dremio.py     source Nessie + vues analytiques
│   ├── scripts/run_validation.py   exécute et affiche les requêtes
│   └── sql/validation_dremio.sql   10 requêtes couvrant la Partie 5
│
├── monitoring/                     Prometheus (scrape + alertes), Grafana
│
├── scripts/
│   ├── verifier_prerequis.py       contrôle avant le premier démarrage
│   ├── verifier_projet.py          contrôle statique avant démonstration
│   ├── init-minio.sh               buckets, cycle de vie, structure
│   ├── postgres-init/              bases airflow / nessie / dremio
│   └── plateforme.ps1              équivalent PowerShell du Makefile
│
├── docs/
│   ├── architecture.md             conception et justification des choix
│   └── scenario_video.md           conducteur de la soutenance 15 min
│
└── slides/
    ├── generer_slides.py           génère le support (reproductible)
    └── soutenance_lakehouse_nifi.pptx   17 diapositives + notes
```

## Modèle de données

### Zone brute (MinIO)

```
lakehouse-raw/fakestore/<domaine>/ingest_date=YYYY-MM-DD/<domaine>_<date>_<epoch>_<uuid>.json
lakehouse-raw/_dead_letter/<domaine>/<date>/<uuid>.json
```

### Médaillon (Iceberg, catalogue Nessie)

| Couche | Table | Grain | Contenu |
|---|---|---|---|
| Bronze | `raw_products` | enregistrement × snapshot | copie fidèle + 8 colonnes de traçabilité |
| Bronze | `raw_users` | idem | mot de passe haché SHA-256 |
| Bronze | `raw_carts` | idem | paniers, structure imbriquée conservée |
| Silver | `dim_products` | produit × jour | typé, aplati, segmenté, historisé |
| Silver | `dim_customers` | client × jour | aplati, géolocalisation validée, minimisé RGPD |
| Silver | `fct_order_items` | commande × produit | flux de commandes sur la fenêtre |
| Gold | `gold_catalog_daily_kpi` | jour × catégorie | prix, notes, avis, complétude |
| Gold | `gold_product_price_trend` | produit × jour | moyennes mobiles, promotions, indice base 100 |
| Gold | `gold_sales_by_category_daily` | jour × catégorie | CA, panier moyen, tendances (jointure 3 domaines) |
| Gold | `gold_customer_360` | client | valeur vie, segmentation RFM |

## Couverture du sujet

| Partie | Exigence | Où |
|---|---|---|
| 1 | Infrastructure complète (services, réseau, volumes, ports) | `docker-compose.yml`, `scripts/init-minio.sh` |
| 2 | NiFi déployé, intégré au réseau, état persistant | service `nifi`, 7 volumes dédiés, `docs/architecture.md` §2.4 |
| 3 | Flux d'ingestion API → MinIO, correct et robuste | `nifi/scripts/build_flow.py` — 12 processeurs, 3 chemins de reprise, file de rebut |
| 4 | Médaillon complet écrit intégralement | `spark/jobs/` — 3 Bronze, 3 Silver, 4 Gold |
| 4.1 | Historique reconstitué de 6 mois | `spark/jobs/common/history.py` + `dag_fakestore_backfill_history.py` |
| 5 | Dremio testé : par couche, jointure, agrégation | `dremio/sql/validation_dremio.sql`, `run_validation.py` |
| 6 | Orchestration Airflow, bout en bout | 3 DAGs, option A retenue et justifiée |
| 6.1 | Vidéo de soutenance 15 min | `slides/soutenance_lakehouse_nifi.pptx` + [`docs/scenario_video.md`](docs/scenario_video.md) |
| 6.2 | Export du flow NiFi en `.json` | `make nifi-export` → `nifi/flow/fakestore_ingestion.json` |
| 7 | Bonus | supervision NiFi, idempotence, 3ᵉ domaine, Parameter Context, time travel |

---

## Note d'intégrité

Travail individuel, conformément aux règles de l'épreuve. La plateforme, le
flow NiFi, les jobs Spark, les DAGs Airflow et la configuration Dremio ont été
conçus et écrits pour cet examen ; aucune base de code n'a été reprise des
dépôts de référence cités dans le sujet. Les justifications techniques
présentées dans [`docs/architecture.md`](docs/architecture.md) et dans la vidéo
de soutenance rendent compte de ces choix.
