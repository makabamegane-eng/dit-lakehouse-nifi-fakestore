# Architecture de la plateforme Data Lakehouse

> DIT — Master 2 Ingénierie des Données / Big Data
> Module « Architectures Data Lakehouse & Ingestion de données » — M. PENE
> Document de conception accompagnant la vidéo de soutenance.

Ce document répond à la question que pose l'examen : **pourquoi cette
architecture-là**. Le sujet fournit un cahier des charges, pas un schéma ; tout
ce qui suit relève de choix personnels, et chaque choix est présenté avec son
alternative écartée.

---

## 1. Vue d'ensemble

```
                     ┌──────────────────────────────┐
                     │      FakeStoreAPI (HTTP)     │
                     │  /products  /users  /carts   │
                     └───────────────┬──────────────┘
                                     │  GET  (seul lien vers l'extérieur)
                                     ▼
   ╔═════════════════════════════════════════════════════════════════════╗
   ║  APACHE NIFI — unique point d'ingestion                             ║
   ║  ListenHTTP → EvaluateJsonPath → SplitJson → InvokeHTTP →           ║
   ║  RouteOnAttribute → DetectDuplicate → UpdateAttribute → PutS3Object ║
   ║  Collecte · contrôle · dépôt.  Aucune transformation métier.        ║
   ╚═══════════════════════════════┬═════════════════════════════════════╝
                                   │  s3a://lakehouse-raw/fakestore/…
                                   ▼
   ┌──────────────────────────── MinIO ───────────────────────────────────┐
   │  lakehouse-raw          zone brute JSON, partitionnée par date       │
   │  lakehouse-warehouse    fichiers Parquet des tables Iceberg          │
   └───────────────┬──────────────────────────────────┬───────────────────┘
                   │                                  │
       lecture s3a │                                  │ lecture/écriture S3FileIO
                   ▼                                  ▼
   ╔═══════════════════════════════╗       ┌──────────────────────────────┐
   ║  APACHE SPARK (1 master +     ║◄─────►│  PROJECT NESSIE              │
   ║  2 workers)                   ║       │  catalogue Iceberg versionné │
   ║  Bronze → Silver → Gold       ║       │  (backend JDBC PostgreSQL)   │
   ╚═══════════════┬═══════════════╝       └──────────────┬───────────────┘
                   │                                      │
       spark-submit│                                      │ API v2
                   │                                      ▼
   ╔═══════════════╧═══════════════╗       ┌──────────────────────────────┐
   ║  APACHE AIRFLOW               ║       │  DREMIO OSS                  │
   ║  3 DAGs : backfill, ingestion ║       │  moteur SQL du lakehouse     │
   ║  quotidienne, médaillon       ║       │  + vues métier (analytics)   │
   ╚═══════════════════════════════╝       └──────────────────────────────┘

   ┌──── PostgreSQL ────┐        ┌──── Prometheus + Grafana ─────┐
   │ airflow │ nessie   │        │ infra · NiFi · MinIO · Spark  │
   └────────────────────┘        └───────────────────────────────┘
```

**Le fil conducteur** : une donnée entre par NiFi, atterrit dans MinIO, est
transformée par Spark en tables Iceberg décrites par Nessie, et ressort par
Dremio en SQL. Airflow ordonnance, Prometheus observe.

---

## 2. Choix d'infrastructure

### 2.1 Un seul réseau Docker, aucune adresse IP

Tous les services partagent le réseau bridge `lakehouse` et se désignent par
leur **nom de service**. C'est le point qui conditionne toute l'intégration :
depuis le conteneur NiFi, `localhost:9000` désigne NiFi lui-même, pas MinIO.
L'endpoint S3 configuré dans le flow est donc `http://minio:9000`, et le
script `scripts/verifier_projet.py` refuse tout `localhost` dans `.env`.

### 2.2 Un PostgreSQL, trois bases logiques

Airflow et Nessie ont chacun besoin d'une base relationnelle. Trois conteneurs
PostgreSQL auraient consommé ~600 Mo de RAM pour un bénéfice nul sur un poste
de développement. Un serveur unique héberge `airflow`, `nessie` et `dremio`,
créées par `scripts/postgres-init/00-create-databases.sql`. L'isolation
logique est suffisante ici ; en production, on séparerait pour des raisons de
cycle de vie et de sauvegarde, pas de sécurité.

### 2.3 Ordre de démarrage maîtrisé

Onze services avec des dépendances croisées ne démarrent pas correctement au
hasard. Chaque service critique expose un `healthcheck`, et les dépendances
utilisent `condition: service_healthy` plutôt que le simple `depends_on` :

| Service | Attend | Pourquoi |
|---|---|---|
| `minio-init` | `minio` healthy | créer un bucket sur un serveur non démarré échoue |
| `nessie` | `postgres` healthy | le version store JDBC migre son schéma au boot |
| `spark-master` | `minio` healthy, `nessie` | le catalogue doit répondre au premier `CREATE NAMESPACE` |
| `dremio` | `nessie`, `minio` healthy | la source Nessie est validée à la création |
| `airflow-webserver` | `airflow-init` **terminé avec succès** | la base doit être migrée et l'admin créé |

### 2.4 NiFi en HTTP simplifié — décision assumée

Depuis la version 1.14, NiFi démarre par défaut en **HTTPS avec un
utilisateur unique généré aléatoirement**, dont le mot de passe n'apparaît que
dans les logs du conteneur :

```bash
docker logs nifi 2>&1 | grep -i "Generated Username\|Generated Password"
```

Nous avons choisi de **désactiver ce mode** en positionnant
`NIFI_WEB_HTTP_PORT=8080`, ce qui bascule NiFi en HTTP sans authentification.

*Justification.* La plateforme tourne sur un réseau Docker local, non exposé.
Le certificat auto-signé de NiFi impose au navigateur une exception de
sécurité, et surtout il empêche les scripts (`build_flow.py`, `export_flow.py`)
et Airflow de dialoguer avec l'API REST sans embarquer une gestion de
certificats sans rapport avec l'objet de l'examen. Le coût de sécurité est
nul dans ce contexte, le gain de reproductibilité est réel.

*Ce que nous ferions en production* : conserver HTTPS, provisionner un
certificat d'autorité interne, et authentifier les appels REST par jeton
(`/access/token`) plutôt que de désactiver la couche de sécurité.

### 2.5 Persistance

Sept volumes nommés couvrent l'état interne de NiFi (`flowfile_repository`,
`content_repository`, `provenance_repository`, `database_repository`, `state`,
`conf`, `logs`), auxquels s'ajoutent MinIO, PostgreSQL, Nessie, Dremio,
Prometheus et Grafana. Conséquence concrète : un `docker compose restart`
conserve le flow NiFi, l'historique de provenance et les tables Iceberg.
Seul `make reset` (`down -v`) remet tout à zéro.

---

## 3. Le flux d'ingestion NiFi

### 3.1 Pourquoi un déclenchement par `ListenHTTP`

L'entrée du flow n'est pas un `GenerateFlowFile` planifié mais un
**`ListenHTTP`** qui attend un JSON :

```json
{ "snapshot_date": "2026-09-10", "domains": ["products", "users", "carts"] }
```

Trois raisons :

1. **La date logique est portée par l'appelant.** C'est ce qui rend possible
   la reconstitution des 6 mois : Airflow envoie 180 demandes, chacune avec sa
   propre date, et NiFi range chaque réponse dans la bonne partition. Un
   `GenerateFlowFile` planifié ne saurait ingérer que « maintenant ».
2. **Le contrat d'interface est trivial et démontrable.** Un `curl` suffit à
   déclencher l'ingestion en direct pendant la soutenance.
3. **Airflow ne manipule pas l'état interne de NiFi.** Piloter NiFi via son API
   REST (`run-once` sur un processeur) imposerait de connaître les
   identifiants UUID des composants, qui changent à chaque reconstruction du
   flow — un couplage fragile.

### 3.2 Le contrôle est minimal, et c'est voulu

Le sujet est explicite : NiFi fait « collecte, contrôle et dépôt », **aucune
transformation métier**. Le contrôle se limite donc à un `RouteOnAttribute` :

```
${invokehttp.status.code:equals(200)
  :and(${fileSize:gt(20)})
  :and(${mime.type:contains('json')})}
```

Code HTTP correct, charge utile non vide, type MIME cohérent. Pas de parsing
du contenu, pas de validation de schéma métier, pas de calcul. Tout le reste
appartient à Spark. Une réponse qui ne satisfait pas ce prédicat part en file
de rebut, avec son contenu intact.

### 3.3 Convention de nommage de la zone brute

```
lakehouse-raw/
└── fakestore/
    ├── products/ingest_date=2026-09-10/products_2026-09-10_1757462400123_<uuid>.json
    ├── users/   ingest_date=2026-09-10/…
    └── carts/   ingest_date=2026-09-10/…
└── _dead_letter/<domaine>/<date>/<uuid>.json
```

Chaque élément a une raison d'être :

| Élément | Raison |
|---|---|
| `ingest_date=YYYY-MM-DD` | syntaxe de partition Hive reconnue nativement par Spark — aucun parsing de nom de fichier n'est nécessaire pour élaguer |
| epoch en millisecondes | conserve l'instant **réel** de l'appel HTTP, distinct de la date **logique** du snapshot. Indispensable pour auditer un backfill |
| UUID | deux exécutions du même jour cohabitent sans écrasement ; le dédoublonnage est fait plus loin, par Spark, sur des critères métier |
| `_dead_letter/` hors de `fakestore/` | les rebuts ne sont jamais lus par Spark, mais restent consultables |

### 3.4 Gestion des erreurs : rien ne se perd

| Chemin d'échec | Traitement |
|---|---|
| Appel API en échec (`Retry`, `No Retry`, `Failure`) | `RetryFlowFile` — 3 tentatives pénalisées, puis file de rebut |
| Réponse non conforme | file de rebut, avec le corps de la réponse |
| Dépôt S3 en échec | `RetryFlowFile` — 3 tentatives, puis file de rebut |
| Doublon détecté | journalisé et abandonné — ce n'est pas une erreur, c'est l'idempotence qui fonctionne |
| JSON de demande invalide | file de rebut |

Toutes les connexions portent une **backpressure** à 10 000 FlowFiles / 1 Go :
une file qui gonfle bloque son amont au lieu de saturer le disque.

### 3.5 Idempotence (bonus 7.2)

Un `DetectDuplicate` adossé à un `DistributedMapCache` utilise la clé
`${domain}::${snapshot.date}`. Rejouer une ingestion déjà effectuée n'écrit pas
un second objet. Péremption à 24 h, pour qu'une reprise le lendemain reste
possible.

### 3.6 Réutilisabilité (bonus 7.4)

Tout ce qui est spécifique à FakeStoreAPI vit dans un **Parameter Context**
(`api.base.url`, `minio.*`, `listen.port`, `airflow.*`). Changer
`#{api.base.url}` suffit à brancher le flow sur une autre API REST renvoyant du
JSON, sans toucher à un seul processeur. Les secrets (`minio.secret.key`,
`airflow.auth`) sont déclarés `sensitive` et chiffrés par NiFi.

---

## 4. Reconstitution de 6 mois d'historique

C'est la contrainte la plus intéressante du sujet (§4.1), parce qu'elle n'a pas
de solution évidente : l'API ne renvoie que l'état courant.

### 4.1 Le problème posé honnêtement

Ingérer 180 fois la même réponse produit un historique techniquement conforme
mais analytiquement mort : toutes les courbes seraient plates. À l'inverse,
générer des données de toutes pièces trahirait la contrainte « données issues
de FakeStoreAPI ».

### 4.2 La réponse retenue — trois niveaux explicites

| Niveau | Nature | Ce qui est réel | Ce qui est reconstruit |
|---|---|---|---|
| **1 — Historisation réelle** | toujours actif | 180 appels HTTP authentiques, un par date logique, chacun horodaté et rangé dans sa partition | rien |
| **2 — Horodatage redistribué** | toujours actif | les 7 paniers de `/carts` et leur composition | leur date, projetée de 2019-2020 vers la fenêtre courante |
| **3 — Dérive synthétique** | désactivable | la valeur du jour renvoyée par l'API | l'évolution du prix et du cumul d'avis dans le passé |

Le niveau 1 satisfait littéralement la contrainte. Les niveaux 2 et 3 donnent à
l'historique sa valeur analytique.

### 4.3 Les deux propriétés qui rendent le niveau 3 défendable

**Déterminisme.** Aucune fonction aléatoire n'est utilisée. Toute la variation
dérive de `hash(clé | date | graine)`, donc deux exécutions produisent
exactement le même historique. Le pipeline reste idempotent et rejouable, et un
correcteur qui relance le backfill obtient les mêmes chiffres.

**Ancrage sur le réel.** La valeur renvoyée aujourd'hui par l'API est traitée
comme le **point d'arrivée** de l'historique ; le modèle rétro-projette le
passé. Sur la date la plus récente, le facteur de prix vaut exactement `1.0` :
**la donnée observée n'est jamais altérée**.

```
prix(t) = prix_api × [ 1 + tendance(t) + saisonnalité(produit, t) + bruit(produit, t) ]
          avec facteur(t = dernier jour) = 1.0 exactement

avis(t) = avis_api × croissance(t)        croissance(t = dernier jour) = 1.0
```

### 4.4 La donnée ne ment jamais sur sa nature

Chaque table Silver et Gold porte un booléen `is_reconstructed`, et la mesure
brute reste disponible à côté de la mesure historisée :

| Colonne | Contenu |
|---|---|
| `unit_price_observed` | ce que l'API a réellement renvoyé ce jour-là |
| `unit_price` | la valeur historisée, égale à la précédente sur la dernière date |
| `template_cart_id` | le panier réel dont une commande générée est issue |

`SYNTHETIC_HISTORY_ENABLED=false` dans `.env` coupe intégralement le niveau 3 :
le pipeline continue de fonctionner, seules les tendances deviennent plates.

---

## 5. Le médaillon

### 5.1 Contrat de chaque couche

| | Bronze | Silver | Gold |
|---|---|---|---|
| **Rôle** | mémoire fidèle | vérité métier | réponses aux questions |
| **Structure** | brute, imbriquée | plate, typée | agrégée |
| **Rejets** | conservés et marqués | exclus | — |
| **Grain** | 1 ligne = 1 enregistrement d'un appel API | 1 ligne = 1 entité × 1 jour | variable, documenté |
| **Écriture** | `overwritePartitions` (incrémental) | `overwritePartitions` (incrémental) | `createOrReplace` (recalcul complet) |
| **Partition** | `_ingest_date` (jour) | `months(snapshot_date)` | `months(...)` ou aucune |

### 5.2 Tables produites

```
bronze.raw_products      bronze.raw_users        bronze.raw_carts
   │                        │                        │
   ▼                        ▼                        ▼
silver.dim_products      silver.dim_customers    silver.fct_order_items
   │                        │                        │
   └────────────┬───────────┴────────────┬───────────┘
                ▼                        ▼
   gold.gold_catalog_daily_kpi     gold.gold_sales_by_category_daily
   gold.gold_product_price_trend   gold.gold_customer_360
```

### 5.3 Décisions de transformation notables

**Schémas explicites, jamais d'inférence.** Un schéma inféré change tout seul
le jour où l'API renvoie un entier au lieu d'un décimal, et casse Silver en
silence. Les schémas sont figés dans `common/schemas.py`, avec
`mode=PERMISSIVE` et une colonne `_corrupt_record` pour isoler l'illisible.

**Le mot de passe n'est jamais persisté en clair.** Seule entorse assumée au
principe « Bronze = copie fidèle » : `/users` expose un champ `password`, qui
est haché en SHA-256 dès l'écriture Bronze. Aucun argument de fidélité ne
justifie de stocker un secret en clair.

**Les prix sont en `DECIMAL(10,2)`, pas en `DOUBLE`.** Une valeur monétaire
agrégée sur 5 000 lignes en flottant produit des écarts au centime qui font
échouer les réconciliations.

**La jointure commandes × produits est temporelle.**
`p.snapshot_date = o.order_date` : chaque ligne de commande est valorisée au
prix en vigueur **le jour où elle a été passée**. Valoriser au prix du jour
serait l'erreur classique d'une dimension à évolution lente mal exploitée.

**Le `LEFT JOIN` plutôt que le `INNER JOIN` en Gold.** Une ligne de commande
sans produit correspondant n'est pas silencieusement perdue : elle est comptée
dans `nb_unmatched_lines`, indicateur d'intégrité référentielle exposé jusqu'en
Gold et contrôlé par la porte qualité.

**Partition mensuelle en Silver, pas journalière.** 180 partitions
journalières pour 20 produits produiraient des fichiers Parquet de quelques
kilo-octets. `months(snapshot_date)` donne 6 partitions, une taille de fichier
saine, et un élagage encore efficace.

**Gold recalculé intégralement.** Le volume est agrégé (quelques milliers de
lignes) : l'incrémental n'apporterait aucun gain mesurable, alors qu'un MERGE
mal ordonné peut faire dériver un agrégat. `createOrReplace` produit un
**nouveau snapshot Iceberg** — il n'efface pas l'historique, le time travel
reste intact.

### 5.4 Idempotence, à tous les étages

Le même raisonnement est appliqué trois fois : comparer les dates présentes en
amont à celles déjà matérialisées en aval, ne traiter que la différence, et
écrire en `overwritePartitions()`.

C'est une **comparaison d'ensembles**, pas un watermark « date maximale ». Un
watermark monotone raterait une partition arrivée en retard (rejeu NiFi d'une
journée manquée) ; la comparaison d'ensembles la rattrape.

---

## 6. Orchestration

### 6.1 Trois DAGs, trois rôles

| DAG | Déclenchement | Rôle |
|---|---|---|
| `fakestore_backfill_history` | manuel | amorçage : rejoue l'ingestion NiFi sur 180 dates |
| `fakestore_daily_ingestion` | horaire | régime de croisière : une photo de plus dans l'historique |
| `lakehouse_medallion` | horaire (H+15) ou déclenché | Bronze → Silver → Gold → contrôles qualité |

### 6.2 Option A retenue, option B implémentée

Le sujet propose deux modes de déclenchement de la partie Spark. Nous retenons
l'**option A** : le DAG inspecte la zone brute et ne poursuit que si des
données sont présentes.

*Pourquoi A.*
- Reconstituer 6 mois impose de rejouer 180 dates. Avec l'option B, ce serait à
  NiFi de piloter ce backfill — or il n'a ni gestion de dépendances, ni reprise
  sur échec, ni vue d'ensemble.
- Un DAG qui **observe l'état réel du stockage** est robuste à une panne de
  NiFi, à un redémarrage, ou à un dépôt manuel. Un DAG déclenché par événement
  perd les données si l'événement est perdu.
- « Le pipeline dépend de la présence des données » s'audite mieux que « le
  pipeline dépend d'un signal ».

*L'option B est néanmoins câblée.* Le processeur `10. notifier-airflow
(InvokeHTTP)` appelle l'API REST d'Airflow à la fin du dépôt. Il est
**volontairement à l'arrêt** ; le démarrer en direct pendant la soutenance
montre que les deux modes fonctionnent.

### 6.3 Pourquoi le backfill boucle dans une tâche

La solution idiomatique serait `catchup=True` avec `start_date` à J-180. Elle a
été écartée : 180 DAG runs saturent l'ordonnanceur et l'interface pendant
plusieurs minutes — illisible en démonstration — pour un travail réel de trois
appels HTTP par run. La boucle, avec `skip_existing`, reprend exactement là où
elle s'était arrêtée après une interruption.

### 6.4 La porte qualité

Le DAG se termine par `maintenance/data_quality_checks.py`, avec `retries=0` :
un contrôle qualité qui échoue ne se « réessaie » pas. Sept familles de
contrôles, dont le plus intéressant est une **réconciliation croisée** entre
`gold_sales_by_category_daily` et `gold_customer_360` : deux tables construites
par deux jobs indépendants, qui agrègent la même matière selon deux axes. Leur
total de chiffre d'affaires doit coïncider — un écart révèle une divergence de
règle de jointure qu'aucun contrôle intra-table ne détecterait.

---

## 7. Requêtage — Dremio

Dremio est déclaré automatiquement par `dremio/scripts/setup_dremio.py`.

Le point critique : une source Nessie a besoin des **deux moitiés** de
l'information — `nessieEndpoint` pour savoir *quelles tables existent et de
quels fichiers elles sont faites*, et `awsRootPath` + propriétés S3 pour savoir
*où sont les octets*. Oublier la seconde donne une source qui se connecte mais
dont toutes les tables sont vides. Trois propriétés rendent MinIO utilisable là
où Dremio attend un vrai S3 :

```
fs.s3a.path.style.access = true     MinIO n'implémente pas le virtual-hosted style
fs.s3a.endpoint          = minio:9000
dremio.s3.compat         = true     bascule Dremio en mode S3-compatible
```

Dix requêtes de validation (`dremio/sql/validation_dremio.sql`) couvrent les
exigences de la Partie 5 : une par couche, deux jointures inter-domaines, deux
agrégations Gold, plus le time travel. Elles sont rejouables d'une commande :

```bash
make dremio-test
```

Trois vues (`analytics.v_ventes_quotidiennes`, `v_synthese_mensuelle`,
`v_clients_a_valeur`) sont publiées au-dessus des tables Gold — élément
valorisé par le sujet.

---

## 8. Supervision

| Niveau | Cibles | Question à laquelle il répond |
|---|---|---|
| Infrastructure | node-exporter, cAdvisor | la machine tient-elle ? |
| Services | MinIO, Nessie, Spark master | les briques sont-elles saines ? |
| Ingestion | NiFi (PrometheusReportingTask) | les données circulent-elles ? |

Le dashboard Grafana `DIT Lakehouse — Vue d'ensemble` est provisionné
automatiquement. Le panneau NiFi (bonus 7.1) montre le débit d'ingestion : les
pics correspondent aux déclenchements Airflow, un plateau à zéro pendant un
backfill signale un blocage.

Les règles d'alerte de `monitoring/prometheus/alerts.yml` sont volontairement
peu nombreuses : chacune correspond à une panne réellement rencontrée pendant
la construction de la plateforme.

---

## 9. Difficultés rencontrées

| Difficulté | Résolution |
|---|---|
| NiFi 1.28 impose HTTPS + utilisateur généré | bascule en HTTP par `NIFI_WEB_HTTP_PORT`, justifiée §2.4 ; procédure `docker logs` documentée en alternative |
| `localhost` dans l'endpoint S3 du flow | endpoint sur le nom de service Docker ; contrôle automatisé dans `verifier_projet.py` |
| MinIO refuse le virtual-hosted style S3 | `path-style-access` activé côté NiFi, Spark **et** Dremio — les trois doivent être configurés |
| Deux chemins d'accès à S3 dans Spark | `s3a://` (Hadoop-AWS) pour lire les JSON bruts, `S3FileIO` (AWS SDK v2) pour les tables Iceberg — les deux doivent être configurés séparément |
| Versions Spark / Iceberg / Nessie | matrice figée dans l'image (Spark 3.5.1 + Iceberg 1.5.2 + Nessie 0.77.1), JARs embarqués plutôt que `--packages` |
| Driver Spark côté Airflow | image Airflow custom avec JRE 17 + distribution Spark 3.5.1 identique au cluster (le protocole RPC n'est pas garanti compatible entre versions mineures) |
| Historique plat sur une API sans dimension temporelle | modèle d'historisation à trois niveaux, §4 |
| Multiplication de petits fichiers Parquet | partition mensuelle en Silver + job de compaction Iceberg |
| 180 DAG runs illisibles en démonstration | boucle unique avec reprise, §6.3 |

---

## 10. Bonus réalisés (Partie 7)

| Bonus | Où |
|---|---|
| Supervision NiFi via Prometheus/Grafana | `build_flow.py` → PrometheusReportingTask + panneau dédié dans le dashboard |
| Idempotence par `DistributedMapCache` | processeur `7. detecter-doublons` + `overwritePartitions` côté Spark |
| Troisième domaine « commandes » | `/carts` → `silver.fct_order_items` → deux tables Gold |
| Process Group paramétrable | Parameter Context `fakestore-ingestion`, §3.6 |
| Fonctionnalités Iceberg avancées | `demo_iceberg_features.py` : time travel (`VERSION AS OF`, `TIMESTAMP AS OF`), schema evolution, branches Nessie |

---

## 11. Ce que nous ferions différemment à plus grande échelle

Ces choix sont adaptés à un volume de démonstration ; ils ne tiendraient pas
tels quels en production, et il est plus honnête de le dire :

- **Gold recalculé intégralement** — à partir de quelques millions de lignes,
  il faudrait passer à un `MERGE INTO` incrémental par partition.
- **Un seul PostgreSQL** — séparer Airflow et Nessie, dont les profils de
  charge et les besoins de sauvegarde n'ont rien à voir.
- **NiFi en HTTP** — HTTPS avec certificat interne et jetons d'accès.
- **Spark en mode client depuis Airflow** — passer en `cluster` mode ou sur
  Kubernetes pour ne pas faire du scheduler un point de défaillance unique.
- **Expiration des snapshots désactivée** — mettre en place une politique de
  rétention explicite, arbitrée entre coût de stockage et profondeur de time
  travel exigée.
