# Scénario de la vidéo de soutenance — 15 minutes

> Conducteur de tournage. Le support PowerPoint
> (`slides/soutenance_lakehouse_nifi.pptx`) contient déjà ce texte dans les
> **notes de présentateur** de chaque diapositive : en mode Présentateur, on
> voit la diapositive projetée d'un côté et le texte à dire de l'autre.

Le sujet est explicite (§6.1) : *« Montrez l'écran réel […], pas des
diapositives seules. »* Cinq séquences sur sept se déroulent donc dans les
interfaces. Les diapositives ne servent qu'à cadrer et à structurer.

---

## Avant d'appuyer sur enregistrer

### La plateforme

- [ ] `make up` lancé depuis au moins 5 minutes
- [ ] `docker compose ps` → tous les services **healthy**
- [ ] flow NiFi construit (`make nifi-flow`) et **démarré**
- [ ] source Dremio déclarée (`make dremio-setup`)
- [ ] **backfill des 6 mois déjà exécuté** — ne pas le lancer pendant la vidéo,
      il dure une quinzaine de minutes
- [ ] `make dremio-test` déjà passé une fois : les 10 requêtes doivent être
      vertes avant de filmer
- [ ] `make medaillon` lancé une seconde fois, pour avoir **deux snapshots
      Iceberg** et pouvoir démontrer le time travel

### L'écran

- [ ] résolution 1920×1080, mise à l'échelle Windows à 100 %
- [ ] navigateur en plein écran, **zoom à 110-125 %** — le texte de NiFi et
      d'Airflow est petit à l'enregistrement
- [ ] terminal en police 14 pt minimum, thème clair ou sombre à fort contraste
- [ ] onglets préparés **dans cet ordre**, de gauche à droite :
      NiFi · Airflow · MinIO · Dremio · Spark master · Grafana
- [ ] VS Code ouvert sur le dépôt, `docker-compose.yml` déjà affiché
- [ ] notifications Windows/Teams/Slack **coupées**
- [ ] arrière-plan de bureau neutre, aucun document personnel visible

### Le contenu

- [ ] diapositive 1 : remplacer « Nom Prénom » par le vrai nom
- [ ] relire les notes des diapositives 4, 7, 9, 10 et 11 — ce sont celles où
      le fond compte le plus
- [ ] faire **un passage complet à blanc**, chronomètre en main

---

## Conducteur

| Temps | Durée | Diapo | Écran | Contenu |
|---|---|---|---|---|
| 00:00 | 0:20 | 1 | diapo | Ouverture, identité, objet |
| 00:20 | 0:25 | 2 | diapo | Plan de la soutenance |
| 00:45 | 0:25 | 3 | diapo | Ce qui est imposé / ce que j'ai conçu |
| 01:10 | 1:10 | 4 | diapo | **Architecture** — le chemin de la donnée |
| 02:20 | 0:40 | 5 | diapo | Quatre choix structurants |
| 03:00 | 1:30 | 6 | **VS Code + terminal** | **docker-compose.yml** |
| 04:30 | 3:00 | 7 | **NiFi + MinIO** | **Le flow d'ingestion** ← 4 pts |
| 07:30 | 0:30 | 8 | diapo | Le contrat des trois couches |
| 08:00 | 0:40 | 9 | diapo | Historisation — le problème |
| 08:40 | 0:40 | 10 | diapo | Historisation — la réponse |
| 09:20 | 1:40 | 11 | **VS Code** | **Les jobs Spark** ← 4 pts |
| 11:00 | 2:00 | 12 | **Dremio + terminal** | **Test SQL** ← 3 pts |
| 13:00 | 1:15 | 13 | **toutes les interfaces** | **Bout en bout** ← 3 pts |
| 14:15 | 0:20 | 14 | diapo | Bonus réalisés |
| 14:35 | 0:15 | 15 | diapo | Difficultés |
| 14:50 | 0:20 | 16 | diapo | Bilan et clôture |

**Total : 15:10.** La fourchette acceptée est 14–17 minutes ; il reste donc de
la marge, mais elle est mince. Les séquences compressibles en cas de retard
sont, dans l'ordre : 3 (cadrage), 14 (bonus), 15 (difficultés).

---

## Les cinq phrases à ne pas rater

Ce sont les moments où le jury évalue la **compréhension**, pas la réalisation.

**1 — Le nom de service Docker** *(séquence 3, sur PutS3Object)*

> « L'endpoint S3 pointe sur `http://minio:9000`, le nom de service Docker.
> Depuis le conteneur NiFi, `localhost` désignerait NiFi lui-même. Et le
> path-style access est obligatoire : MinIO n'implémente pas le virtual-hosted
> style d'AWS. »

**2 — La frontière NiFi / Spark** *(séquence 3, sur RouteOnAttribute)*

> « Le contrôle se limite au code HTTP, à la taille et au type MIME. Pas de
> parsing, pas de calcul, pas de jointure : la transformation métier appartient
> à Spark, c'est une contrainte explicite du sujet. »

**3 — L'ancrage de l'historisation** *(séquence exposé, diapositive 10)*

> « La valeur que l'API renvoie aujourd'hui est traitée comme le point
> d'arrivée de l'historique, et le modèle rétro-projette le passé. Sur la date
> la plus récente, le facteur vaut exactement un : la donnée observée n'est
> jamais altérée. Et tout est déterministe — relancer le backfill reproduit
> exactement le même historique. »

**4 — La jointure temporelle** *(séquence 4, sur gold_sales_daily.py)*

> « La jointure entre les commandes et les produits porte sur
> `p.snapshot_date = o.order_date`. Chaque ligne est valorisée au prix en
> vigueur le jour de la commande. Valoriser une commande de mars avec le prix
> d'aujourd'hui serait une erreur classique de dimension à évolution lente. »

**5 — Le choix de l'option A** *(séquence 6, pendant le bout en bout)*

> « La tâche `attendre_depot` ne suppose pas que NiFi a terminé : elle observe
> la zone brute jusqu'à constater le dépôt. C'est l'option A. Un DAG qui
> observe l'état réel du stockage survit à une panne de NiFi ; un DAG déclenché
> par événement perd les données si le signal se perd. »

---

## Gestion des incidents pendant l'enregistrement

| Incident | Réaction |
|---|---|
| Une interface met du temps à charger | continuer de parler, ne pas laisser de blanc : « pendant que la page se charge, je précise que… » |
| Le DAG échoue en direct | **ne pas s'acharner**. Ouvrir les logs de la tâche, lire l'erreur à voix haute, expliquer la cause probable, puis montrer le run précédent réussi. Un incident lu et compris vaut mieux qu'un silence |
| NiFi ne dépose rien | montrer la file de rebut `lakehouse-raw/_dead_letter/` — c'est justement ce qu'elle est là pour faire |
| Le temps déborde de 2 minutes | sauter les diapositives 14 et 15, aller directement au bilan |
| Perte du fil | revenir au support : les notes de présentateur contiennent le texte |

**Filet de sécurité de la séquence 6** — si Airflow est lent, déclencher
l'ingestion directement :

```bash
curl -X POST http://localhost:9095/ingest -H "Content-Type: application/json" -d "{\"snapshot_date\":\"2026-09-10\",\"domains\":[\"products\",\"users\",\"carts\"]}"
```

puis montrer MinIO, lancer `make medaillon`, et finir sur Dremio.

---

## Après l'enregistrement

- [ ] durée entre 14 et 17 minutes
- [ ] audio audible du début à la fin, sans saturation
- [ ] aucune information personnelle visible à l'écran (mails, onglets privés)
- [ ] dépôt Git privé partagé avec l'enseignant, contenant :
      `docker-compose.yml`, `spark/jobs/`, `airflow/dags/`,
      **`nifi/flow/fakestore_ingestion.json`** (produit par `make nifi-export`),
      `dremio/`, `docs/`, `README.md`
- [ ] vidéo déposée en lien non listé (YouTube/Drive), **lien testé en
      navigation privée**
- [ ] lien fonctionnel jusqu'à la correction
