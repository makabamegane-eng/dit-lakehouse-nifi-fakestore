-- =============================================================================
--  VALIDATION DU MOTEUR DE REQUÊTE DREMIO — Partie 5 du sujet
-- -----------------------------------------------------------------------------
--  Chaque bloc est délimité par un marqueur `-- @query:` exploité par
--  dremio/scripts/run_validation.py, qui les exécute dans l'ordre et affiche
--  les résultats. Les mêmes requêtes se copient-collent telles quelles dans
--  l'éditeur SQL de Dremio pendant la démonstration vidéo.
--
--  Couverture des exigences :
--    5.1  tables Bronze/Silver/Gold visibles ......... Q0
--    5.2  une requête par couche .................... Q1 (Bronze), Q2 (Silver),
--                                                     Q3 (Gold)
--    5.3  jointure entre deux domaines .............. Q4, Q5
--    5.4  agrégation sur une table Gold ............. Q6, Q7
--    bonus : time travel Iceberg .................... Q8
-- =============================================================================


-- @query: Q0 — Inventaire des tables exposées par le catalogue Nessie
-- Première chose à montrer : Dremio voit bien les trois couches du médaillon,
-- sans qu'aucune table n'ait été déclarée manuellement. C'est le catalogue
-- Nessie qui les décrit, Dremio ne fait que le lire.
SELECT
    TABLE_SCHEMA                    AS couche,
    TABLE_NAME                      AS table_iceberg,
    TABLE_TYPE                      AS type
FROM INFORMATION_SCHEMA."TABLES"
WHERE TABLE_SCHEMA LIKE 'lakehouse%'
ORDER BY
    CASE
        WHEN TABLE_SCHEMA LIKE '%bronze%' THEN 1
        WHEN TABLE_SCHEMA LIKE '%silver%' THEN 2
        ELSE 3
    END,
    TABLE_NAME;


-- @query: Q1 — COUCHE BRONZE : traçabilité d'un enregistrement jusqu'à son fichier
-- Ce que cette requête démontre : la couche Bronze conserve la structure brute
-- de l'API (la colonne `rating` est encore un STRUCT) et sait dire, pour chaque
-- ligne, de quel objet MinIO elle provient et à quel instant réel l'appel a eu
-- lieu. C'est exactement ce qu'on attend d'une couche d'atterrissage.
SELECT
    id                              AS produit_id,
    title                           AS libelle,
    price                           AS prix_brut,
    category                        AS categorie,
    "rating"['rate']                AS note,
    "rating"['count']               AS nb_avis,
    _ingest_date                    AS date_snapshot,
    _ingest_ts                      AS instant_appel_api,
    _is_valid                       AS conforme,
    REGEXP_REPLACE(_source_file, '^.*/', '')  AS objet_minio
FROM lakehouse.bronze.raw_products
WHERE _ingest_date = (SELECT MAX(_ingest_date) FROM lakehouse.bronze.raw_products)
ORDER BY id
LIMIT 10;


-- @query: Q2 — COUCHE SILVER : données conformées, typées et enrichies
-- Différence visible avec Bronze : plus aucune structure imbriquée, un prix
-- en décimal, une catégorie normalisée, et des colonnes calculées (segment de
-- prix, complétude). La colonne `unit_price_observed` conserve la mesure brute
-- à côté de la mesure historisée — la traçabilité n'est jamais perdue.
SELECT
    product_id,
    title,
    category_label                  AS categorie,
    unit_price                      AS prix_historise,
    unit_price_observed             AS prix_observe_api,
    price_band                      AS segment_prix,
    rating_score                    AS note,
    review_count                    AS nb_avis,
    description_length              AS longueur_description,
    is_complete                     AS fiche_complete,
    snapshot_date
FROM lakehouse.silver.dim_products
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM lakehouse.silver.dim_products)
ORDER BY unit_price DESC
LIMIT 15;


-- @query: Q3 — COUCHE GOLD : indicateurs prêts à l'emploi
-- Aucun calcul dans la requête : tout est déjà matérialisé par les jobs Spark.
-- C'est le contrat de la couche Gold — l'outil de restitution n'a plus qu'à
-- lire. Les 30 derniers jours du catalogue, tous rayons confondus.
SELECT
    snapshot_date                   AS jour,
    nb_products                     AS nb_produits,
    avg_unit_price                  AS prix_moyen,
    median_unit_price               AS prix_median,
    avg_rating_weighted             AS note_ponderee,
    total_reviews                   AS avis_cumules,
    new_reviews                     AS nouveaux_avis,
    catalog_completeness_pct        AS completude_pct
FROM lakehouse.gold.gold_catalog_daily_kpi
WHERE category_slug = 'TOTAL'
ORDER BY snapshot_date DESC
LIMIT 30;


-- @query: Q4 — JOINTURE ENTRE DOMAINES : commandes x produits x clients
-- Exigence 5.3 du sujet. Trois domaines fonctionnels réunis en une requête :
--   * fct_order_items  -> domaine « commandes » (issu de /carts)
--   * dim_products     -> domaine « produits »  (issu de /products)
--   * dim_customers    -> domaine « clients »   (issu de /users)
-- Point important : la jointure sur les produits est TEMPORELLE
-- (p.snapshot_date = o.order_date). Chaque ligne est valorisée au prix en
-- vigueur le jour de la commande, pas au prix d'aujourd'hui.
SELECT
    o.order_date                    AS date_commande,
    o.order_id                      AS commande,
    c.full_name                     AS client,
    c.city                          AS ville,
    p.title                         AS produit,
    p.category_label                AS categorie,
    o.quantity                      AS quantite,
    p.unit_price                    AS prix_unitaire,
    CAST(o.quantity * p.unit_price AS DECIMAL(12,2))  AS montant_ligne
FROM lakehouse.silver.fct_order_items o
INNER JOIN lakehouse.silver.dim_products p
        ON p.product_id    = o.product_id
       AND p.snapshot_date = o.order_date
INNER JOIN lakehouse.silver.dim_customers c
        ON c.customer_id   = o.customer_id
       AND c.snapshot_date = o.order_date
WHERE o.order_date >= DATE_SUB(CURRENT_DATE, 7)
ORDER BY o.order_date DESC, o.order_id
LIMIT 25;


-- @query: Q5 — JOINTURE + AGRÉGATION : le croisement clients x produits
-- Variante de la précédente qui répond à une vraie question commerciale :
-- « quelles villes achètent quelles catégories, et pour combien ? »
SELECT
    c.city                          AS ville,
    p.category_label                AS categorie,
    COUNT(DISTINCT o.order_id)      AS nb_commandes,
    COUNT(DISTINCT o.customer_id)   AS nb_clients,
    SUM(o.quantity)                 AS articles_vendus,
    CAST(SUM(o.quantity * p.unit_price) AS DECIMAL(14,2))  AS chiffre_affaires,
    CAST(AVG(p.unit_price) AS DECIMAL(10,2))               AS prix_moyen_panier
FROM lakehouse.silver.fct_order_items o
INNER JOIN lakehouse.silver.dim_products p
        ON p.product_id    = o.product_id
       AND p.snapshot_date = o.order_date
INNER JOIN lakehouse.silver.dim_customers c
        ON c.customer_id   = o.customer_id
       AND c.snapshot_date = o.order_date
GROUP BY c.city, p.category_label
HAVING SUM(o.quantity * p.unit_price) > 0
ORDER BY chiffre_affaires DESC
LIMIT 20;


-- @query: Q6 — AGRÉGATION SUR TABLE GOLD : chiffre d'affaires mensuel
-- Exigence 5.4. Six mois d'historique agrégés par mois et par catégorie :
-- c'est la démonstration directe que la contrainte d'historisation (§4.1)
-- produit bien de la valeur analytique.
SELECT
    YEAR(order_date)                AS annee,
    MONTH(order_date)               AS mois,
    category_label                  AS categorie,
    SUM(nb_orders)                  AS commandes,
    SUM(nb_items_sold)              AS articles,
    CAST(SUM(revenue) AS DECIMAL(14,2))                    AS chiffre_affaires,
    CAST(AVG(avg_order_value) AS DECIMAL(10,2))            AS panier_moyen,
    COUNT(DISTINCT order_date)      AS jours_avec_donnees
FROM lakehouse.gold.gold_sales_by_category_daily
WHERE category_slug <> 'TOTAL'
GROUP BY YEAR(order_date), MONTH(order_date), category_label
ORDER BY annee, mois, chiffre_affaires DESC;


-- @query: Q7 — AGRÉGATION SUR TABLE GOLD : segmentation client RFM
-- Deuxième agrégation, sur l'autre table Gold à jointure inter-domaines.
-- Elle montre que la couche Gold sert des questions de nature très différente
-- à partir des mêmes couches Silver.
SELECT
    customer_segment                AS segment,
    COUNT(*)                        AS nb_clients,
    CAST(SUM(lifetime_value) AS DECIMAL(14,2))             AS valeur_totale,
    CAST(AVG(lifetime_value) AS DECIMAL(12,2))             AS valeur_moyenne,
    CAST(AVG(avg_order_value) AS DECIMAL(10,2))            AS panier_moyen,
    CAST(AVG(CAST(nb_orders AS DOUBLE)) AS DECIMAL(8,2))   AS commandes_moyennes,
    CAST(AVG(CAST(recency_days AS DOUBLE)) AS DECIMAL(8,1)) AS recence_moyenne_jours
FROM lakehouse.gold.gold_customer_360
GROUP BY customer_segment
ORDER BY valeur_totale DESC;


-- @query: Q8 — BONUS : tendance tarifaire sur la fenêtre historisée
-- Cette requête n'aurait aucun sens sans les 180 snapshots quotidiens : elle
-- exploite les moyennes mobiles et la détection de promotions calculées en
-- Gold. C'est la meilleure illustration de l'intérêt de l'historisation.
SELECT
    product_id,
    title                           AS produit,
    category_label                  AS categorie,
    MIN(price_min_window)           AS prix_plancher,
    MAX(price_max_window)           AS prix_plafond,
    CAST(AVG(price_volatility_30d) AS DECIMAL(10,3))       AS volatilite_moyenne,
    SUM(CASE WHEN is_promotion THEN 1 ELSE 0 END)          AS jours_en_promotion,
    MAX(cumulative_change_pct)      AS evolution_max_pct
FROM lakehouse.gold.gold_product_price_trend
GROUP BY product_id, title, category_label
ORDER BY jours_en_promotion DESC, volatilite_moyenne DESC
LIMIT 15;


-- @query: Q9 — BONUS : vue analytique publiée par Dremio
-- Les virtual datasets créés dans l'espace `analytics` (valorisés par le sujet)
-- masquent la complexité du schéma physique aux utilisateurs métier.
SELECT annee, mois, categorie, commandes, chiffre_affaires, panier_moyen
FROM analytics.v_synthese_mensuelle
ORDER BY annee, mois, chiffre_affaires DESC
LIMIT 20;
