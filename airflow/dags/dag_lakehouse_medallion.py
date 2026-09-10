"""DAG PRINCIPAL — construction du médaillon Bronze → Silver → Gold.

------------------------------------------------------------------------------
 PLACE DANS LA PLATEFORME
------------------------------------------------------------------------------
 Ce DAG ne parle jamais à FakeStoreAPI : il consomme uniquement ce que NiFi a
 déposé dans la zone brute MinIO. Cette séparation est volontaire et découle
 de la contrainte 3.1.2 du sujet (NiFi seul point d'ingestion). Elle a aussi
 une vertu pratique : on peut rejouer l'intégralité du médaillon autant de fois
 qu'on veut sans re-solliciter l'API publique.

------------------------------------------------------------------------------
 DÉCLENCHEMENT — OPTION A RETENUE (détection de nouvelles données)
------------------------------------------------------------------------------
 Le sujet propose deux options. Nous retenons l'OPTION A : le DAG commence par
 INSPECTER la zone brute et ne poursuit que si de nouvelles partitions sont
 apparues depuis la dernière construction.

 Pourquoi A plutôt que B (NiFi appelle l'API REST d'Airflow) :
   * la reconstitution de 6 mois d'historique impose de rejouer 180 dates. Avec
     l'option B, ce serait à NiFi de piloter ce backfill — or ce n'est pas son
     rôle, et il n'a ni gestion de dépendances ni reprise sur échec ;
   * un DAG qui observe l'état réel du stockage est robuste à une panne de
     NiFi, à un redémarrage, ou à un dépôt manuel de fichiers. Un DAG déclenché
     par événement perd silencieusement les données si l'événement est perdu ;
   * la relation « le pipeline dépend de la présence des données », et non
     « le pipeline dépend d'un signal », est plus facile à auditer.

 L'option B reste néanmoins implémentée côté NiFi (processeur InvokeHTTP vers
 l'API REST d'Airflow, désactivé par défaut) : elle est démontrée en vidéo
 comme variante événementielle, et documentée dans docs/architecture.md §6.

------------------------------------------------------------------------------
 STRUCTURE
------------------------------------------------------------------------------
   detecter_nouvelles_donnees
        └─> [bronze_products | bronze_users | bronze_carts]      (parallèle)
              └─> [silver_products | silver_customers]           (parallèle)
                    └─> silver_orders   (dépend des 2 dimensions)
                          └─> [gold_catalog_kpi | gold_price_trend]
                              [gold_sales_daily | gold_customer_360]
                                └─> controles_qualite
                                      └─> publication_ok

 Les trois jobs Bronze sont indépendants : un échec sur `carts` n'empêche pas
 `products` d'aboutir. En revanche `silver_orders` attend les deux dimensions,
 car les tables Gold qui en dépendent joignent les trois domaines.
"""

from __future__ import annotations

import pendulum

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowSkipException

from lakehouse_common import (
    DEFAULT_ARGS,
    DOMAINS,
    list_raw_dates,
    spark_task,
)

DOC_MD = __doc__

with DAG(
    dag_id="lakehouse_medallion",
    description="Bronze → Silver → Gold en tables Iceberg via Nessie",
    doc_md=DOC_MD,
    default_args=DEFAULT_ARGS,
    schedule="15 * * * *",  # toutes les heures à H+15, après l'ingestion NiFi
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # un seul médaillon à la fois : les couches sont chaînées
    tags=["lakehouse", "medaillon", "iceberg", "nessie", "dit"],
) as dag:

    # ----------------------------------------------------------------------- #
    #  Détection (Option A)
    # ----------------------------------------------------------------------- #
    @task(task_id="detecter_nouvelles_donnees")
    def detecter_nouvelles_donnees(mode: str = "incremental") -> dict:
        """Inspecte la zone brute et décide si le médaillon doit être construit.

        On ne compare pas ici avec le contenu de Bronze : cette comparaison
        fine est faite par chaque job Spark, qui a accès au catalogue. Le rôle
        de cette tâche est plus modeste et plus sûr : constater qu'il y a bien
        quelque chose à traiter, et publier l'inventaire dans XCom pour que la
        vidéo et les logs montrent exactement ce que le run a vu.
        """
        inventaire = {domain: list_raw_dates(domain) for domain in DOMAINS}
        resume = {d: len(v) for d, v in inventaire.items()}

        if not any(resume.values()):
            raise AirflowSkipException(
                "Zone brute vide — aucune ingestion NiFi constatée. "
                "Déclencher `fakestore_daily_ingestion` ou `fakestore_backfill_history`."
            )

        for domain, dates in inventaire.items():
            if dates:
                print(
                    f"  {domain:<10} : {len(dates):>3} partition(s) "
                    f"[{dates[0]} → {dates[-1]}]"
                )
            else:
                print(f"  {domain:<10} :   0 partition — domaine non alimenté")

        return {"mode": mode, "partitions": resume}

    detection = detecter_nouvelles_donnees()

    # ----------------------------------------------------------------------- #
    #  BRONZE — un job par domaine, exécutés en parallèle
    # ----------------------------------------------------------------------- #
    bronze = {
        domain: spark_task(
            task_id=f"bronze_{domain}",
            script="bronze/bronze_ingest.py",
            application_args=["--domain", domain, "--mode", "incremental"],
        )
        for domain in DOMAINS
    }

    bronze_termine = EmptyOperator(task_id="bronze_termine")

    # ----------------------------------------------------------------------- #
    #  SILVER — dimensions d'abord, faits ensuite
    # ----------------------------------------------------------------------- #
    silver_products = spark_task(
        task_id="silver_products",
        script="silver/silver_products.py",
        application_args=["--mode", "incremental"],
    )
    silver_customers = spark_task(
        task_id="silver_customers",
        script="silver/silver_customers.py",
        application_args=["--mode", "incremental"],
    )
    silver_orders = spark_task(
        task_id="silver_orders",
        script="silver/silver_orders.py",
        application_args=["--mode", "incremental"],
    )

    silver_termine = EmptyOperator(task_id="silver_termine")

    # ----------------------------------------------------------------------- #
    #  GOLD — recalcul complet, les quatre tables sont indépendantes
    # ----------------------------------------------------------------------- #
    gold_catalog_kpi = spark_task(
        task_id="gold_catalog_kpi", script="gold/gold_catalog_kpi.py"
    )
    gold_price_trend = spark_task(
        task_id="gold_price_trend", script="gold/gold_price_trend.py"
    )
    gold_sales_daily = spark_task(
        task_id="gold_sales_daily", script="gold/gold_sales_daily.py"
    )
    gold_customer_360 = spark_task(
        task_id="gold_customer_360", script="gold/gold_customer_360.py"
    )

    # ----------------------------------------------------------------------- #
    #  QUALITÉ — porte de sortie du pipeline
    # ----------------------------------------------------------------------- #
    controles_qualite = spark_task(
        task_id="controles_qualite",
        script="maintenance/data_quality_checks.py",
        application_args=["--fail-on-error"],
        retries=0,  # un contrôle qualité qui échoue ne doit pas être « réessayé »
    )

    publication_ok = EmptyOperator(task_id="publication_ok")

    # ----------------------------------------------------------------------- #
    #  Graphe de dépendances
    # ----------------------------------------------------------------------- #
    detection >> list(bronze.values()) >> bronze_termine

    bronze_termine >> [silver_products, silver_customers]
    [silver_products, silver_customers] >> silver_orders
    silver_orders >> silver_termine

    silver_termine >> [
        gold_catalog_kpi,
        gold_price_trend,
        gold_sales_daily,
        gold_customer_360,
    ] >> controles_qualite >> publication_ok
