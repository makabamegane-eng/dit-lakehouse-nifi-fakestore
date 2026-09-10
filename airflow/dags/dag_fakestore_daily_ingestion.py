"""DAG D'INGESTION QUOTIDIENNE — pilote NiFi, constate le dépôt, enchaîne.

------------------------------------------------------------------------------
 RÔLE
------------------------------------------------------------------------------
 C'est le régime de croisière de la plateforme : une fois par jour, une
 nouvelle photographie de FakeStoreAPI vient s'ajouter à l'historique.

 Airflow ne récupère JAMAIS les données lui-même : il demande à NiFi de le
 faire (un POST JSON sur le processeur ListenHTTP du flow), puis observe la
 zone brute MinIO jusqu'à constater le dépôt. NiFi reste ainsi l'unique point
 d'ingestion, conformément à la contrainte 3.1.2 du sujet, et Airflow garde son
 rôle d'ordonnanceur : planification, reprise sur échec, traçabilité.

------------------------------------------------------------------------------
 ENCHAÎNEMENT
------------------------------------------------------------------------------
   verifier_nifi → declencher_ingestion → attendre_depot
                 → verifier_volumetrie → declencher_medaillon

 `attendre_depot` est le point clé : sans lui, le DAG déclencherait le
 médaillon avant que les fichiers ne soient écrits, et le run se terminerait
 « vert » sans avoir rien traité — le pire des cas.
"""

from __future__ import annotations

import pendulum

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from lakehouse_common import (
    DEFAULT_ARGS,
    DOMAINS,
    count_raw_objects,
    nifi_health,
    trigger_nifi_ingestion,
    wait_for_ingestion,
)

DOC_MD = __doc__

with DAG(
    dag_id="fakestore_daily_ingestion",
    description="Déclenche l'ingestion NiFi FakeStoreAPI → MinIO pour la journée",
    doc_md=DOC_MD,
    default_args=DEFAULT_ARGS,
    schedule="0 * * * *",  # horaire : accélère la démonstration de bout en bout
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "nifi", "fakestore", "dit"],
) as dag:

    @task(task_id="verifier_nifi")
    def verifier_nifi() -> dict:
        """Échoue tôt et clairement si NiFi n'est pas joignable."""
        return {"heap": nifi_health().get("usedHeap")}

    @task(task_id="declencher_ingestion")
    def declencher_ingestion(**context) -> str:
        """Envoie la demande d'ingestion à NiFi pour la date logique du run.

        La date logique vient de `data_interval_start` et non de `now()` : un
        run rejoué manuellement pour le 3 mars ingérera bien dans la partition
        du 3 mars.
        """
        snapshot_date = context["data_interval_start"].format("YYYY-MM-DD")
        trigger_nifi_ingestion(
            snapshot_date=snapshot_date,
            domains=DOMAINS,
            run_id=context["run_id"],
        )
        return snapshot_date

    @task(task_id="attendre_depot")
    def attendre_depot(snapshot_date: str) -> dict:
        """Observe MinIO jusqu'au dépôt effectif (Option A du sujet)."""
        return wait_for_ingestion(snapshot_date, DOMAINS, timeout=300, poll_interval=5)

    @task(task_id="verifier_volumetrie")
    def verifier_volumetrie(snapshot_date: str, depots: dict) -> dict:
        """Contrôle de volumétrie avant de lancer le médaillon.

        Une ingestion qui « réussit » en déposant un fichier vide est un piège
        classique. On vérifie donc qu'au moins les deux domaines obligatoires
        du sujet ont bien reçu un objet.
        """
        obligatoires = ("products", "users")
        manquants = [d for d in obligatoires if depots.get(d, 0) == 0]
        if manquants:
            raise ValueError(
                f"Domaines obligatoires non alimentés pour {snapshot_date} : "
                f"{', '.join(manquants)}"
            )

        detail = {d: count_raw_objects(d, snapshot_date) for d in DOMAINS}
        print(f"Volumétrie déposée le {snapshot_date} : {detail}")
        return detail

    declencher_medaillon = TriggerDagRunOperator(
        task_id="declencher_medaillon",
        trigger_dag_id="lakehouse_medallion",
        wait_for_completion=False,  # découplage : l'ingestion n'attend pas le calcul
        reset_dag_run=True,
        conf={"declenche_par": "fakestore_daily_ingestion"},
    )

    sante = verifier_nifi()
    date_du_run = declencher_ingestion()
    depots = attendre_depot(date_du_run)
    volumetrie = verifier_volumetrie(date_du_run, depots)

    sante >> date_du_run
    volumetrie >> declencher_medaillon
