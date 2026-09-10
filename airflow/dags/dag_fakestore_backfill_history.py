"""DAG DE BACKFILL — reconstitution des 6 mois d'historique (contrainte 4.1).

------------------------------------------------------------------------------
 CE QUE FAIT CE DAG
------------------------------------------------------------------------------
 Il rejoue l'ingestion NiFi une fois par date logique sur une fenêtre de
 180 jours. Chaque itération est un VRAI appel HTTP à FakeStoreAPI, déposé
 dans sa propre partition `ingest_date=YYYY-MM-DD`. À l'arrivée, la zone brute
 contient 180 snapshots authentiques et horodatés — la matière première de
 l'historique.

 C'est le seul DAG à déclenchement manuel : reconstituer l'historique est une
 opération d'amorçage, pas une routine. Le régime permanent, c'est
 `fakestore_daily_ingestion`.

------------------------------------------------------------------------------
 POURQUOI UN SEUL TASK QUI BOUCLE, ET NON 180 DAG RUNS EN CATCHUP
------------------------------------------------------------------------------
 La solution « idiomatique » Airflow serait `catchup=True` avec un start_date à
 J-180. Elle a été écartée à l'usage :
   * 180 DAG runs saturent l'ordonnanceur et l'interface pendant plusieurs
     minutes, ce qui rend la démonstration vidéo illisible ;
   * chaque run porterait un coût fixe (planification, création de processus)
     bien supérieur au travail réel — 3 appels HTTP ;
   * la reprise après incident est plus simple ici : le paramètre `skip_existing`
     saute les dates déjà présentes dans MinIO, donc relancer le DAG après une
     interruption reprend exactement où il s'était arrêté.

 La boucle reste néanmoins observable : progression journalisée tous les 10 %,
 et une tâche de vérification finale contrôle la couverture obtenue.

------------------------------------------------------------------------------
 PARAMÈTRES (modifiables au déclenchement, via « Trigger DAG w/ config »)
------------------------------------------------------------------------------
   history_days   : profondeur en jours          (défaut 180)
   end_date       : dernier jour de la fenêtre   (défaut : aujourd'hui)
   throttle_ms    : pause entre deux dates       (défaut 250 ms)
   skip_existing  : ignorer les dates déjà là    (défaut true)
   run_medallion  : enchaîner le médaillon       (défaut true)
"""

from __future__ import annotations

import time

import pendulum

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from lakehouse_common import (
    DEFAULT_ARGS,
    DOMAINS,
    HISTORY_DAYS,
    count_raw_objects,
    history_dates,
    list_raw_dates,
    nifi_health,
    trigger_nifi_ingestion,
    wait_for_ingestion,
)

DOC_MD = __doc__

BACKFILL_ARGS = dict(DEFAULT_ARGS)
BACKFILL_ARGS["execution_timeout"] = pendulum.duration(hours=3)
BACKFILL_ARGS["retries"] = 1

with DAG(
    dag_id="fakestore_backfill_history",
    description="Reconstitue ~6 mois d'historique en rejouant l'ingestion NiFi",
    doc_md=DOC_MD,
    default_args=BACKFILL_ARGS,
    schedule=None,  # amorçage manuel uniquement
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    params={
        "history_days": Param(HISTORY_DAYS, type="integer", minimum=1, maximum=400),
        "end_date": Param("today", type="string"),
        "throttle_ms": Param(250, type="integer", minimum=0, maximum=5000),
        "skip_existing": Param(True, type="boolean"),
        "run_medallion": Param(True, type="boolean"),
    },
    tags=["ingestion", "nifi", "backfill", "historique", "dit"],
) as dag:

    @task(task_id="preparer_fenetre")
    def preparer_fenetre(**context) -> dict:
        """Calcule la liste des dates à ingérer et retire celles déjà présentes."""
        params = context["params"]
        nifi_health()

        toutes = history_dates(params["end_date"], int(params["history_days"]))
        deja_la = set(list_raw_dates("products")) if params["skip_existing"] else set()
        a_faire = [d for d in toutes if d not in deja_la]

        print(f"Fenêtre demandée   : {toutes[0]} → {toutes[-1]} ({len(toutes)} jours)")
        print(f"Déjà dans MinIO    : {len(deja_la)} date(s)")
        print(f"Restant à ingérer  : {len(a_faire)} date(s)")

        return {
            "dates": a_faire,
            "fenetre": [toutes[0], toutes[-1]],
            "throttle_ms": int(params["throttle_ms"]),
        }

    @task(task_id="ingerer_historique")
    def ingerer_historique(plan: dict) -> dict:
        """Rejoue l'ingestion NiFi date par date, du plus ancien au plus récent.

        L'ordre chronologique n'est pas cosmétique : il fait que les
        horodatages `_ingest_ts` réels sont croissants avec la date logique,
        ce qui rend l'historique cohérent à l'audit.
        """
        dates = plan["dates"]
        if not dates:
            print("Historique déjà complet — rien à faire.")
            return {"ingerees": 0, "echecs": []}

        pause = plan["throttle_ms"] / 1000.0
        jalon = max(1, len(dates) // 10)
        echecs: list[str] = []

        for index, snapshot_date in enumerate(dates, start=1):
            try:
                trigger_nifi_ingestion(
                    snapshot_date=snapshot_date,
                    domains=DOMAINS,
                    run_id=f"backfill-{snapshot_date}",
                )
            except Exception as exc:  # noqa: BLE001
                # Une date en échec ne doit pas interrompre 180 jours de travail :
                # on la consigne et la vérification finale la fera remonter.
                echecs.append(snapshot_date)
                print(f"  ! échec sur {snapshot_date} : {exc}")

            if index % jalon == 0 or index == len(dates):
                print(f"  progression : {index}/{len(dates)} ({100 * index // len(dates)} %)")

            if pause:
                time.sleep(pause)

        # La dernière date sert de témoin : si NiFi a suivi le rythme jusqu'au
        # bout, l'ensemble du lot est très probablement arrivé.
        wait_for_ingestion(dates[-1], DOMAINS, timeout=180, poll_interval=5)
        return {"ingerees": len(dates) - len(echecs), "echecs": echecs}

    @task(task_id="verifier_couverture")
    def verifier_couverture(plan: dict, bilan: dict) -> dict:
        """Contrôle la couverture réelle de la fenêtre dans la zone brute."""
        debut, fin = plan["fenetre"]
        couverture = {}
        for domaine in DOMAINS:
            dates = [d for d in list_raw_dates(domaine) if debut <= d <= fin]
            couverture[domaine] = len(dates)
            print(f"  {domaine:<10} : {len(dates):>3} partition(s) sur la fenêtre")

        attendu = (
            pendulum.parse(fin).diff(pendulum.parse(debut)).in_days() + 1
        )
        taux = min(couverture.values()) / attendu if attendu else 0

        if bilan["echecs"]:
            print(f"Dates en échec au déclenchement : {bilan['echecs']}")

        # Seuil à 95 % : quelques trous ponctuels (timeout réseau sur l'API
        # publique) sont tolérables, un historique amputé ne l'est pas.
        if taux < 0.95:
            raise ValueError(
                f"Couverture insuffisante : {taux:.0%} de la fenêtre "
                f"({attendu} jours attendus). Relancer le DAG — les dates déjà "
                "ingérées seront automatiquement sautées."
            )

        print(f"Couverture de l'historique : {taux:.0%} — objectif atteint")
        print(f"Objets sur la dernière date : {count_raw_objects('products', fin)}")
        return {"couverture": couverture, "taux": round(taux, 3)}

    construire_medaillon = TriggerDagRunOperator(
        task_id="construire_medaillon",
        trigger_dag_id="lakehouse_medallion",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        conf={"declenche_par": "fakestore_backfill_history"},
    )

    plan = preparer_fenetre()
    bilan = ingerer_historique(plan)
    verifier_couverture(plan, bilan) >> construire_medaillon
