#!/usr/bin/env python3
"""Construction du dataflow d'ingestion NiFi — FakeStoreAPI → MinIO.

==============================================================================
 POURQUOI CE FLOW EST CONSTRUIT PAR SCRIPT
==============================================================================
 Un flow NiFi dessiné à la souris n'existe que dans le conteneur qui l'héberge.
 Le construire par l'API REST donne trois choses qu'un export XML/JSON seul ne
 donne pas :
   * REPRODUCTIBILITÉ — `docker compose down -v` puis `make nifi-flow` restitue
     exactement le même flow, sans clic ;
   * REVUE DE CODE — chaque processeur, propriété et relation est lisible et
     versionnable en clair dans Git ;
   * DOCUMENTATION — le script EST la description du flow.
 L'export `.json` exigé par le sujet est produit à la fin par export_flow.py,
 depuis l'instance réellement construite.

==============================================================================
 TOPOLOGIE DU DATAFLOW
==============================================================================

  [1] ListenHTTP ............ point d'entrée : Airflow POSTe {snapshot_date,
      « recevoir-demande »      domains, run_id} sur /ingest
                 |
  [2] EvaluateJsonPath ...... extrait snapshot.date et run.id en attributs
      « extraire-contexte »     (les attributs survivent au SplitJson suivant)
                 |
  [3] SplitJson ............. éclate le tableau `domains` : 1 FlowFile par
      « eclater-domaines »      domaine => les 3 appels API partent en parallèle
                 |
  [4] EvaluateJsonPath ...... pose l'attribut `domain`
      « extraire-domaine »
                 |
  [5] InvokeHTTP ............ GET ${api.base.url}/${domain}
      « appeler-fakestore »     seule sortie vers l'extérieur du système
                 |
  [6] RouteOnAttribute ...... CONTRÔLE MINIMAL (et non transformation) :
      « controler-reponse »     code 200 + charge utile non vide + JSON tableau
                 |
  [7] DetectDuplicate ....... IDEMPOTENCE (bonus 7.2) : rejette un couple
      « detecter-doublons »     (domaine, date) déjà ingéré, via le
                 |              DistributedMapCache
  [8] UpdateAttribute ....... construit la clé d'objet S3 et l'horodatage réel
      « preparer-cle-objet »    de l'appel
                 |
  [9] PutS3Object ........... dépôt dans la zone brute MinIO
      « deposer-minio »
                 |
 [10] InvokeHTTP ............ OPTION B du sujet (déclenchement événementiel de
      « notifier-airflow »      l'API REST Airflow) — DÉSACTIVÉ par défaut,
                                l'Option A est celle retenue. Présent pour être
                                démontré en vidéo.

 GESTION DES ERREURS — aucun chemin ne se termine par une perte silencieuse :
   * InvokeHTTP (Retry / No Retry / Failure) → RetryFlowFile → 3 tentatives
     espacées, puis dépôt dans `_dead_letter/` ;
   * réponse non conforme (route `unmatched`) → `_dead_letter/` ;
   * échec de dépôt S3 → RetryFlowFile → `_dead_letter/` ;
   * doublon détecté → journalisé puis abandonné volontairement (ce n'est pas
     une erreur, c'est le comportement attendu d'une ré-ingestion).

==============================================================================
 USAGE
==============================================================================
   python nifi/scripts/build_flow.py                 # construit et démarre
   python nifi/scripts/build_flow.py --no-start      # construit sans démarrer
   python nifi/scripts/build_flow.py --recreate      # détruit puis reconstruit
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Dict, List

sys.path.insert(0, str(Path(__file__).resolve().parent))

from nifi_client import NiFiClient, NiFiError  # noqa: E402

# --------------------------------------------------------------------------- #
#  Configuration
# --------------------------------------------------------------------------- #
NIFI_URL = os.environ.get("NIFI_URL", "http://localhost:8080")
PG_NAME = "FakeStoreAPI Ingestion"
PARAM_CONTEXT_NAME = "fakestore-ingestion"
CACHE_SERVICE_NAME = "cache-idempotence"
AWS_SERVICE_NAME = "minio-credentials"

PARAMETERS = [
    ("api.base.url", os.environ.get("FAKESTORE_API_BASE_URL", "https://fakestoreapi.com"),
     "URL de base de la source imposee FakeStoreAPI", False),
    ("minio.endpoint", os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
     "Endpoint S3 - NOM DE SERVICE DOCKER, jamais localhost", False),
    ("minio.bucket.raw", os.environ.get("RAW_BUCKET", "lakehouse-raw"),
     "Bucket de la zone brute", False),
    ("minio.region", os.environ.get("MINIO_REGION", "us-east-1"),
     "Region S3 declaree (MinIO l'ignore mais le SDK l'exige)", False),
    ("minio.access.key", os.environ.get("MINIO_ROOT_USER", "lakehouse"),
     "Identifiant MinIO - sensible : la propriete Access Key ID de NiFi l'exige", True),
    ("minio.secret.key", os.environ.get("MINIO_ROOT_PASSWORD", "lakehouse123"),
     "Secret MinIO - parametre sensible, chiffre par NiFi", True),
    ("listen.port", os.environ.get("NIFI_LISTEN_HTTP_PORT", "9095"),
     "Port d'ecoute du declenchement HTTP", False),
    ("airflow.trigger.url",
     "http://airflow-webserver:8080/api/v1/dags/lakehouse_medallion/dagRuns",
     "Option B - API REST Airflow (processeur desactive par defaut)", False),
    ("airflow.auth", "YWRtaW46YWRtaW4=",
     "Basic auth Airflow encodee (admin:admin) pour l'option B", True),
]

# Grille de positionnement dans le canevas NiFi : le flow doit être LISIBLE
# quand on l'ouvre pour la démonstration vidéo.
COL = 420
ROW = 190


def pos(col: int, row: int) -> Dict[str, float]:
    return {"x": 40.0 + col * COL, "y": 40.0 + row * ROW}


# --------------------------------------------------------------------------- #
#  Construction
# --------------------------------------------------------------------------- #
class FlowBuilder:
    def __init__(self, client: NiFiClient) -> None:
        self.nifi = client
        self.root_id: str = ""
        self.pg_id: str = ""
        self.processors: Dict[str, str] = {}
        self.services: Dict[str, str] = {}
        self.used_relationships: Dict[str, set] = {}

    # ----------------------------------------------------------- Nettoyage  #
    def find_process_group(self, name: str) -> str | None:
        flow = self.nifi.get(f"/flow/process-groups/{self.root_id}")
        for group in flow["processGroupFlow"]["flow"]["processGroups"]:
            if group["component"]["name"] == name:
                return group["id"]
        return None

    def delete_process_group(self, pg_id: str) -> None:
        print(f"    arrêt du groupe existant {pg_id}")
        self.nifi.put(
            f"/flow/process-groups/{pg_id}",
            {"id": pg_id, "state": "STOPPED", "disconnectedNodeAcknowledged": False},
        )
        time.sleep(3)
        # Les services de contrôle doivent être désactivés avant suppression
        try:
            self.nifi.put(
                f"/flow/process-groups/{pg_id}/controller-services",
                {"id": pg_id, "state": "DISABLED"},
            )
            time.sleep(2)
        except NiFiError:
            pass
        # Vider les files d'attente, sinon NiFi refuse la suppression
        try:
            self.nifi.post(f"/process-groups/{pg_id}/empty-all-connections-requests", {})
            time.sleep(2)
        except NiFiError:
            pass

        revision = self.nifi.revision_of(f"/process-groups/{pg_id}")
        self.nifi.delete(
            f"/process-groups/{pg_id}",
            params={"version": revision["version"], "clientId": revision["clientId"]},
        )
        print("    groupe supprimé")

    # ------------------------------------------------- Contexte de paramètres #
    def ensure_parameter_context(self) -> str:
        existing = self.nifi.get("/flow/parameter-contexts")
        for context in existing.get("parameterContexts", []):
            if context["component"]["name"] == PARAM_CONTEXT_NAME:
                revision = self.nifi.revision_of(
                    f"/parameter-contexts/{context['id']}"
                )
                self.nifi.delete(
                    f"/parameter-contexts/{context['id']}",
                    params={
                        "version": revision["version"],
                        "clientId": revision["clientId"],
                    },
                )
                print("    ancien contexte de paramètres supprimé")
                break

        payload = {
            "revision": self.nifi.new_revision(),
            "component": {
                "name": PARAM_CONTEXT_NAME,
                "description": (
                    "Parametres du flow d'ingestion FakeStoreAPI. Rend le flow "
                    "reutilisable avec une autre API sans toucher aux processeurs "
                    "(bonus 7.4 du sujet)."
                ),
                "parameters": [
                    {
                        "parameter": {
                            "name": name,
                            "value": value,
                            "description": description,
                            "sensitive": sensitive,
                        }
                    }
                    for name, value, description, sensitive in PARAMETERS
                ],
            },
        }
        context = self.nifi.post("/parameter-contexts", payload)
        print(f"    contexte « {PARAM_CONTEXT_NAME} » créé ({len(PARAMETERS)} paramètres)")
        return context["id"]

    # ------------------------------------------------------ Groupe de flux  #
    def create_process_group(self, context_id: str) -> str:
        payload = {
            "revision": self.nifi.new_revision(),
            "component": {
                "name": PG_NAME,
                "position": {"x": 0.0, "y": 0.0},
                "comments": (
                    "Ingestion FakeStoreAPI vers la zone brute MinIO. "
                    "Collecte, controle et depot uniquement - aucune "
                    "transformation metier (contrainte 3.1.3 du sujet)."
                ),
            },
        }
        group = self.nifi.post(f"/process-groups/{self.root_id}/process-groups", payload)
        pg_id = group["id"]

        revision = self.nifi.revision_of(f"/process-groups/{pg_id}")
        self.nifi.put(
            f"/process-groups/{pg_id}",
            {
                "revision": revision,
                "component": {"id": pg_id, "parameterContext": {"id": context_id}},
            },
        )
        print(f"    groupe « {PG_NAME} » créé et rattaché au contexte")
        return pg_id

    # -------------------------------------------------- Services de contrôle #
    def create_controller_service(
        self, name: str, type_name: str, properties: Dict[str, str]
    ) -> str:
        payload = {
            "revision": self.nifi.new_revision(),
            "component": {
                "name": name,
                "type": type_name,
                "bundle": self.nifi.resolve_bundle("controller-service", type_name),
            },
        }
        service = self.nifi.post(
            f"/process-groups/{self.pg_id}/controller-services", payload
        )
        service_id = service["id"]

        resolved = self.nifi.resolve_properties(
            f"/controller-services/{service_id}", properties
        )
        revision = self.nifi.revision_of(f"/controller-services/{service_id}")
        self.nifi.put(
            f"/controller-services/{service_id}",
            {
                "revision": revision,
                "component": {"id": service_id, "properties": resolved},
            },
        )
        self.services[name] = service_id
        print(f"    service « {name} » créé")
        return service_id

    def enable_services(self) -> None:
        for name, service_id in self.services.items():
            revision = self.nifi.revision_of(f"/controller-services/{service_id}")
            self.nifi.put(
                f"/controller-services/{service_id}/run-status",
                {"revision": revision, "state": "ENABLED"},
            )
            print(f"    service « {name} » activé")
        time.sleep(4)

    # -------------------------------------------------------- Processeurs   #
    def create_processor(
        self,
        key: str,
        name: str,
        type_name: str,
        position: Dict[str, float],
        properties: Dict[str, str] | None = None,
        scheduling: Dict[str, str] | None = None,
        comments: str = "",
    ) -> str:
        payload = {
            "revision": self.nifi.new_revision(),
            "component": {
                "name": name,
                "type": type_name,
                "bundle": self.nifi.resolve_bundle("processor", type_name),
                "position": position,
                "config": {"comments": comments},
            },
        }
        processor = self.nifi.post(f"/process-groups/{self.pg_id}/processors", payload)
        processor_id = processor["id"]
        self.processors[key] = processor_id
        self.used_relationships[key] = set()

        config: Dict[str, object] = {}
        if properties:
            config["properties"] = self.nifi.resolve_properties(
                f"/processors/{processor_id}", properties
            )
        if scheduling:
            config.update(scheduling)

        if config:
            revision = self.nifi.revision_of(f"/processors/{processor_id}")
            self.nifi.put(
                f"/processors/{processor_id}",
                {"revision": revision, "component": {"id": processor_id, "config": config}},
            )
        print(f"    processeur « {name} »")
        return processor_id

    # -------------------------------------------------------- Connexions    #
    def connect(
        self, source_key: str, relationships: List[str], target_key: str, label: str = ""
    ) -> None:
        payload = {
            "revision": self.nifi.new_revision(),
            "component": {
                "name": label,
                "source": {
                    "id": self.processors[source_key],
                    "groupId": self.pg_id,
                    "type": "PROCESSOR",
                },
                "destination": {
                    "id": self.processors[target_key],
                    "groupId": self.pg_id,
                    "type": "PROCESSOR",
                },
                "selectedRelationships": relationships,
                # Garde-fou : une file qui gonfle indéfiniment finit par saturer
                # le content repository. 10 000 FlowFiles / 1 Go, puis backpressure.
                "backPressureObjectThreshold": 10000,
                "backPressureDataSizeThreshold": "1 GB",
                "flowFileExpiration": "0 sec",
            },
        }
        self.nifi.post(f"/process-groups/{self.pg_id}/connections", payload)
        self.used_relationships[source_key].update(relationships)

    def auto_terminate_unused(self) -> None:
        """Auto-termine toute relation non connectée.

        NiFi refuse de démarrer un processeur dont une relation n'est ni
        connectée ni auto-terminée. Plutôt que d'énumérer à la main les
        relations à terminer — source classique d'oubli —, on déduit celles qui
        restent après la construction du graphe.
        """
        for key, processor_id in self.processors.items():
            entity = self.nifi.get(f"/processors/{processor_id}")
            toutes = {r["name"] for r in entity["component"]["relationships"]}
            restantes = sorted(toutes - self.used_relationships[key])
            if not restantes:
                continue
            revision = self.nifi.revision_of(f"/processors/{processor_id}")
            self.nifi.put(
                f"/processors/{processor_id}",
                {
                    "revision": revision,
                    "component": {
                        "id": processor_id,
                        "config": {"autoTerminatedRelationships": restantes},
                    },
                },
            )
            print(f"    auto-terminé sur « {key} » : {', '.join(restantes)}")

    # ----------------------------------------------------------- Démarrage  #
    def start(self, keep_stopped: List[str]) -> None:
        self.nifi.put(
            f"/flow/process-groups/{self.pg_id}",
            {"id": self.pg_id, "state": "RUNNING"},
        )
        time.sleep(3)
        for key in keep_stopped:
            processor_id = self.processors[key]
            revision = self.nifi.revision_of(f"/processors/{processor_id}")
            self.nifi.put(
                f"/processors/{processor_id}/run-status",
                {"revision": revision, "state": "STOPPED"},
            )
            print(f"    « {key} » laissé à l'arrêt (activation manuelle en démo)")


# --------------------------------------------------------------------------- #
#  Définition du dataflow
# --------------------------------------------------------------------------- #
def build(builder: FlowBuilder) -> None:
    # ---- Services de contrôle ---------------------------------------------- #
    print("\n[3/7] Services de contrôle")
    builder.create_controller_service(
        AWS_SERVICE_NAME,
        "org.apache.nifi.processors.aws.credentials.provider.service."
        "AWSCredentialsProviderControllerService",
        {
            "Access Key": "#{minio.access.key}",
            "Secret Key": "#{minio.secret.key}",
        },
    )
    builder.create_controller_service(
        "cache-serveur",
        "org.apache.nifi.distributed.cache.server.map.DistributedMapCacheServer",
        {"Port": "4557"},
    )
    builder.create_controller_service(
        CACHE_SERVICE_NAME,
        "org.apache.nifi.distributed.cache.client.DistributedMapCacheClientService",
        {"Server Hostname": "localhost", "Server Port": "4557"},
    )
    builder.enable_services()

    # ---- Processeurs -------------------------------------------------------- #
    print("\n[4/7] Processeurs")

    builder.create_processor(
        "listen",
        "1. recevoir-demande (ListenHTTP)",
        "org.apache.nifi.processors.standard.ListenHTTP",
        pos(0, 0),
        properties={
            "Listening Port": "#{listen.port}",
            "Base Path": "ingest",
            "Max Data to Receive per Second": "10 MB",
        },
        comments=(
            "Point d'entree du flow. Airflow POSTe ici un JSON "
            "{snapshot_date, domains, run_id}. Choix d'un declenchement pousse "
            "plutot qu'un GenerateFlowFile planifie : la date logique du "
            "snapshot est portee par l'appelant, ce qui rend le backfill de "
            "6 mois possible sans toucher au flow."
        ),
    )

    builder.create_processor(
        "contexte",
        "2. extraire-contexte (EvaluateJsonPath)",
        "org.apache.nifi.processors.standard.EvaluateJsonPath",
        pos(1, 0),
        properties={
            "Destination": "flowfile-attribute",
            "Return Type": "scalar",
            "Path Not Found Behavior": "warn",
            "snapshot.date": "$.snapshot_date",
            "run.id": "$.run_id",
        },
        comments=(
            "Extrait la date logique AVANT l'eclatement par domaine : les "
            "attributs d'un FlowFile sont herites par tous ses fragments, "
            "la date suit donc les 3 branches sans etre relue."
        ),
    )

    builder.create_processor(
        "split",
        "3. eclater-domaines (SplitJson)",
        "org.apache.nifi.processors.standard.SplitJson",
        pos(2, 0),
        properties={"JsonPath Expression": "$.domains"},
        comments=(
            "Un FlowFile par domaine demande. Les 3 appels API partent ainsi "
            "en parallele et un echec sur /carts n'empeche pas /products "
            "d'aboutir."
        ),
    )

    builder.create_processor(
        "domaine",
        "4. extraire-domaine (ExtractText)",
        "org.apache.nifi.processors.standard.ExtractText",
        pos(3, 0),
        properties={
            # SplitJson ecrit l'element de tableau tel quel ("products" sans
            # guillemets) : ce n'est pas du JSON, donc EvaluateJsonPath echoue.
            # ExtractText lit le contenu comme du texte et promeut le domaine.
            "domain": "([A-Za-z]+)",
        },
        comments="Le fragment contient la chaine du domaine : on la promeut en attribut (lecture texte).",
    )

    builder.create_processor(
        "invoke",
        "5. appeler-fakestore (InvokeHTTP)",
        "org.apache.nifi.processors.standard.InvokeHTTP",
        pos(4, 0),
        properties={
            "HTTP Method": "GET",
            "HTTP URL": "#{api.base.url}/${domain}",
            "Connection Timeout": "10 secs",
            "Read Timeout": "20 secs",
            "Include Date Header": "True",
            # FakeStoreAPI est derriere Cloudflare : sans User-Agent de
            # navigateur, la requete recoit une page de defi 403 « Just a
            # moment ». On renseigne la propriete NATIVE Useragent d'InvokeHTTP.
            "Useragent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        },
        comments=(
            "Seul point de sortie du systeme vers Internet. L'URL est "
            "parametree : changer #{api.base.url} suffit pour brancher le flow "
            "sur une autre API REST."
        ),
    )

    builder.create_processor(
        "controle",
        "6. controler-reponse (RouteOnAttribute)",
        "org.apache.nifi.processors.standard.RouteOnAttribute",
        pos(5, 0),
        properties={
            "Routing Strategy": "Route to Property name",
            "conforme": (
                "${invokehttp.status.code:equals(200)"
                ":and(${fileSize:gt(20)})"
                ":and(${mime.type:contains('json')})}"
            ),
        },
        comments=(
            "CONTROLE MINIMAL exige par le sujet, et rien de plus : code HTTP, "
            "charge utile non vide, type MIME JSON. Aucune jointure, aucun "
            "calcul, aucun agregat - la transformation metier appartient a "
            "Spark (contrainte 3.1.3)."
        ),
    )

    builder.create_processor(
        "doublon",
        "7. detecter-doublons (DetectDuplicate)",
        "org.apache.nifi.processors.standard.DetectDuplicate",
        pos(6, 0),
        properties={
            "Cache Entry Identifier": "${domain}::${snapshot.date}",
            "Distributed Cache Service": builder.services[CACHE_SERVICE_NAME],
            "Age Off Duration": "24 hours",
            "FlowFile Description": "${domain} du ${snapshot.date}",
        },
        comments=(
            "IDEMPOTENCE (bonus 7.2). Cle = domaine + date logique : rejouer "
            "une ingestion deja faite n'ecrit pas un second objet dans MinIO. "
            "Peremption a 24 h pour qu'une reprise le lendemain reste possible."
        ),
    )

    builder.create_processor(
        "cle",
        "8. preparer-cle-objet (UpdateAttribute)",
        "org.apache.nifi.processors.attributes.UpdateAttribute",
        pos(7, 0),
        properties={
            "ingest.epoch": "${now():toNumber()}",
            # UpdateAttribute evalue ses proprietes contre les attributs
            # d'ORIGINE : une propriete ne peut pas referencer une autre posee
            # dans le meme processeur. On construit donc la cle S3 de facon
            # autonome (sans ${filename} ni ${ingest.epoch}), sinon la cle
            # reprend l'UUID d'origine et Spark ne peut plus deduire la date.
            "filename": "${domain}_${snapshot.date}_${now():toNumber()}_${UUID()}.json",
            "s3.key": (
                "fakestore/${domain}/ingest_date=${snapshot.date}/"
                "${domain}_${snapshot.date}_${now():toNumber()}_${UUID()}.json"
            ),
            "source.api": "#{api.base.url}/${domain}",
            "ingest.run.id": "${run.id}",
        },
        comments=(
            "Convention de nommage concue ici (rien n'est impose par le sujet) :\n"
            "  fakestore/<domaine>/ingest_date=YYYY-MM-DD/<domaine>_<date>_<epoch>_<uuid>.json\n"
            "  * le prefixe ingest_date= est reconnu nativement par Spark comme "
            "une partition Hive ;\n"
            "  * l'epoch conserve l'instant REEL de l'appel, distinct de la date "
            "logique - indispensable pour auditer un backfill ;\n"
            "  * l'UUID rend deux executions du meme jour non destructrices."
        ),
    )

    builder.create_processor(
        "depot",
        "9. deposer-minio (PutS3Object)",
        "org.apache.nifi.processors.aws.s3.PutS3Object",
        pos(8, 0),
        properties={
            "Bucket": "#{minio.bucket.raw}",
            "Object Key": "${s3.key}",
            "Region": "#{minio.region}",
            "Endpoint Override URL": "#{minio.endpoint}",
            "use-path-style-access": "true",
            "AWS Credentials Provider service": builder.services[AWS_SERVICE_NAME],
            "Content Type": "application/json",
        },
        comments=(
            "SEUL point technique impose par le sujet : l'endpoint pointe sur le "
            "NOM DE SERVICE DOCKER (http://minio:9000) et non localhost - depuis "
            "le conteneur NiFi, localhost designerait NiFi lui-meme.\n"
            "Le path-style access est obligatoire avec MinIO : le virtual-hosted "
            "style d'AWS supposerait un DNS du type bucket.minio."
        ),
    )

    builder.create_processor(
        "reessai_api",
        "R1. reessayer-appel (RetryFlowFile)",
        "org.apache.nifi.processors.standard.RetryFlowFile",
        pos(4, 1),
        properties={
            "Maximum Retries": "3",
            "Retry Attribute": "api.retry.count",
            "Penalize on Retry": "true",
        },
        comments=(
            "L'API publique peut renvoyer un 429 ou une coupure reseau. Trois "
            "tentatives penalisees avant abandon, plutot qu'un echec immediat."
        ),
    )

    builder.create_processor(
        "reessai_s3",
        "R2. reessayer-depot (RetryFlowFile)",
        "org.apache.nifi.processors.standard.RetryFlowFile",
        pos(8, 1),
        properties={
            "Maximum Retries": "3",
            "Retry Attribute": "s3.retry.count",
            "Penalize on Retry": "true",
        },
        comments="MinIO peut etre momentanement indisponible au demarrage de la pile.",
    )

    builder.create_processor(
        "rejet",
        "X. archiver-rejet (PutS3Object)",
        "org.apache.nifi.processors.aws.s3.PutS3Object",
        pos(6, 2),
        properties={
            "Bucket": "#{minio.bucket.raw}",
            "Object Key": (
                "_dead_letter/${domain:isEmpty():ifElse('inconnu', ${domain})}/"
                "${snapshot.date:isEmpty():ifElse('sans-date', ${snapshot.date})}/"
                "${uuid}.json"
            ),
            "Region": "#{minio.region}",
            "Endpoint Override URL": "#{minio.endpoint}",
            "use-path-style-access": "true",
            "AWS Credentials Provider service": builder.services[AWS_SERVICE_NAME],
        },
        comments=(
            "FILE DE REBUT. Tout ce qui echoue finit ici, avec son contenu et "
            "ses attributs : rien n'est perdu silencieusement. La zone "
            "_dead_letter/ est ignoree par Spark (prefixe hors du chemin "
            "fakestore/), elle sert au diagnostic."
        ),
    )

    builder.create_processor(
        "journal_doublon",
        "L1. journaliser-doublon (LogAttribute)",
        "org.apache.nifi.processors.standard.LogAttribute",
        pos(6, 1),
        properties={
            "Log Level": "info",
            "Log prefix": "DOUBLON IGNORE",
            "Attributes to Log": "domain, snapshot.date, ingest.run.id",
        },
        comments=(
            "Un doublon n'est pas une erreur : c'est la preuve que "
            "l'idempotence fonctionne. On le trace, on ne le rejoue pas."
        ),
    )

    builder.create_processor(
        "notifier",
        "10. notifier-airflow (InvokeHTTP) - OPTION B",
        "org.apache.nifi.processors.standard.InvokeHTTP",
        pos(9, 0),
        properties={
            "HTTP Method": "POST",
            "HTTP URL": "#{airflow.trigger.url}",
            "Request Content-Type": "application/json",
            "Connection Timeout": "10 secs",
            "Read Timeout": "15 secs",
        },
        comments=(
            "OPTION B du sujet - declenchement evenementiel : NiFi appelle "
            "l'API REST d'Airflow des la fin du depot.\n"
            "PROCESSEUR VOLONTAIREMENT A L'ARRET. L'option retenue est "
            "l'option A (Airflow observe la zone brute), pour les raisons "
            "detaillees dans le DAG lakehouse_medallion. Il est conserve ici "
            "pour etre demarre en direct pendant la soutenance et montrer que "
            "les deux modes sont operationnels."
        ),
    )

    # ---- Connexions --------------------------------------------------------- #
    print("\n[5/7] Connexions")
    builder.connect("listen", ["success"], "contexte", "demande recue")
    builder.connect("contexte", ["matched"], "split", "contexte extrait")
    builder.connect("contexte", ["failure", "unmatched"], "rejet", "JSON de demande invalide")
    builder.connect("split", ["split"], "domaine", "1 FlowFile par domaine")
    builder.connect("split", ["failure"], "rejet", "tableau domains illisible")
    builder.connect("domaine", ["matched"], "invoke", "domaine identifie")
    builder.connect("domaine", ["unmatched"], "rejet", "domaine absent")

    builder.connect("invoke", ["Response"], "controle", "reponse HTTP")
    builder.connect("invoke", ["Retry", "No Retry", "Failure"], "reessai_api", "appel en echec")
    builder.connect("reessai_api", ["retry"], "invoke", "nouvelle tentative")
    builder.connect(
        "reessai_api", ["retries_exceeded", "failure"], "rejet", "abandon apres 3 essais"
    )

    builder.connect("controle", ["conforme"], "doublon", "reponse conforme")
    builder.connect("controle", ["unmatched"], "rejet", "reponse non conforme")

    builder.connect("doublon", ["non-duplicate"], "cle", "premiere ingestion")
    builder.connect("doublon", ["duplicate"], "journal_doublon", "deja ingere")
    builder.connect("doublon", ["failure"], "rejet", "cache indisponible")

    builder.connect("cle", ["success"], "depot", "cle S3 construite")

    builder.connect("depot", ["success"], "notifier", "objet depose")
    builder.connect("depot", ["failure"], "reessai_s3", "depot en echec")
    builder.connect("reessai_s3", ["retry"], "depot", "nouvelle tentative")
    builder.connect(
        "reessai_s3", ["retries_exceeded", "failure"], "rejet", "abandon du depot"
    )

    print("\n[6/7] Auto-terminaison des relations libres")
    builder.auto_terminate_unused()


# --------------------------------------------------------------------------- #
#  Reporting task Prometheus (bonus 7.1)
# --------------------------------------------------------------------------- #
def create_prometheus_reporting_task(client: NiFiClient) -> None:
    type_name = "org.apache.nifi.reporting.prometheus.PrometheusReportingTask"
    try:
        bundle = client.resolve_bundle("reporting-task", type_name)
    except NiFiError:
        print("    extension Prometheus absente de cette image NiFi — ignorée")
        return

    existing = client.get("/flow/reporting-tasks")
    for task in existing.get("reportingTasks", []):
        if task["component"]["type"] == type_name:
            print("    reporting task Prometheus déjà présente")
            return

    task = client.post(
        "/controller/reporting-tasks",
        {
            "revision": client.new_revision(),
            "component": {
                "name": "metriques-prometheus",
                "type": type_name,
                "bundle": bundle,
                "schedulingPeriod": "30 sec",
                "comments": (
                    "Bonus 7.1 - expose les metriques NiFi sur :9092/metrics, "
                    "scrapees par Prometheus et affichees dans Grafana."
                ),
                "properties": {
                    "prometheus-reporting-task-metrics-endpoint-port": "9092",
                    "prometheus-reporting-task-metrics-strategy": "All Components",
                    "prometheus-reporting-task-instance-id": "dit-lakehouse-nifi",
                },
            },
        },
    )
    revision = client.revision_of(f"/reporting-tasks/{task['id']}")
    client.put(
        f"/reporting-tasks/{task['id']}/run-status",
        {"revision": revision, "state": "RUNNING"},
    )
    print("    reporting task Prometheus créée et démarrée (port 9092)")


# --------------------------------------------------------------------------- #
#  Point d'entrée
# --------------------------------------------------------------------------- #
def main() -> int:
    parser = argparse.ArgumentParser(description="Construction du flow NiFi")
    parser.add_argument("--url", default=NIFI_URL)
    parser.add_argument("--no-start", action="store_true", help="Ne pas démarrer le flow")
    parser.add_argument(
        "--recreate",
        action="store_true",
        default=True,
        help="Supprime le groupe existant avant reconstruction (défaut)",
    )
    parser.add_argument("--no-prometheus", action="store_true")
    args = parser.parse_args()

    client = NiFiClient(args.url)

    print("=" * 78)
    print("  CONSTRUCTION DU FLOW D'INGESTION NIFI — FakeStoreAPI → MinIO")
    print("=" * 78)

    print("\n[1/7] Connexion à NiFi")
    version = client.wait_until_ready()
    print(f"    NiFi {version} sur {args.url}")

    builder = FlowBuilder(client)
    builder.root_id = client.get("/flow/process-groups/root")["processGroupFlow"]["id"]

    print("\n[2/7] Préparation")
    existing = builder.find_process_group(PG_NAME)
    if existing:
        if not args.recreate:
            print(f"    groupe « {PG_NAME} » déjà présent — rien à faire")
            return 0
        builder.delete_process_group(existing)

    context_id = builder.ensure_parameter_context()
    builder.pg_id = builder.create_process_group(context_id)

    build(builder)

    print("\n[7/7] Démarrage")
    if args.no_start:
        print("    --no-start : le flow reste à l'arrêt")
    else:
        builder.start(keep_stopped=["notifier"])
        print("    flow démarré")

    if not args.no_prometheus:
        create_prometheus_reporting_task(client)

    print("\n" + "=" * 78)
    print(f"  FLOW PRÊT — groupe {builder.pg_id}")
    print(f"  Interface     : {args.url}/nifi")
    print(f"  Déclenchement : POST http://localhost:"
          f"{os.environ.get('NIFI_LISTEN_HTTP_PORT', '9095')}/ingest")
    print("  Corps attendu : "
          '{"snapshot_date":"2026-09-09","domains":["products","users","carts"]}')
    print("=" * 78)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
