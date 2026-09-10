"""Client minimal de l'API REST NiFi, taillé pour construire un flow.

Pourquoi un client maison plutôt qu'une bibliothèque tierce : l'API NiFi est
simple (JSON + révisions optimistes) mais très verbeuse, et les bibliothèques
disponibles suivent mal les changements de version. Les trois helpers ci-dessous
suffisent et rendent le script de construction lisible.

Deux points de mécanique NiFi qu'il faut connaître pour lire ce fichier :

  * RÉVISIONS — chaque composant porte un couple (clientId, version). Toute
    modification doit citer la révision courante, sinon NiFi refuse : c'est un
    verrouillage optimiste. `revision_of()` va la relire juste avant l'appel.

  * NOMS DE PROPRIÉTÉS — l'API attend le NOM interne d'une propriété
    (« use-path-style-access »), pas son libellé affiché (« Use Path Style
    Access »), et ces noms ont changé selon les versions de NiFi. Plutôt que de
    coder en dur une table de correspondance fragile, `resolve_properties()`
    interroge les descripteurs du composant et fait la correspondance sur l'un
    ou l'autre. Le script fonctionne donc sur plusieurs versions de NiFi sans
    modification.
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Any, Dict, Iterable, Optional

import requests


class NiFiError(RuntimeError):
    pass


class NiFiClient:
    def __init__(self, base_url: str, timeout: int = 30) -> None:
        self.base = base_url.rstrip("/") + "/nifi-api"
        self.timeout = timeout
        self.client_id = str(uuid.uuid4())
        self.session = requests.Session()

    # ------------------------------------------------------------------ HTTP #
    def _request(self, method: str, path: str, **kwargs) -> Any:
        url = f"{self.base}{path}"
        response = self.session.request(method, url, timeout=self.timeout, **kwargs)
        if response.status_code >= 400:
            raise NiFiError(
                f"{method} {path} -> HTTP {response.status_code}\n{response.text[:1500]}"
            )
        if not response.content:
            return None
        try:
            return response.json()
        except json.JSONDecodeError:
            return response.text

    def get(self, path: str, **kwargs) -> Any:
        return self._request("GET", path, **kwargs)

    def post(self, path: str, payload: Dict[str, Any]) -> Any:
        return self._request("POST", path, json=payload)

    def put(self, path: str, payload: Dict[str, Any]) -> Any:
        return self._request("PUT", path, json=payload)

    def delete(self, path: str, **kwargs) -> Any:
        return self._request("DELETE", path, **kwargs)

    # -------------------------------------------------------------- Attente #
    def wait_until_ready(self, attempts: int = 60, delay: int = 5) -> str:
        """Attend que NiFi réponde et retourne sa version."""
        for i in range(attempts):
            try:
                about = self.get("/flow/about")
                return about["about"]["version"]
            except Exception:  # noqa: BLE001
                print(f"    NiFi pas encore prêt ({i + 1}/{attempts})…")
                time.sleep(delay)
        raise NiFiError("NiFi n'a pas répondu dans le délai imparti")

    # ------------------------------------------------------------ Révisions #
    def revision_of(self, entity_path: str) -> Dict[str, Any]:
        entity = self.get(entity_path)
        revision = dict(entity["revision"])
        revision["clientId"] = self.client_id
        return revision

    def new_revision(self) -> Dict[str, Any]:
        return {"version": 0, "clientId": self.client_id}

    # ------------------------------------------------------------- Bundles  #
    def resolve_bundle(self, kind: str, type_name: str) -> Dict[str, str]:
        """Retrouve le bundle (NAR) qui fournit un type de composant.

        Fournir explicitement le bundle évite les ambiguïtés quand plusieurs
        NAR exposent le même type, et donne un message d'erreur clair si une
        extension attendue n'est pas installée.
        """
        path = {
            "processor": "/flow/processor-types",
            "controller-service": "/flow/controller-service-types",
            "reporting-task": "/flow/reporting-task-types",
        }[kind]
        key = {
            "processor": "processorTypes",
            "controller-service": "controllerServiceTypes",
            "reporting-task": "reportingTaskTypes",
        }[kind]

        if not hasattr(self, "_type_cache"):
            self._type_cache: Dict[str, Dict[str, Dict[str, str]]] = {}
        if kind not in self._type_cache:
            catalogue = self.get(path)[key]
            self._type_cache[kind] = {
                entry["type"]: entry["bundle"] for entry in catalogue
            }

        bundle = self._type_cache[kind].get(type_name)
        if bundle is None:
            raise NiFiError(
                f"Type {kind} introuvable dans cette installation NiFi : {type_name}"
            )
        return {
            "group": bundle["group"],
            "artifact": bundle["artifact"],
            "version": bundle["version"],
        }

    # ---------------------------------------------------------- Propriétés  #
    @staticmethod
    def _descriptor_index(descriptors: Dict[str, Any]) -> Dict[str, str]:
        index: Dict[str, str] = {}
        for name, descriptor in descriptors.items():
            index[name.lower()] = name
            display = descriptor.get("displayName")
            if display:
                index[display.lower()] = name
        return index

    def resolve_properties(
        self, entity_path: str, wanted: Dict[str, Optional[str]]
    ) -> Dict[str, Optional[str]]:
        """Traduit des libellés lisibles en noms de propriétés réels.

        Une clé inconnue des descripteurs est conservée telle quelle : c'est le
        cas des propriétés dynamiques (attributs d'EvaluateJsonPath, routes de
        RouteOnAttribute), qui n'ont pas de descripteur préexistant.
        """
        entity = self.get(entity_path)
        component = entity["component"]
        descriptors = component.get("config", component).get("descriptors", {})
        index = self._descriptor_index(descriptors)
        return {index.get(key.lower(), key): value for key, value in wanted.items()}
