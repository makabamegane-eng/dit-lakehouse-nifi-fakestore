#!/usr/bin/env python3
"""Export du flow NiFi au format `.json` — livrable exigé par le sujet (§6.2).

Le sujet demande « l'export du flow NiFi au format .json » parmi les livrables
complémentaires. Ce script le produit depuis l'instance réellement en cours
d'exécution, ce qui garantit que le fichier versionné correspond exactement au
flow démontré dans la vidéo.

Deux fichiers sont écrits dans nifi/flow/ :
  * fakestore_ingestion.json  — la définition de flow (« flow definition »),
    directement ré-importable dans une autre instance NiFi par
    « Upload Process Group » ;
  * fakestore_ingestion_snapshot.json — la vue complète du groupe telle que
    l'API la retourne (positions, états, statistiques). Utile pour la revue,
    pas pour le ré-import.

Usage :
    python nifi/scripts/export_flow.py
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from nifi_client import NiFiClient, NiFiError  # noqa: E402

NIFI_URL = os.environ.get("NIFI_URL", "http://localhost:8080")
PG_NAME = "FakeStoreAPI Ingestion"
OUTPUT_DIR = Path(__file__).resolve().parents[1] / "flow"


def find_group(client: NiFiClient, name: str) -> str:
    root = client.get("/flow/process-groups/root")["processGroupFlow"]["id"]
    flow = client.get(f"/flow/process-groups/{root}")
    for group in flow["processGroupFlow"]["flow"]["processGroups"]:
        if group["component"]["name"] == name:
            return group["id"]
    raise NiFiError(
        f"Groupe « {name} » introuvable — exécuter build_flow.py au préalable"
    )


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=False),
        encoding="utf-8",
    )
    taille = path.stat().st_size / 1024
    print(f"    {path.name:<42} {taille:>8.1f} Ko")


def main() -> int:
    parser = argparse.ArgumentParser(description="Export du flow NiFi")
    parser.add_argument("--url", default=NIFI_URL)
    parser.add_argument("--name", default=PG_NAME)
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR))
    args = parser.parse_args()

    client = NiFiClient(args.url)
    client.wait_until_ready(attempts=12, delay=5)
    group_id = find_group(client, args.name)
    output = Path(args.output_dir)

    print(f"Export du groupe « {args.name} » ({group_id})")

    # Définition de flow ré-importable. `includeReferencedServices` embarque les
    # services de contrôle (identifiants MinIO, cache d'idempotence) : sans eux,
    # le flow ré-importé serait invalide.
    definition = client.get(
        f"/process-groups/{group_id}/download",
        params={"includeReferencedServices": "true"},
    )
    if isinstance(definition, str):
        definition = json.loads(definition)
    write_json(output / "fakestore_ingestion.json", definition)

    snapshot = client.get(f"/flow/process-groups/{group_id}")
    write_json(output / "fakestore_ingestion_snapshot.json", snapshot)

    # Inventaire lisible : sert de table des matières au moment de la revue.
    composants = snapshot["processGroupFlow"]["flow"]
    inventaire = {
        "groupe": args.name,
        "processeurs": sorted(
            [
                {
                    "nom": p["component"]["name"],
                    "type": p["component"]["type"].rsplit(".", 1)[-1],
                    "etat": p["component"]["state"],
                }
                for p in composants["processors"]
            ],
            key=lambda d: d["nom"],
        ),
        "connexions": [
            {
                "de": c["component"]["source"]["name"],
                "vers": c["component"]["destination"]["name"],
                "relations": c["component"]["selectedRelationships"],
            }
            for c in composants["connections"]
        ],
    }
    write_json(output / "fakestore_ingestion_inventaire.json", inventaire)

    print(
        f"\n{len(inventaire['processeurs'])} processeur(s), "
        f"{len(inventaire['connexions'])} connexion(s) exportés."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
