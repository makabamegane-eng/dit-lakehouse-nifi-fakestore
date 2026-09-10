#!/usr/bin/env python3
"""Exécute les requêtes de validation Dremio et affiche les résultats.

Le sujet demande que le bon fonctionnement de Dremio soit « testé et démontré
explicitement ». Ce script industrialise cette preuve : il joue les requêtes de
dremio/sql/validation_dremio.sql, mesure leur durée, affiche un extrait des
résultats et rend un verdict global.

Il sert deux usages :
  * en démonstration — une seule commande produit la preuve complète, lisible
    à l'écran, sans dépendre de la vitesse de frappe ;
  * en contrôle continu — appelable depuis Airflow ou un script de recette pour
    vérifier que le moteur SQL reste opérationnel après un changement.

Usage :
    python dremio/scripts/run_validation.py
    python dremio/scripts/run_validation.py --only Q4,Q5 --rows 20
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent))

from setup_dremio import Dremio, DREMIO_URL  # noqa: E402

SQL_FILE = Path(__file__).resolve().parents[1] / "sql" / "validation_dremio.sql"
MARKER = re.compile(r"^--\s*@query:\s*(\S+)\s*(?:—|-)?\s*(.*)$", re.MULTILINE)


def parse_queries(path: Path) -> List[Tuple[str, str, str]]:
    """Découpe le fichier SQL en (identifiant, titre, requête)."""
    content = path.read_text(encoding="utf-8")
    markers = list(MARKER.finditer(content))
    queries: List[Tuple[str, str, str]] = []

    for index, marker in enumerate(markers):
        start = marker.end()
        end = markers[index + 1].start() if index + 1 < len(markers) else len(content)
        body = content[start:end]
        # On retire les lignes de commentaire pédagogiques : Dremio les accepte,
        # mais l'affichage est plus lisible sans elles.
        statement = "\n".join(
            line for line in body.splitlines() if not line.strip().startswith("--")
        ).strip().rstrip(";")
        if statement:
            queries.append((marker.group(1), marker.group(2).strip(), statement))

    return queries


def render(rows: List[Dict], limit: int) -> None:
    """Affiche un résultat en tableau texte, colonnes alignées."""
    if not rows:
        print("      (aucune ligne)")
        return

    colonnes = list(rows[0].keys())
    extrait = rows[:limit]
    largeurs = {
        col: max(len(col), *(len(str(r.get(col, ""))) for r in extrait))
        for col in colonnes
    }
    largeurs = {col: min(width, 28) for col, width in largeurs.items()}

    def ligne(valeurs: Dict) -> str:
        return "  ".join(
            str(valeurs.get(col, ""))[: largeurs[col]].ljust(largeurs[col])
            for col in colonnes
        )

    entete = "  ".join(col[: largeurs[col]].ljust(largeurs[col]) for col in colonnes)
    print("      " + entete)
    print("      " + "-" * len(entete))
    for row in extrait:
        print("      " + ligne(row))
    if len(rows) > limit:
        print(f"      … {len(rows) - limit} ligne(s) supplémentaire(s)")


def main() -> int:
    parser = argparse.ArgumentParser(description="Validation SQL de Dremio")
    parser.add_argument("--url", default=DREMIO_URL)
    parser.add_argument("--rows", type=int, default=8, help="Lignes affichées par requête")
    parser.add_argument("--only", default="", help="Identifiants à jouer, ex. Q4,Q6")
    parser.add_argument("--sql-file", default=str(SQL_FILE))
    args = parser.parse_args()

    queries = parse_queries(Path(args.sql_file))
    retenues = {q.strip().upper() for q in args.only.split(",") if q.strip()}
    if retenues:
        queries = [q for q in queries if q[0].upper() in retenues]

    print("=" * 78)
    print("  VALIDATION DU MOTEUR DE REQUÊTE DREMIO")
    print(f"  {len(queries)} requête(s) — source : {Path(args.sql_file).name}")
    print("=" * 78)

    dremio = Dremio(args.url)
    dremio.wait_ready(attempts=24, delay=5)
    dremio.login()

    succes = 0
    echecs: List[Tuple[str, str]] = []

    for identifiant, titre, statement in queries:
        print(f"\n  [{identifiant}] {titre}")
        debut = time.time()
        try:
            resultat = dremio.sql(statement)
            duree = time.time() - debut
            rows = resultat.get("rows", [])
            total = resultat.get("rowCount", len(rows))
            print(f"      OK — {total} ligne(s) en {duree:.2f} s")
            render(rows, args.rows)
            succes += 1
        except Exception as exc:  # noqa: BLE001
            duree = time.time() - debut
            message = str(exc).splitlines()[0][:200]
            print(f"      ECHEC après {duree:.2f} s — {message}")
            echecs.append((identifiant, message))

    print("\n" + "=" * 78)
    print(f"  BILAN : {succes}/{len(queries)} requête(s) exécutée(s) avec succès")
    for identifiant, message in echecs:
        print(f"    - {identifiant} : {message}")
    print("=" * 78)

    return 0 if not echecs else 1


if __name__ == "__main__":
    raise SystemExit(main())
