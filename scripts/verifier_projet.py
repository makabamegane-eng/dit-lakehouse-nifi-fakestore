#!/usr/bin/env python3
"""Contrôle statique du projet — à lancer avant toute démonstration.

Ne remplace pas l'exécution réelle de la plateforme, mais attrape en quelques
secondes les fautes qui coûtent le plus cher en direct : une coquille dans un
job Spark qui n'apparaîtrait qu'après 40 secondes de spark-submit, un YAML
invalide qui empêche `docker compose up`, un DAG que l'ordonnanceur refusera de
charger, un fichier de dashboard Grafana malformé.

Usage :
    python scripts/verifier_projet.py
"""

from __future__ import annotations

import ast
import json
import sys
from pathlib import Path
from typing import List, Tuple

RACINE = Path(__file__).resolve().parents[1]

VERT = "\033[32m"
ROUGE = "\033[31m"
JAUNE = "\033[33m"
GRIS = "\033[90m"
FIN = "\033[0m"


def _preparer_console() -> None:
    """Rend la sortie lisible quelle que soit la console.

    Sous Windows, la console hérite de la page de code cp1252 : une flèche ou
    un accent y provoque un UnicodeEncodeError en plein rapport. On force
    l'UTF-8 et on active les séquences ANSI, en repliant sur du texte sans
    couleur si le terminal ne les gère pas.
    """
    for flux in (sys.stdout, sys.stderr):
        try:
            flux.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError):
            pass
    if sys.platform == "win32":
        try:
            import ctypes  # noqa: PLC0415

            noyau = ctypes.windll.kernel32
            noyau.SetConsoleMode(noyau.GetStdHandle(-11), 7)
        except Exception:  # noqa: BLE001
            globals().update(VERT="", ROUGE="", JAUNE="", GRIS="", FIN="")


class Bilan:
    def __init__(self) -> None:
        self.controles = 0
        self.erreurs: List[Tuple[str, str]] = []
        self.avertissements: List[Tuple[str, str]] = []

    def ok(self, quoi: str, detail: str = "") -> None:
        self.controles += 1
        print(f"  {VERT}OK{FIN}      {quoi}{GRIS}{(' — ' + detail) if detail else ''}{FIN}")

    def erreur(self, quoi: str, detail: str) -> None:
        self.controles += 1
        self.erreurs.append((quoi, detail))
        print(f"  {ROUGE}ERREUR{FIN}  {quoi}\n          {detail}")

    def avertir(self, quoi: str, detail: str) -> None:
        self.controles += 1
        self.avertissements.append((quoi, detail))
        print(f"  {JAUNE}ALERTE{FIN}  {quoi} — {detail}")


def titre(texte: str) -> None:
    print(f"\n{texte}\n{'-' * len(texte)}")


# --------------------------------------------------------------------------- #
def verifier_python(bilan: Bilan) -> None:
    titre("Syntaxe des sources Python")
    dossiers = ["spark/jobs", "airflow/dags", "nifi/scripts", "dremio/scripts", "scripts"]
    fichiers = sorted(
        f for d in dossiers for f in (RACINE / d).rglob("*.py") if f.is_file()
    )
    for fichier in fichiers:
        relatif = fichier.relative_to(RACINE).as_posix()
        try:
            ast.parse(fichier.read_text(encoding="utf-8"), filename=str(fichier))
            bilan.ok(relatif)
        except SyntaxError as exc:
            bilan.erreur(relatif, f"ligne {exc.lineno} : {exc.msg}")


def verifier_yaml(bilan: Bilan) -> None:
    titre("Fichiers YAML")
    try:
        import yaml  # noqa: PLC0415
    except ImportError:
        bilan.avertir("PyYAML", "non installé — contrôle YAML ignoré (pip install pyyaml)")
        return

    cibles = [
        "docker-compose.yml",
        "monitoring/prometheus/prometheus.yml",
        "monitoring/prometheus/alerts.yml",
        "monitoring/grafana/provisioning/datasources/prometheus.yml",
        "monitoring/grafana/provisioning/dashboards/dashboards.yml",
    ]
    for cible in cibles:
        chemin = RACINE / cible
        if not chemin.exists():
            bilan.erreur(cible, "fichier absent")
            continue
        try:
            # Les ancres YAML de docker-compose utilisent la fusion `<<`, que le
            # chargeur sûr accepte, mais `<<: [*a, *b]` nécessite le chargeur
            # complet. On tente le plus strict, puis on retombe.
            try:
                yaml.safe_load(chemin.read_text(encoding="utf-8"))
            except yaml.constructor.ConstructorError:
                yaml.unsafe_load(chemin.read_text(encoding="utf-8"))
            bilan.ok(cible)
        except Exception as exc:  # noqa: BLE001
            bilan.erreur(cible, str(exc).splitlines()[0])


def verifier_json(bilan: Bilan) -> None:
    titre("Fichiers JSON")
    for chemin in sorted((RACINE / "monitoring").rglob("*.json")):
        relatif = chemin.relative_to(RACINE).as_posix()
        try:
            json.loads(chemin.read_text(encoding="utf-8"))
            bilan.ok(relatif)
        except json.JSONDecodeError as exc:
            bilan.erreur(relatif, f"ligne {exc.lineno} : {exc.msg}")


def verifier_arborescence(bilan: Bilan) -> None:
    titre("Livrables attendus par le sujet")
    attendus = {
        "docker-compose.yml": "Partie 1 — infrastructure complète",
        "nifi/scripts/build_flow.py": "Partie 3 — flow d'ingestion NiFi",
        "spark/jobs/bronze/bronze_ingest.py": "Partie 4 — couche Bronze",
        "spark/jobs/silver/silver_products.py": "Partie 4 — couche Silver",
        "spark/jobs/gold/gold_sales_daily.py": "Partie 4 — couche Gold",
        "dremio/sql/validation_dremio.sql": "Partie 5 — validation Dremio",
        "airflow/dags/dag_lakehouse_medallion.py": "Partie 6 — orchestration",
        "docs/architecture.md": "Support de la soutenance",
        "README.md": "Mode d'emploi",
    }
    for relatif, role in attendus.items():
        if (RACINE / relatif).exists():
            bilan.ok(relatif, role)
        else:
            bilan.erreur(relatif, f"manquant ({role})")


def verifier_coherence(bilan: Bilan) -> None:
    titre("Cohérence de la configuration")

    compose = (RACINE / "docker-compose.yml").read_text(encoding="utf-8")
    services_attendus = [
        "minio", "postgres", "nessie", "spark-master", "spark-worker-1",
        "nifi", "dremio", "airflow-webserver", "airflow-scheduler",
        "prometheus", "grafana",
    ]
    manquants = [s for s in services_attendus if f"\n  {s}:" not in compose]
    if manquants:
        bilan.erreur("docker-compose.yml", f"services absents : {', '.join(manquants)}")
    else:
        bilan.ok("docker-compose.yml", f"{len(services_attendus)} services imposés présents")

    # Aucun `localhost` ne doit apparaître comme endpoint inter-conteneurs :
    # c'est l'erreur la plus fréquente et la plus pénible à diagnostiquer.
    env = (RACINE / ".env").read_text(encoding="utf-8")
    fautifs = [
        ligne
        for ligne in env.splitlines()
        if "localhost" in ligne and not ligne.strip().startswith("#")
    ]
    if fautifs:
        bilan.erreur(".env", f"endpoint en localhost : {fautifs[0]}")
    else:
        bilan.ok(".env", "aucun endpoint inter-services en localhost")

    # Les ports publiés ne doivent pas se marcher dessus.
    import re  # noqa: PLC0415

    ports = re.findall(r'^\s+- "(\d+):\d+"', compose, re.MULTILINE)
    doublons = {p for p in ports if ports.count(p) > 1}
    if doublons:
        bilan.erreur("docker-compose.yml", f"ports hôte en conflit : {sorted(doublons)}")
    else:
        bilan.ok("docker-compose.yml", f"{len(ports)} ports publiés, sans conflit")


def verifier_sql(bilan: Bilan) -> None:
    titre("Requêtes de validation Dremio")
    chemin = RACINE / "dremio" / "sql" / "validation_dremio.sql"
    if not chemin.exists():
        bilan.erreur("validation_dremio.sql", "fichier absent")
        return

    contenu = chemin.read_text(encoding="utf-8")
    marqueurs = [
        ligne for ligne in contenu.splitlines() if ligne.strip().startswith("-- @query:")
    ]
    bilan.ok("validation_dremio.sql", f"{len(marqueurs)} requête(s) déclarée(s)")

    exigences = {
        "requête sur Bronze": "bronze.raw_products",
        "requête sur Silver": "silver.dim_products",
        "requête sur Gold": "gold.gold_catalog_daily_kpi",
        "jointure inter-domaines": "silver.dim_customers",
        "agrégation Gold": "GROUP BY",
    }
    for libelle, motif in exigences.items():
        if motif in contenu:
            bilan.ok(libelle, "couverte")
        else:
            bilan.erreur(libelle, f"aucune requête ne contient « {motif} »")


def main() -> int:
    _preparer_console()
    print("=" * 78)
    print("  VÉRIFICATION STATIQUE DE LA PLATEFORME LAKEHOUSE")
    print("=" * 78)

    bilan = Bilan()
    verifier_python(bilan)
    verifier_yaml(bilan)
    verifier_json(bilan)
    verifier_arborescence(bilan)
    verifier_coherence(bilan)
    verifier_sql(bilan)

    print("\n" + "=" * 78)
    print(
        f"  {bilan.controles} contrôle(s) — "
        f"{len(bilan.erreurs)} erreur(s), {len(bilan.avertissements)} alerte(s)"
    )
    print("=" * 78)

    if bilan.erreurs:
        print("\nÀ corriger :")
        for quoi, detail in bilan.erreurs:
            print(f"  - {quoi} : {detail}")
        return 1

    print("\nProjet cohérent — prêt pour le démarrage de la plateforme.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
