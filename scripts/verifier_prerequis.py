#!/usr/bin/env python3
"""Contrôle des prérequis — à lancer AVANT le premier démarrage.

Onze conteneurs qui ne démarrent pas, c'est difficile à diagnostiquer après
coup. Ce script vérifie en quelques secondes tout ce qui doit être vrai avant
de lancer la plateforme, et dit précisément quoi corriger :

  * Docker installé, moteur démarré, Compose v2 disponible
  * mémoire allouée à Docker (le point qui pose problème sous Windows/WSL2)
  * espace disque suffisant
  * dépendances Python des scripts d'administration
  * ports hôte libres
  * accès à FakeStoreAPI

Usage :
    python scripts/verifier_prerequis.py
"""

from __future__ import annotations

import json
import shutil
import socket
import subprocess
import sys
from pathlib import Path

RACINE = Path(__file__).resolve().parents[1]

VERT = "\033[32m"
ROUGE = "\033[31m"
JAUNE = "\033[33m"
GRIS = "\033[90m"
FIN = "\033[0m"

GIO = 1024 ** 3

# Mémoire réellement consommée par la pile en régime de croisière, mesurée
# service par service. En dessous de 12 Gio, Dremio et les workers Spark se
# font tuer par l'OOM killer pendant la construction du médaillon.
MEMOIRE_RECOMMANDEE_GIO = 14
MEMOIRE_MINIMALE_GIO = 9
DISQUE_REQUIS_GIO = 25

PORTS = {
    9000: "MinIO (API S3)",
    9001: "MinIO (console)",
    5432: "PostgreSQL",
    19120: "Nessie",
    7077: "Spark (RPC master)",
    8090: "Spark (UI master)",
    8091: "Spark (UI worker 1)",
    8092: "Spark (UI worker 2)",
    8080: "NiFi (UI)",
    9095: "NiFi (ListenHTTP)",
    9092: "NiFi (métriques)",
    8085: "Airflow",
    9047: "Dremio (UI)",
    31010: "Dremio (JDBC)",
    32010: "Dremio (Arrow Flight)",
    9090: "Prometheus",
    3001: "Grafana",
    8093: "cAdvisor",
    4040: "Spark (UI application)",
}

MODULES_PYTHON = {
    "requests": "pilotage des API NiFi et Dremio",
    "yaml": "contrôle statique des fichiers YAML (paquet pyyaml)",
    "pptx": "génération du support de soutenance (paquet python-pptx)",
}


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
        self.bloquants: list[str] = []
        self.alertes: list[str] = []

    def ok(self, quoi: str, detail: str = "") -> None:
        print(f"  {VERT}OK{FIN}       {quoi}{GRIS}{('  ' + detail) if detail else ''}{FIN}")

    def bloquant(self, quoi: str, correction: str) -> None:
        self.bloquants.append(f"{quoi} → {correction}")
        print(f"  {ROUGE}BLOQUANT{FIN} {quoi}")
        print(f"           {JAUNE}{correction}{FIN}")

    def alerte(self, quoi: str, correction: str) -> None:
        self.alertes.append(f"{quoi} → {correction}")
        print(f"  {JAUNE}ALERTE{FIN}   {quoi}")
        print(f"           {GRIS}{correction}{FIN}")


def titre(texte: str) -> None:
    print(f"\n{texte}\n{'-' * len(texte)}")


def commande(args: list[str], timeout: int = 25) -> tuple[int, str]:
    try:
        proc = subprocess.run(
            args, capture_output=True, text=True, timeout=timeout, check=False
        )
        return proc.returncode, (proc.stdout or proc.stderr).strip()
    except FileNotFoundError:
        return 127, "commande introuvable"
    except subprocess.TimeoutExpired:
        return 124, "délai dépassé"


# --------------------------------------------------------------------------- #
def verifier_docker(bilan: Bilan) -> dict | None:
    titre("Docker")

    if shutil.which("docker") is None:
        bilan.bloquant(
            "Docker n'est pas installé",
            "Installer Docker Desktop : https://www.docker.com/products/docker-desktop",
        )
        return None

    code, sortie = commande(["docker", "--version"])
    bilan.ok("Docker installé", sortie)

    code, sortie = commande(["docker", "compose", "version"])
    if code != 0:
        bilan.bloquant(
            "Docker Compose v2 indisponible",
            "Mettre à jour Docker Desktop — la syntaxe `docker compose` (sans tiret) est requise",
        )
    else:
        bilan.ok("Docker Compose v2", sortie.splitlines()[0])

    code, sortie = commande(["docker", "info", "--format", "{{json .}}"], timeout=35)
    if code != 0:
        bilan.bloquant(
            "Le moteur Docker ne répond pas",
            "Lancer Docker Desktop et attendre que l'icône passe au vert, puis relancer ce script",
        )
        return None

    try:
        info = json.loads(sortie)
    except json.JSONDecodeError:
        bilan.alerte("Réponse inattendue de `docker info`", "Contrôle mémoire/CPU ignoré")
        return None

    bilan.ok("Moteur Docker démarré", info.get("OperatingSystem", ""))
    return info


def verifier_ressources(bilan: Bilan, info: dict | None) -> None:
    titre("Ressources allouées à Docker")

    if info is None:
        bilan.alerte("Ressources non vérifiables", "Le moteur Docker n'est pas joignable")
        return

    memoire = info.get("MemTotal", 0) / GIO
    cpus = info.get("NCPU", 0)

    if memoire >= MEMOIRE_RECOMMANDEE_GIO:
        bilan.ok("Mémoire", f"{memoire:.1f} Gio alloués au moteur")
    elif memoire >= MEMOIRE_MINIMALE_GIO:
        bilan.alerte(
            f"Mémoire un peu juste : {memoire:.1f} Gio "
            f"(recommandé {MEMOIRE_RECOMMANDEE_GIO} Gio)",
            "Fonctionnera en profil allégé — voir la section « Machine limitée » du README",
        )
    else:
        bilan.bloquant(
            f"Mémoire insuffisante : {memoire:.1f} Gio "
            f"(minimum {MEMOIRE_MINIMALE_GIO} Gio)",
            "Windows : créer %USERPROFILE%\\.wslconfig avec [wsl2] / memory=20GB, "
            "puis `wsl --shutdown` et relancer Docker Desktop. "
            "macOS/Linux : régler la mémoire dans les préférences de Docker Desktop",
        )

    if cpus >= 4:
        bilan.ok("Processeurs", f"{cpus} alloués au moteur")
    else:
        bilan.alerte(
            f"Seulement {cpus} processeur(s) alloué(s)",
            "4 minimum recommandés — les jobs Spark seront lents en dessous",
        )


def verifier_disque(bilan: Bilan) -> None:
    titre("Espace disque")

    usage = shutil.disk_usage(RACINE)
    libre = usage.free / GIO
    if libre >= DISQUE_REQUIS_GIO:
        bilan.ok(f"Volume du projet", f"{libre:.1f} Gio libres")
    else:
        bilan.bloquant(
            f"Espace insuffisant sur le volume du projet : {libre:.1f} Gio",
            f"{DISQUE_REQUIS_GIO} Gio requis (images Docker, volumes MinIO et NiFi)",
        )

    # Sous Windows, les images vivent dans le disque virtuel WSL, sur C:,
    # pas sur le volume du projet : c'est un piège classique.
    if sys.platform == "win32":
        try:
            libre_c = shutil.disk_usage("C:\\").free / GIO
            if libre_c >= DISQUE_REQUIS_GIO:
                bilan.ok("Volume système C:", f"{libre_c:.1f} Gio libres")
            else:
                bilan.bloquant(
                    f"Espace insuffisant sur C: : {libre_c:.1f} Gio",
                    "Les images Docker sont stockées dans le disque virtuel WSL, sur C:",
                )
        except OSError:
            pass


def verifier_python(bilan: Bilan) -> None:
    titre("Python et dépendances")

    version = sys.version_info
    if version >= (3, 9):
        bilan.ok("Version de Python", f"{version.major}.{version.minor}.{version.micro}")
    else:
        bilan.bloquant(
            f"Python {version.major}.{version.minor} trop ancien",
            "Python 3.9 ou supérieur est requis",
        )

    manquants = []
    for module, role in MODULES_PYTHON.items():
        try:
            __import__(module)
            bilan.ok(f"Module {module}", role)
        except ImportError:
            manquants.append(module)
            bilan.alerte(f"Module {module} absent", role)

    if manquants:
        paquets = {"yaml": "pyyaml", "pptx": "python-pptx"}
        noms = " ".join(paquets.get(m, m) for m in manquants)
        print(f"           {JAUNE}pip install {noms}{FIN}")


def verifier_ports(bilan: Bilan) -> None:
    titre("Ports hôte")

    occupes = []
    for port, service in sorted(PORTS.items()):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.35)
            if sock.connect_ex(("127.0.0.1", port)) == 0:
                occupes.append((port, service))

    if not occupes:
        bilan.ok(f"{len(PORTS)} ports requis", "tous libres")
        return

    # Un port occupé n'est pas forcément un problème : c'est peut-être la
    # plateforme elle-même, déjà démarrée.
    code, sortie = commande(
        ["docker", "compose", "ps", "--format", "{{.Service}}"], timeout=25
    )
    deja_lancee = code == 0 and sortie.strip() != ""

    if deja_lancee:
        bilan.ok(
            f"{len(occupes)} port(s) occupé(s)",
            "par la plateforme elle-même, déjà démarrée",
        )
        return

    for port, service in occupes:
        bilan.bloquant(
            f"Port {port} déjà utilisé (attendu pour {service})",
            f"Libérer le port, ou modifier le mapping dans docker-compose.yml",
        )


def verifier_source(bilan: Bilan) -> None:
    titre("Source de données")

    try:
        import requests  # noqa: PLC0415

        reponse = requests.get("https://fakestoreapi.com/products/1", timeout=12)
        if reponse.status_code == 200:
            bilan.ok("FakeStoreAPI joignable", f"HTTP 200, {len(reponse.content)} octets")
        else:
            bilan.alerte(
                f"FakeStoreAPI répond HTTP {reponse.status_code}",
                "L'API publique est peut-être momentanément indisponible",
            )
    except ImportError:
        bilan.alerte("Test de l'API ignoré", "Module requests absent")
    except Exception as exc:  # noqa: BLE001
        bilan.alerte(
            f"FakeStoreAPI injoignable : {str(exc)[:90]}",
            "Vérifier la connexion Internet ou le proxy d'entreprise",
        )


def verifier_projet(bilan: Bilan) -> None:
    titre("Fichiers du projet")

    for relatif in ("docker-compose.yml", ".env", "spark/jobs", "airflow/dags"):
        if (RACINE / relatif).exists():
            bilan.ok(relatif)
        else:
            bilan.bloquant(f"{relatif} introuvable", "Se placer à la racine du dépôt")


def main() -> int:
    _preparer_console()
    print("=" * 78)
    print("  CONTRÔLE DES PRÉREQUIS — plateforme Data Lakehouse")
    print("=" * 78)

    bilan = Bilan()
    verifier_projet(bilan)
    info = verifier_docker(bilan)
    verifier_ressources(bilan, info)
    verifier_disque(bilan)
    verifier_python(bilan)
    verifier_ports(bilan)
    verifier_source(bilan)

    print("\n" + "=" * 78)
    if bilan.bloquants:
        print(f"  {len(bilan.bloquants)} point(s) BLOQUANT(S) à corriger avant de démarrer :")
        for ligne in bilan.bloquants:
            print(f"    - {ligne}")
        print("=" * 78)
        return 1

    if bilan.alertes:
        print(f"  Prêt à démarrer, avec {len(bilan.alertes)} alerte(s) :")
        for ligne in bilan.alertes:
            print(f"    - {ligne}")
    else:
        print("  Tous les prérequis sont réunis.")

    print("\n  Étape suivante :  docker compose build")
    print("=" * 78)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
