<#
.SYNOPSIS
    Pilotage de la plateforme lakehouse depuis PowerShell (équivalent du Makefile).

.DESCRIPTION
    `make` n'est pas installé par défaut sous Windows. Ce script expose les
    mêmes opérations, avec la même sémantique, pour que la plateforme se pilote
    d'une seule commande quel que soit le poste.

.PARAMETER Action
    Opération à exécuter. `.\scripts\plateforme.ps1 aide` liste les valeurs.

.EXAMPLE
    .\scripts\plateforme.ps1 demarrage-complet
    .\scripts\plateforme.ps1 logs -Service nifi
    .\scripts\plateforme.ps1 dremio-sql -Requete Q4
#>

[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [string]$Action = "aide",

    [string]$Service = "",
    [string]$Requete = "Q4"
)

$ErrorActionPreference = "Stop"
$Racine = Split-Path -Parent $PSScriptRoot
Set-Location $Racine

function Titre($texte) {
    Write-Host ""
    Write-Host "  $texte" -ForegroundColor Cyan
    Write-Host ("  " + ("-" * $texte.Length)) -ForegroundColor DarkGray
}

function Airflow-Trigger($dag) {
    docker compose exec airflow-scheduler airflow dags unpause $dag
    docker compose exec airflow-scheduler airflow dags trigger $dag
}

function Spark-Submit($script) {
    docker compose exec spark-master /opt/spark/bin/spark-submit `
        --master spark://spark-master:7077 "/opt/spark-jobs/$script"
}

switch ($Action.ToLower()) {

    "aide" {
        Write-Host ""
        Write-Host "  PLATEFORME DATA LAKEHOUSE — commandes PowerShell" -ForegroundColor Cyan
        Write-Host "  ================================================" -ForegroundColor DarkGray
        $commandes = [ordered]@{
            "prerequis"          = "Contrôle les prérequis AVANT le premier démarrage"
            "build"              = "Construit les images custom (Spark et Airflow)"
            "up"                 = "Démarre l'ensemble des services"
            "down"               = "Arrête les services (volumes conservés)"
            "ps"                 = "État des conteneurs"
            "logs"               = "Suit les journaux (-Service nifi)"
            "urls"               = "Rappelle les interfaces et identifiants"
            "nifi-flow"          = "Construit le dataflow NiFi via l'API REST"
            "nifi-export"        = "Exporte le flow NiFi en .json (livrable)"
            "dremio-setup"       = "Déclare la source Nessie et les vues Dremio"
            "backfill"           = "Reconstitue les 6 mois puis construit le médaillon"
            "ingest"             = "Déclenche une ingestion NiFi pour aujourd'hui"
            "medaillon"          = "Reconstruit Bronze -> Silver -> Gold"
            "dremio-test"        = "Joue les 10 requêtes de validation"
            "dremio-sql"         = "Joue une requête précise (-Requete Q4)"
            "demo-iceberg"       = "Time travel, schema evolution, branches Nessie"
            "qualite"            = "Rejoue les contrôles qualité"
            "maintenance"        = "Compacte les tables Iceberg"
            "verifier"           = "Contrôle statique du projet"
            "reset"              = "DESTRUCTIF — supprime aussi les volumes"
            "demarrage-complet"  = "build -> up -> init -> backfill"
        }
        foreach ($cle in $commandes.Keys) {
            Write-Host ("    {0,-20}" -f $cle) -ForegroundColor Yellow -NoNewline
            Write-Host $commandes[$cle] -ForegroundColor Gray
        }
        Write-Host ""
    }

    "build"        { docker compose build }
    "up"           { docker compose up -d; Write-Host "Services démarrés." -ForegroundColor Green }
    "down"         { docker compose down }
    "ps"           { docker compose ps }
    "logs"         { if ($Service) { docker compose logs -f $Service } else { docker compose logs -f } }

    "urls" {
        Titre "Interfaces de la plateforme"
        $acces = [ordered]@{
            "MinIO console" = "http://localhost:9001    lakehouse / lakehouse123"
            "Apache NiFi"   = "http://localhost:8080/nifi"
            "Apache Airflow" = "http://localhost:8085    admin / admin"
            "Spark master"  = "http://localhost:8090"
            "Dremio"        = "http://localhost:9047    dremio / dremio123"
            "Nessie API"    = "http://localhost:19120/api/v2/config"
            "Prometheus"    = "http://localhost:9090"
            "Grafana"       = "http://localhost:3001    admin / admin"
        }
        foreach ($cle in $acces.Keys) {
            Write-Host ("    {0,-16}" -f $cle) -ForegroundColor Yellow -NoNewline
            Write-Host $acces[$cle] -ForegroundColor Gray
        }
        Write-Host ""
    }

    "nifi-flow"    { python nifi/scripts/build_flow.py }
    "nifi-export"  { python nifi/scripts/export_flow.py }
    "dremio-setup" { python dremio/scripts/setup_dremio.py }
    "dremio-test"  { python dremio/scripts/run_validation.py }
    "dremio-sql"   { python dremio/scripts/run_validation.py --only $Requete --rows 25 }
    "prerequis"    { python scripts/verifier_prerequis.py }
    "verifier"     { python scripts/verifier_projet.py }

    "backfill" {
        Airflow-Trigger "lakehouse_medallion"
        Airflow-Trigger "fakestore_backfill_history"
        Write-Host "Backfill déclenché — suivi sur http://localhost:8085" -ForegroundColor Green
    }
    "ingest"       { Airflow-Trigger "fakestore_daily_ingestion" }
    "medaillon"    { Airflow-Trigger "lakehouse_medallion" }

    "qualite"      { Spark-Submit "maintenance/data_quality_checks.py" }
    "maintenance"  { Spark-Submit "maintenance/iceberg_maintenance.py" }
    "demo-iceberg" { Spark-Submit "maintenance/demo_iceberg_features.py" }

    "reset" {
        Write-Host "Cette opération SUPPRIME toutes les données du lakehouse." -ForegroundColor Red
        $reponse = Read-Host "Confirmer en tapant OUI"
        if ($reponse -eq "OUI") {
            docker compose down -v --remove-orphans
            Write-Host "Plateforme remise à zéro." -ForegroundColor Green
        } else {
            Write-Host "Annulé." -ForegroundColor Yellow
        }
    }

    "demarrage-complet" {
        Titre "0/5  Contrôle des prérequis"
        python scripts/verifier_prerequis.py
        if ($LASTEXITCODE -ne 0) {
            Write-Host "Prérequis non réunis — corriger les points bloquants ci-dessus." -ForegroundColor Red
            exit 1
        }
        Titre "1/5  Construction des images"
        docker compose build
        Titre "2/5  Démarrage des services"
        docker compose up -d
        Titre "3/5  Attente de la disponibilité (90 s)"
        Start-Sleep -Seconds 90
        Titre "4/5  Configuration de NiFi et Dremio"
        python nifi/scripts/build_flow.py
        python dremio/scripts/setup_dremio.py
        Titre "5/5  Reconstitution de l'historique"
        Airflow-Trigger "lakehouse_medallion"
        Airflow-Trigger "fakestore_backfill_history"
        & $PSCommandPath urls
    }

    default {
        Write-Host "Action inconnue : $Action" -ForegroundColor Red
        Write-Host "Lancer '.\scripts\plateforme.ps1 aide' pour la liste." -ForegroundColor Yellow
        exit 1
    }
}
