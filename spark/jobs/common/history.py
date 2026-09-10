"""Modèle d'historisation sur 6 mois — contrainte 4.1 du sujet.

------------------------------------------------------------------------------
 LE PROBLÈME
------------------------------------------------------------------------------
FakeStoreAPI n'expose que l'état courant : pas de champ de date sur /products
ni /users, et seulement 7 paniers figés sur /carts. Une ingestion naïve
répétée pendant 6 mois produirait 180 snapshots strictement identiques : un
historique techniquement présent, mais analytiquement mort (aucune tendance,
aucune évolution à observer en couche Gold).

------------------------------------------------------------------------------
 LA STRATÉGIE RETENUE — en trois niveaux, du plus réel au plus reconstruit
------------------------------------------------------------------------------
 Niveau 1 — Historisation RÉELLE (toujours active)
   NiFi est déclenché une fois par date logique sur une fenêtre de 180 jours
   (DAG `fakestore_backfill_history`). Chaque exécution est un véritable appel
   HTTP à l'API, horodaté, déposé dans sa propre partition
   `ingest_date=YYYY-MM-DD`. La couche Bronze conserve donc 180 snapshots
   authentiques : si l'API change entre deux appels, le lakehouse le capte.
   C'est ce niveau qui satisfait littéralement la contrainte « historique
   d'environ 6 mois ingéré depuis cette API ».

 Niveau 2 — Horodatage RÉEL redistribué (paniers)
   Les 7 paniers de /carts portent une vraie date, mais figée en 2019-2020.
   Elle est projetée de façon déterministe sur la fenêtre des 6 mois, ce qui
   conserve les enregistrements réels tout en les rendant exploitables.

 Niveau 3 — Reconstitution SYNTHÉTIQUE assumée (désactivable)
   Pour que les analyses temporelles Gold aient du sens (évolution des prix,
   accumulation des avis, flux de commandes), un modèle déterministe applique
   une dérive aux mesures. Deux propriétés le rendent défendable :

     (a) DÉTERMINISME : aucune fonction aléatoire. Tout dérive de
         `hash(clé | date | graine)`. Deux exécutions produisent exactement
         le même historique — le pipeline reste idempotent et rejouable.

     (b) ANCRAGE SUR LE RÉEL : la valeur renvoyée aujourd'hui par l'API est
         traitée comme le POINT D'ARRIVÉE de l'historique, et le modèle
         rétro-projette le passé. Sur la date la plus récente, le facteur vaut
         exactement 1.0 : la donnée observée n'est jamais altérée.

   Toute ligne issue de ce niveau porte `is_reconstructed = true` et la mesure
   d'origine reste disponible dans une colonne `*_source`. Le niveau 3 se
   coupe intégralement avec `SYNTHETIC_HISTORY_ENABLED=false` : le pipeline
   continue de tourner, seules les tendances deviennent plates.

 Cette séparation explicite entre mesure observée et mesure reconstruite est
 le point important : le lakehouse ne ment jamais sur la nature d'une donnée.
------------------------------------------------------------------------------
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession

from common.config import SETTINGS

# Longueur de la fenêtre d'historisation, en jours.
HISTORY_DAYS = SETTINGS.history_months * 30


# --------------------------------------------------------------------------- #
#  Fenêtre temporelle
# --------------------------------------------------------------------------- #
def history_window(end: date | None = None) -> Tuple[date, date]:
    """Bornes [début, fin] de la fenêtre d'historisation."""
    end_date = end or date.today()
    return end_date - timedelta(days=HISTORY_DAYS - 1), end_date


def history_dates(end: date | None = None) -> list[str]:
    """Liste des dates logiques à ingérer, du plus ancien au plus récent."""
    start, end_date = history_window(end)
    return [
        (start + timedelta(days=i)).isoformat()
        for i in range((end_date - start).days + 1)
    ]


# --------------------------------------------------------------------------- #
#  Générateur pseudo-aléatoire déterministe, exprimé en SQL Spark
# --------------------------------------------------------------------------- #
def uniform_expr(key_expr: str, date_expr: str, salt: str) -> str:
    """Expression SQL renvoyant un réel déterministe dans [0, 1[.

    `hash()` de Spark est stable entre versions et entre exécutions (Murmur3),
    ce qui garantit la reproductibilité de l'historique. On évite volontairement
    `rand()`, non déterministe, et une UDF Python, qui casserait le pushdown.
    """
    seed = SETTINGS.synthetic_seed
    return (
        f"(pmod(hash(concat_ws('|', cast({key_expr} as string), "
        f"cast({date_expr} as string), '{salt}', '{seed}')), 100000) / 100000.0)"
    )


def day_index_expr(date_expr: str, end_expr: str) -> str:
    """Index du jour dans la fenêtre : 0 = plus ancien, HISTORY_DAYS-1 = récent."""
    return f"(datediff({date_expr}, date_sub({end_expr}, {HISTORY_DAYS - 1})))"


# --------------------------------------------------------------------------- #
#  Modèles de dérive
# --------------------------------------------------------------------------- #
def price_factor_expr(product_key: str, date_expr: str, end_expr: str) -> str:
    """Indice de prix appliqué au prix courant pour reconstituer le passé.

    Trois composantes, toutes bornées :
      * tendance      : -6 % au début de la fenêtre, 0 % à la fin (inflation
                        douce ~1 % / mois, cohérente avec du retail) ;
      * saisonnalité  : sinusoïde de période trimestrielle, amplitude 4 %,
                        déphasée par produit (les catégories ne soldent pas
                        toutes en même temps) ;
      * bruit produit : ±2 %, déterministe, pour éviter des courbes trop lisses.

    Contrainte forte : sur le dernier jour, le facteur vaut exactement 1.0.
    """
    if not SETTINGS.synthetic_history:
        return "1.0"

    di = day_index_expr(date_expr, end_expr)
    last = HISTORY_DAYS - 1
    # La phase ne dépend que du produit (date fixée à une constante) : chaque
    # produit garde le même calendrier de promotions sur toute la fenêtre.
    phase = f"({uniform_expr(product_key, chr(39) + 'static' + chr(39), 'phase')} * 6.2831853)"
    noise = f"({uniform_expr(product_key, date_expr, 'price')} - 0.5)"

    trend = f"(-0.06 * (1.0 - ({di} / {float(last)})))"
    season = f"(0.04 * sin((2.0 * 3.14159265 * {di} / 91.0) + {phase}))"
    jitter = f"(0.04 * {noise})"

    return (
        f"CASE WHEN {di} >= {last} THEN 1.0 "
        f"ELSE greatest(0.55, least(1.35, 1.0 + {trend} + {season} + {jitter})) END"
    )


def review_growth_expr(date_expr: str, end_expr: str) -> str:
    """Part du volume d'avis déjà accumulée à une date donnée.

    Les avis ne disparaissent pas : le compteur renvoyé aujourd'hui est un
    cumul. On le rétro-projette par une croissance logistique douce allant de
    68 % au début de la fenêtre à 100 % le dernier jour.
    """
    if not SETTINGS.synthetic_history:
        return "1.0"

    di = day_index_expr(date_expr, end_expr)
    last = HISTORY_DAYS - 1
    progress = f"({di} / {float(last)})"
    return f"least(1.0, 0.68 + 0.32 * pow({progress}, 0.85))"


def order_intensity_expr(date_expr: str) -> str:
    """Probabilité qu'un panier « modèle » se matérialise un jour donné.

    Saisonnalité hebdomadaire simple mais réaliste : creux en milieu de
    semaine, pic le week-end. `dayofweek()` renvoie 1 = dimanche.
    """
    return (
        "CASE dayofweek(" + date_expr + ") "
        "WHEN 1 THEN 0.80 "   # dimanche
        "WHEN 7 THEN 0.85 "   # samedi
        "WHEN 6 THEN 0.70 "   # vendredi
        "WHEN 2 THEN 0.45 "
        "WHEN 3 THEN 0.40 "
        "WHEN 4 THEN 0.42 "
        "ELSE 0.50 END"
    )


# --------------------------------------------------------------------------- #
#  Dimension de dates
# --------------------------------------------------------------------------- #
def build_date_dimension(spark: SparkSession, end: date | None = None) -> DataFrame:
    """Dimension calendaire couvrant la fenêtre d'historisation.

    Sert de squelette temporel aux tables Gold : sans elle, un jour sans
    commande disparaîtrait des séries et fausserait les moyennes mobiles.
    """
    start, end_date = history_window(end)
    return spark.sql(
        f"""
        SELECT
            d                                        AS date_key,
            year(d)                                  AS year,
            month(d)                                 AS month,
            dayofmonth(d)                            AS day,
            weekofyear(d)                            AS week_of_year,
            date_format(d, 'EEEE')                   AS day_name,
            dayofweek(d) IN (1, 7)                   AS is_weekend,
            date_trunc('MONTH', d)                   AS month_start,
            {day_index_expr('d', f"date('{end_date.isoformat()}')")} AS day_index
        FROM (
            SELECT explode(sequence(
                date('{start.isoformat()}'),
                date('{end_date.isoformat()}'),
                interval 1 day
            )) AS d
        )
        """
    )
