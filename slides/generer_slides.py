#!/usr/bin/env python3
"""Génère le support PowerPoint de la vidéo de soutenance (15 minutes).

Le sujet est explicite : « Montrez l'écran réel […], pas des diapositives
seules ». Ce support n'est donc PAS le contenu de la vidéo — c'est sa colonne
vertébrale. Il alterne deux types de diapositives :

  * les diapositives d'EXPOSÉ, projetées à l'écran (architecture, choix de
    conception, modèle d'historisation) ;
  * les diapositives de BASCULE, marquées « À L'ÉCRAN », qui annoncent ce qui
    doit être montré en direct et servent de repère de minutage.

Chaque diapositive porte des notes de présentateur : le minutage cible, ce
qu'il faut dire, et ce qu'il faut avoir ouvert à l'écran.

Usage :
    python slides/generer_slides.py
"""

from __future__ import annotations

from pathlib import Path

from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_CONNECTOR, MSO_SHAPE
from pptx.enum.text import MSO_ANCHOR, PP_ALIGN
from pptx.util import Emu, Inches, Pt

SORTIE = Path(__file__).resolve().parent / "soutenance_lakehouse_nifi.pptx"

# --------------------------------------------------------------------------- #
#  Charte graphique
# --------------------------------------------------------------------------- #
FOND = RGBColor(0x0D, 0x15, 0x22)
PANNEAU = RGBColor(0x16, 0x23, 0x38)
PANNEAU_CLAIR = RGBColor(0x1E, 0x2F, 0x49)
TEXTE = RGBColor(0xE9, 0xEF, 0xF7)
TEXTE_DOUX = RGBColor(0x9A, 0xB0, 0xCC)
TEXTE_SOMBRE = RGBColor(0x0D, 0x15, 0x22)

ORANGE = RGBColor(0xFF, 0x7A, 0x1A)   # ingestion / NiFi
BLEU = RGBColor(0x4C, 0xA5, 0xFF)     # traitement / Spark
TURQUOISE = RGBColor(0x2E, 0xC4, 0xB6)  # stockage / catalogue
VIOLET = RGBColor(0xA9, 0x8B, 0xFF)           # requêtage / Dremio
VERT = RGBColor(0x5A, 0xD1, 0x8B)     # validé
ROUGE = RGBColor(0xF2, 0x6B, 0x6B)    # alerte

BRONZE_C = RGBColor(0xC0, 0x7A, 0x3E)
ARGENT_C = RGBColor(0x9E, 0xAD, 0xBE)
OR_C = RGBColor(0xE0, 0xB4, 0x4A)

POLICE = "Segoe UI"
POLICE_MONO = "Consolas"

L = Inches(13.333)
H = Inches(7.5)
MARGE = Inches(0.75)


# --------------------------------------------------------------------------- #
#  Primitives de mise en page
# --------------------------------------------------------------------------- #
def nouvelle_presentation() -> Presentation:
    prs = Presentation()
    prs.slide_width = L
    prs.slide_height = H
    return prs


def diapo(prs: Presentation):
    slide = prs.slides.add_slide(prs.slide_layouts[6])  # vierge
    fond = slide.background.fill
    fond.solid()
    fond.fore_color.rgb = FOND
    return slide


def rect(
    slide,
    x,
    y,
    w,
    h,
    couleur=PANNEAU,
    forme=MSO_SHAPE.ROUNDED_RECTANGLE,
    bordure=None,
    epaisseur=Pt(1.25),
    transparence=None,
):
    shape = slide.shapes.add_shape(forme, x, y, w, h)
    shape.fill.solid()
    shape.fill.fore_color.rgb = couleur
    if bordure is None:
        shape.line.fill.background()
    else:
        shape.line.color.rgb = bordure
        shape.line.width = epaisseur
    shape.shadow.inherit = False
    if forme == MSO_SHAPE.ROUNDED_RECTANGLE:
        try:
            shape.adjustments[0] = 0.08
        except (IndexError, KeyError):
            pass
    return shape


def texte(
    slide,
    contenu,
    x,
    y,
    w,
    h,
    taille=18,
    couleur=TEXTE,
    gras=False,
    italique=False,
    align=PP_ALIGN.LEFT,
    ancre=MSO_ANCHOR.TOP,
    police=POLICE,
    interligne=1.0,
    espace_avant=0,
):
    boite = slide.shapes.add_textbox(x, y, w, h)
    cadre = boite.text_frame
    cadre.word_wrap = True
    cadre.vertical_anchor = ancre
    cadre.margin_left = cadre.margin_right = Emu(0)
    cadre.margin_top = cadre.margin_bottom = Emu(0)

    lignes = contenu.split("\n") if isinstance(contenu, str) else list(contenu)
    for index, ligne in enumerate(lignes):
        para = cadre.paragraphs[0] if index == 0 else cadre.add_paragraph()
        para.alignment = align
        para.line_spacing = interligne
        if index > 0 and espace_avant:
            para.space_before = Pt(espace_avant)
        run = para.add_run()
        run.text = ligne
        run.font.size = Pt(taille)
        run.font.color.rgb = couleur
        run.font.bold = gras
        run.font.italic = italique
        run.font.name = police
    return boite


def riche(slide, x, y, w, h, blocs, taille=16, interligne=1.25, espace=6):
    """Paragraphes composés de fragments (texte, couleur, gras, police)."""
    boite = slide.shapes.add_textbox(x, y, w, h)
    cadre = boite.text_frame
    cadre.word_wrap = True
    cadre.margin_left = cadre.margin_right = Emu(0)
    cadre.margin_top = cadre.margin_bottom = Emu(0)

    for index, fragments in enumerate(blocs):
        para = cadre.paragraphs[0] if index == 0 else cadre.add_paragraph()
        para.line_spacing = interligne
        if index > 0:
            para.space_before = Pt(espace)
        for fragment in fragments:
            run = para.add_run()
            run.text = fragment.get("t", "")
            run.font.size = Pt(fragment.get("taille", taille))
            run.font.color.rgb = fragment.get("couleur", TEXTE)
            run.font.bold = fragment.get("gras", False)
            run.font.italic = fragment.get("italique", False)
            run.font.name = fragment.get("police", POLICE)
    return boite


def fleche(slide, x1, y1, x2, y2, couleur=TEXTE_DOUX, epaisseur=Pt(1.6)):
    conn = slide.shapes.add_connector(MSO_CONNECTOR.STRAIGHT, x1, y1, x2, y2)
    conn.line.color.rgb = couleur
    conn.line.width = epaisseur
    return conn


def notes(slide, contenu: str) -> None:
    slide.notes_slide.notes_text_frame.text = contenu.strip()


def bandeau_titre(slide, titre, sous_titre="", accent=BLEU, numero=""):
    rect(slide, Inches(0), Inches(0), Inches(0.13), Inches(1.55), accent, MSO_SHAPE.RECTANGLE)
    texte(slide, titre, MARGE, Inches(0.42), Inches(10.6), Inches(0.6), 30, TEXTE, True)
    if sous_titre:
        texte(slide, sous_titre, MARGE, Inches(1.02), Inches(10.6), Inches(0.4), 15, TEXTE_DOUX)
    if numero:
        texte(
            slide, numero, Inches(11.4), Inches(0.42), Inches(1.2), Inches(0.5),
            30, accent, True, align=PP_ALIGN.RIGHT,
        )


def pied(slide, gauche, droite=""):
    texte(slide, gauche, MARGE, Inches(6.92), Inches(8.5), Inches(0.35), 11, TEXTE_DOUX)
    if droite:
        texte(
            slide, droite, Inches(9.0), Inches(6.92), Inches(3.6), Inches(0.35),
            11, TEXTE_DOUX, align=PP_ALIGN.RIGHT,
        )


def puce_carree(slide, x, y, couleur, taille=Inches(0.11)):
    rect(slide, x, y, taille, taille, couleur, MSO_SHAPE.RECTANGLE)


def carte(slide, x, y, w, h, accent, titre, corps, taille_titre=16, taille_corps=13):
    rect(slide, x, y, w, h, PANNEAU)
    rect(slide, x, y, Inches(0.075), h, accent, MSO_SHAPE.RECTANGLE)
    texte(slide, titre, x + Inches(0.28), y + Inches(0.2), w - Inches(0.5),
          Inches(0.35), taille_titre, accent, True)
    texte(slide, corps, x + Inches(0.28), y + Inches(0.66), w - Inches(0.5),
          h - Inches(0.85), taille_corps, TEXTE_DOUX, interligne=1.22)


def tableau(slide, x, y, w, colonnes, lignes, largeurs, taille=13, hauteur_ligne=Inches(0.42)):
    """Tableau dessiné à la main : contrôle total du rendu sombre."""
    total = sum(largeurs)
    positions = []
    curseur = x
    for part in largeurs:
        largeur = Emu(int(w * part / total))
        positions.append((curseur, largeur))
        curseur = Emu(curseur + largeur)

    entete = rect(slide, x, y, w, Inches(0.42), PANNEAU_CLAIR, MSO_SHAPE.RECTANGLE)
    entete.line.fill.background()
    for (cx, cw), libelle in zip(positions, colonnes):
        texte(slide, libelle, cx + Inches(0.14), y + Inches(0.09), cw - Inches(0.2),
              Inches(0.3), taille, TEXTE, True)

    curseur_y = Emu(y + Inches(0.42))
    for index, ligne in enumerate(lignes):
        if index % 2 == 0:
            bande = rect(slide, x, curseur_y, w, hauteur_ligne, PANNEAU, MSO_SHAPE.RECTANGLE)
            bande.line.fill.background()
        for (cx, cw), cellule in zip(positions, ligne):
            valeur = cellule if isinstance(cellule, str) else cellule[0]
            couleur = TEXTE_DOUX if isinstance(cellule, str) else cellule[1]
            police = POLICE if isinstance(cellule, str) else cellule[2] if len(cellule) > 2 else POLICE
            texte(slide, valeur, cx + Inches(0.14), curseur_y + Inches(0.09),
                  cw - Inches(0.2), hauteur_ligne - Inches(0.1), taille, couleur,
                  police=police)
        curseur_y = Emu(curseur_y + hauteur_ligne)
    return curseur_y


def bandeau_ecran(slide, minutage: str):
    """Bandeau des diapositives de bascule vers la démonstration live."""
    barre = rect(slide, Inches(0), Inches(0), L, Inches(0.34), ORANGE, MSO_SHAPE.RECTANGLE)
    barre.line.fill.background()
    texte(slide, "À L'ÉCRAN — DÉMONSTRATION EN DIRECT", MARGE, Inches(0.05),
          Inches(7.0), Inches(0.26), 13, TEXTE_SOMBRE, True)
    texte(slide, minutage, Inches(9.4), Inches(0.05), Inches(3.2), Inches(0.26),
          13, TEXTE_SOMBRE, True, align=PP_ALIGN.RIGHT)


# =========================================================================== #
#  DIAPOSITIVES
# =========================================================================== #
def d01_titre(prs):
    slide = diapo(prs)
    rect(slide, Inches(0), Inches(0), Inches(0.18), H, ORANGE, MSO_SHAPE.RECTANGLE)
    rect(slide, Inches(0.18), Inches(0), Inches(0.18), H, TURQUOISE, MSO_SHAPE.RECTANGLE)
    rect(slide, Inches(0.36), Inches(0), Inches(0.18), H, BLEU, MSO_SHAPE.RECTANGLE)

    texte(slide, "DIT — DAKAR INSTITUTE OF TECHNOLOGIES", Inches(1.15), Inches(1.15),
          Inches(10.5), Inches(0.35), 14, TEXTE_DOUX, True)
    texte(slide, "Master 2 — Ingénierie des Données / Big Data", Inches(1.15),
          Inches(1.52), Inches(10.5), Inches(0.35), 14, TEXTE_DOUX)

    texte(slide, "Plateforme Data Lakehouse", Inches(1.15), Inches(2.25),
          Inches(11.0), Inches(0.85), 46, TEXTE, True)
    texte(slide, "Ingestion Apache NiFi depuis une API publique vers MinIO",
          Inches(1.15), Inches(3.15), Inches(11.0), Inches(0.5), 24, ORANGE)
    texte(slide, "Médaillon Bronze → Silver → Gold en Apache Iceberg, catalogue Nessie, requêtage Dremio",
          Inches(1.15), Inches(3.68), Inches(11.0), Inches(0.5), 16, TEXTE_DOUX)

    rect(slide, Inches(1.15), Inches(4.45), Inches(10.6), Inches(0.04),
         PANNEAU_CLAIR, MSO_SHAPE.RECTANGLE)

    riche(slide, Inches(1.15), Inches(4.8), Inches(11.0), Inches(1.4), [
        [{"t": "Module : ", "couleur": TEXTE_DOUX},
         {"t": "Architectures Data Lakehouse & Ingestion de données", "couleur": TEXTE}],
        [{"t": "Enseignant : ", "couleur": TEXTE_DOUX},
         {"t": "M. PENE — Data Engineer & Data Lecturer", "couleur": TEXTE}],
        [{"t": "Épreuve : ", "couleur": TEXTE_DOUX},
         {"t": "examen individuel à domicile — soutenance vidéo 15 minutes", "couleur": TEXTE}],
    ], taille=15, interligne=1.5)

    texte(slide, "Nom Prénom  ·  Promotion 2025-2026", Inches(1.15), Inches(6.6),
          Inches(8.0), Inches(0.4), 15, TURQUOISE, True)

    notes(slide, """
[00:00 — 00:20]  OUVERTURE

« Bonjour, je présente ma plateforme data lakehouse conteneurisée, construite
de zéro à partir du cahier des charges : ingestion NiFi depuis FakeStoreAPI
vers MinIO, médaillon Bronze/Silver/Gold en Iceberg catalogué par Nessie,
orchestration Airflow, requêtage Dremio. »

À PRÉPARER AVANT D'ENREGISTRER :
  * la pile démarrée depuis au moins 5 minutes, tous les services en healthy ;
  * le backfill des 6 mois déjà exécuté (sinon la démo dure trop longtemps) ;
  * onglets navigateur ouverts DANS CET ORDRE : NiFi, Airflow, MinIO, Dremio,
    Spark master, Grafana ;
  * VS Code ouvert sur le dépôt, docker-compose.yml déjà affiché ;
  * un terminal en plein écran, police agrandie (14 pt minimum) ;
  * REMPLACER « Nom Prénom » sur cette diapositive.
""")


def d02_plan(prs):
    slide = diapo(prs)
    bandeau_titre(slide, "Déroulé de la soutenance", "7 séquences — 15 minutes", TURQUOISE)

    etapes = [
        ("1", "Architecture conçue et choix structurants", "1 à 2 min", "EXPOSÉ", TURQUOISE),
        ("2", "Revue du docker-compose : services, réseau, volumes, ports", "1 à 2 min", "ÉCRAN", ORANGE),
        ("3", "Flow NiFi : processeurs, dépôt MinIO, routage, erreurs", "3 min", "ÉCRAN", ORANGE),
        ("4", "Jobs Spark du médaillon Bronze / Silver / Gold", "2 à 3 min", "ÉCRAN", BLEU),
        ("5", "Test Dremio : par couche, jointure, agrégation", "2 à 3 min", "ÉCRAN", VIOLET),
        ("6", "Démonstration de bout en bout, en direct", "2 à 3 min", "ÉCRAN", VERT),
        ("7", "Bilan : difficultés, choix, bonus", "1 min", "EXPOSÉ", TURQUOISE),
    ]

    y = Inches(1.75)
    for numero, libelle, duree, nature, couleur in etapes:
        rect(slide, MARGE, y, Inches(11.85), Inches(0.63), PANNEAU)
        rect(slide, MARGE, y, Inches(0.07), Inches(0.63), couleur, MSO_SHAPE.RECTANGLE)
        texte(slide, numero, MARGE + Inches(0.3), y + Inches(0.14), Inches(0.4),
              Inches(0.4), 19, couleur, True)
        texte(slide, libelle, MARGE + Inches(0.85), y + Inches(0.17), Inches(7.6),
              Inches(0.4), 16, TEXTE)
        texte(slide, duree, MARGE + Inches(8.6), y + Inches(0.19), Inches(1.3),
              Inches(0.35), 14, TEXTE_DOUX, align=PP_ALIGN.RIGHT)
        pastille = rect(slide, MARGE + Inches(10.15), y + Inches(0.15),
                        Inches(1.05), Inches(0.33), couleur, MSO_SHAPE.ROUNDED_RECTANGLE)
        texte(slide, nature, MARGE + Inches(10.15), y + Inches(0.2), Inches(1.05),
              Inches(0.25), 11, TEXTE_SOMBRE, True, align=PP_ALIGN.CENTER)
        y = Emu(y + Inches(0.75))

    pied(slide, "Cinq séquences sur sept se déroulent sur l'écran réel — les diapositives ne servent qu'à structurer.")

    notes(slide, """
[00:20 — 00:45]  PLAN

« Voici le déroulé : je commence par l'architecture et les choix structurants,
puis l'essentiel se passe sur l'écran réel — le docker-compose, le flow NiFi,
les jobs Spark, Dremio, et enfin une démonstration complète de bout en bout.
Je termine par le bilan. »

Annoncer le plan vite : c'est du temps pris sur la démonstration.
""")


def d03_cahier_des_charges(prs):
    slide = diapo(prs)
    bandeau_titre(slide, "Ce qui est imposé, ce que j'ai conçu",
                  "Le sujet est un cahier des charges, pas une architecture", TURQUOISE)

    rect(slide, MARGE, Inches(1.7), Inches(5.75), Inches(4.9), PANNEAU)
    rect(slide, MARGE, Inches(1.7), Inches(5.75), Inches(0.5), PANNEAU_CLAIR)
    texte(slide, "IMPOSÉ PAR LE SUJET", MARGE + Inches(0.3), Inches(1.83),
          Inches(5.0), Inches(0.3), 14, TEXTE_DOUX, True)

    impose = [
        ("Source", "FakeStoreAPI — aucun CSV statique"),
        ("Ingestion", "NiFi seul point d'entrée, sans transformation"),
        ("Stockage", "MinIO — zone brute séparée du warehouse"),
        ("Format", "Iceberg, catalogue Project Nessie"),
        ("Traitement", "Spark : 1 master + au moins 1 worker"),
        ("Orchestration", "Airflow"),
        ("Requêtage", "Dremio OSS — sans substitut"),
        ("Supervision", "Prometheus + Grafana"),
        ("Historique", "environ 6 mois de données"),
    ]
    y = Inches(2.35)
    for libelle, detail in impose:
        puce_carree(slide, MARGE + Inches(0.32), y + Inches(0.11), TEXTE_DOUX)
        texte(slide, libelle, MARGE + Inches(0.58), y, Inches(1.5), Inches(0.3), 13, TEXTE, True)
        texte(slide, detail, MARGE + Inches(2.1), y, Inches(3.5), Inches(0.3), 13, TEXTE_DOUX)
        y = Emu(y + Inches(0.45))

    x2 = Inches(6.85)
    rect(slide, x2, Inches(1.7), Inches(5.75), Inches(4.9), PANNEAU)
    rect(slide, x2, Inches(1.7), Inches(5.75), Inches(0.5), ORANGE)
    texte(slide, "DE MA CONCEPTION — ET DONC ÉVALUÉ", x2 + Inches(0.3), Inches(1.83),
          Inches(5.0), Inches(0.3), 14, TEXTE_SOMBRE, True)

    concu = [
        ("Topologie", "11 services, 1 réseau, 13 volumes, ordre de boot"),
        ("Flow NiFi", "12 processeurs, 3 chemins de reprise, file de rebut"),
        ("Zone brute", "convention de clés partitionnée par date"),
        ("Médaillon", "contrat de chaque couche, 10 tables Iceberg"),
        ("Historisation", "modèle déterministe à 3 niveaux"),
        ("Orchestration", "3 DAGs, option A justifiée"),
        ("Idempotence", "cache NiFi + overwritePartitions Spark"),
        ("Qualité", "7 contrôles dont une réconciliation croisée"),
        ("Restitution", "10 requêtes de validation + 3 vues Dremio"),
    ]
    y = Inches(2.35)
    for libelle, detail in concu:
        puce_carree(slide, x2 + Inches(0.32), y + Inches(0.11), ORANGE)
        texte(slide, libelle, x2 + Inches(0.58), y, Inches(1.6), Inches(0.3), 13, TEXTE, True)
        texte(slide, detail, x2 + Inches(2.2), y, Inches(3.4), Inches(0.3), 13, TEXTE_DOUX)
        y = Emu(y + Inches(0.45))

    notes(slide, """
[00:45 — 01:10]  CADRAGE

« Le sujet impose une pile technique, mais pas d'architecture. Tout ce qui est
à droite relève de mes choix : la topologie Docker, la conception du flow NiFi,
la convention de nommage de la zone brute, le contenu de chaque couche du
médaillon, la stratégie d'historisation, l'orchestration. C'est là-dessus que
je vais m'attarder. »

Diapositive à passer rapidement — elle pose le cadre, elle n'est pas le fond.
""")


def d04_architecture(prs):
    slide = diapo(prs)
    bandeau_titre(slide, "Architecture de la plateforme",
                  "Un seul réseau Docker — chaque service en désigne un autre par son nom", BLEU)

    def bloc(x, y, w, h, couleur, titre_bloc, detail, taille_titre=14, taille_detail=11):
        rect(slide, x, y, w, h, PANNEAU, bordure=couleur, epaisseur=Pt(1.6))
        texte(slide, titre_bloc, x + Inches(0.14), y + Inches(0.12), w - Inches(0.28),
              Inches(0.3), taille_titre, couleur, True, align=PP_ALIGN.CENTER)
        texte(slide, detail, x + Inches(0.14), y + Inches(0.44), w - Inches(0.28),
              h - Inches(0.55), taille_detail, TEXTE_DOUX, align=PP_ALIGN.CENTER,
              interligne=1.12)

    # Source
    bloc(Inches(0.75), Inches(1.72), Inches(2.0), Inches(0.95), TEXTE_DOUX,
         "FakeStoreAPI", "/products\n/users · /carts")

    # NiFi
    bloc(Inches(3.15), Inches(1.6), Inches(2.55), Inches(1.2), ORANGE,
         "APACHE NIFI", "unique point d'ingestion\ncollecte · contrôle · dépôt")

    # MinIO
    bloc(Inches(6.1), Inches(1.6), Inches(2.75), Inches(1.2), TURQUOISE,
         "MinIO", "lakehouse-raw  (JSON)\nlakehouse-warehouse  (Parquet)")

    # Nessie
    bloc(Inches(9.25), Inches(1.6), Inches(3.3), Inches(1.2), TURQUOISE,
         "PROJECT NESSIE", "catalogue Iceberg versionné\nversion store JDBC PostgreSQL")

    # Spark
    bloc(Inches(3.15), Inches(3.25), Inches(5.7), Inches(1.35), BLEU,
         "APACHE SPARK — 1 master + 2 workers",
         "bronze.raw_*   →   silver.dim_* / fct_*   →   gold.gold_*\n"
         "10 tables Apache Iceberg", 15, 12)

    # Airflow
    bloc(Inches(0.75), Inches(3.25), Inches(2.0), Inches(1.35), BLEU,
         "AIRFLOW", "backfill\ningestion\nmédaillon")

    # Dremio
    bloc(Inches(9.25), Inches(3.25), Inches(3.3), Inches(1.35), VIOLET,
         "DREMIO OSS", "moteur SQL du lakehouse\n10 requêtes de validation\n3 vues métier")

    # Socle
    bloc(Inches(0.75), Inches(5.1), Inches(4.1), Inches(0.95), TEXTE_DOUX,
         "PostgreSQL", "métadonnées Airflow + Nessie")
    bloc(Inches(5.15), Inches(5.1), Inches(7.4), Inches(0.95), TEXTE_DOUX,
         "Prometheus + Grafana",
         "infrastructure · NiFi (débit d'ingestion) · MinIO · Nessie · Spark")

    # Flèches principales
    fleche(slide, Inches(2.45), Inches(2.2), Inches(3.05), Inches(2.2), ORANGE, Pt(2.2))
    fleche(slide, Inches(5.65), Inches(2.2), Inches(6.1), Inches(2.2), ORANGE, Pt(2.2))
    fleche(slide, Inches(8.85), Inches(2.2), Inches(9.25), Inches(2.2), TURQUOISE, Pt(2.0))
    fleche(slide, Inches(7.4), Inches(2.8), Inches(7.4), Inches(3.25), BLEU, Pt(2.0))
    fleche(slide, Inches(2.75), Inches(3.92), Inches(3.15), Inches(3.92), BLEU, Pt(2.0))

    fleche(slide, Inches(8.85), Inches(3.92), Inches(9.25), Inches(3.92), VIOLET, Pt(2.0))
    fleche(slide, Inches(10.9), Inches(2.8), Inches(10.9), Inches(3.25), VIOLET, Pt(2.0))

    # Étiquettes de protocole, centrées dans l'espace laissé entre les blocs
    texte(slide, "HTTP", Inches(2.45), Inches(1.9), Inches(0.6), Inches(0.22), 10,
          ORANGE, True, align=PP_ALIGN.CENTER)
    texte(slide, "S3", Inches(5.65), Inches(1.9), Inches(0.45), Inches(0.22), 10,
          ORANGE, True, align=PP_ALIGN.CENTER)

    pied(slide, "Aucune adresse IP dans la configuration : la résolution DNS du réseau « lakehouse » fait tout le travail.",
         "docs/architecture.md §1")

    notes(slide, """
[01:10 — 02:20]  ARCHITECTURE — LE CŒUR DE L'EXPOSÉ

Suivre le chemin de la donnée, de gauche à droite :

« Une donnée entre par NiFi — c'est le seul composant qui parle à l'extérieur.
Elle atterrit en JSON brut dans le bucket lakehouse-raw de MinIO, partitionnée
par date d'ingestion. Spark la reprend et construit les trois couches du
médaillon en tables Iceberg ; ces tables sont décrites par Nessie, qui joue le
rôle de catalogue versionné, et leurs fichiers Parquet vivent dans le second
bucket. Dremio lit ce même catalogue et expose le tout en SQL. Airflow
ordonnance l'ensemble, Prometheus et Grafana l'observent. »

DEUX POINTS À SOULIGNER :
 1. les deux buckets sont volontairement séparés — contrainte du sujet, mais
    aussi deux politiques différentes : rétention 90 jours sur la zone brute,
    versionnement sur le warehouse ;
 2. tout se désigne par nom de service Docker. Depuis le conteneur NiFi,
    localhost désignerait NiFi lui-même : l'endpoint S3 est http://minio:9000.
    C'est LE piège de ce type de plateforme.
""")


def d05_choix(prs):
    slide = diapo(prs)
    bandeau_titre(slide, "Quatre choix structurants", "Et l'alternative écartée à chaque fois", BLEU)

    cartes = [
        (ORANGE, "NiFi déclenché par ListenHTTP",
         "Airflow POSTe {snapshot_date, domains}.\n\n"
         "Écarté : GenerateFlowFile planifié — il ne sait ingérer que "
         "« maintenant », ce qui rend le backfill de 6 mois impossible."),
        (BLEU, "Airflow observe, il n'attend pas de signal",
         "Option A du sujet : le DAG inspecte la zone brute.\n\n"
         "Écarté : option B seule — un signal perdu fait disparaître les "
         "données en silence. L'option B reste câblée et démontrable."),
        (TURQUOISE, "JARs embarqués dans l'image Spark",
         "Iceberg 1.5.2 + Nessie 0.77.1 + S3A figés.\n\n"
         "Écarté : --packages au spark-submit — résolution Maven à chaque "
         "run, dérive possible entre driver et executors."),
        (OR_C, "Gold recalculé intégralement",
         "createOrReplace : un nouveau snapshot Iceberg, pas un effacement.\n\n"
         "Écarté : MERGE incrémental — aucun gain sur ce volume, et un "
         "agrégat peut dériver silencieusement."),
    ]

    positions = [
        (MARGE, Inches(1.8)),
        (Inches(6.85), Inches(1.8)),
        (MARGE, Inches(4.25)),
        (Inches(6.85), Inches(4.25)),
    ]
    for (couleur, titre_c, corps), (x, y) in zip(cartes, positions):
        carte(slide, x, y, Inches(5.75), Inches(2.2), couleur, titre_c, corps, 16, 13)

    notes(slide, """
[02:20 — 03:00]  CHOIX STRUCTURANTS

Ne pas lire les quatre cartes. En développer DEUX, celles qui portent le plus :

 1. « NiFi est déclenché par un ListenHTTP, et pas par un GenerateFlowFile
    planifié. La raison est directement liée à la contrainte des 6 mois :
    c'est l'appelant qui porte la date logique du snapshot. Airflow envoie
    180 demandes, chacune avec sa date, et NiFi range chaque réponse dans la
    bonne partition. Un processeur planifié ne saurait ingérer que maintenant. »

 2. « Sur le déclenchement du médaillon, j'ai retenu l'option A. Un DAG qui
    observe l'état réel du stockage survit à une panne de NiFi ou à un
    redémarrage ; un DAG déclenché par événement perd les données si
    l'événement se perd. J'ai quand même câblé l'option B — je la montrerai. »

Puis enchaîner : « Passons à l'écran. »
""")


def d06_bascule_compose(prs):
    slide = diapo(prs)
    bandeau_ecran(slide, "Séquence 2 · 1 à 2 min")
    texte(slide, "Revue du docker-compose.yml", MARGE, Inches(0.7), Inches(11.8),
          Inches(0.6), 32, TEXTE, True)
    texte(slide, "VS Code — docker-compose.yml, puis un terminal", MARGE, Inches(1.3),
          Inches(11.8), Inches(0.4), 15, ORANGE)

    points = [
        ("Les 11 services", "faire défiler : minio, minio-init, postgres, nessie, "
                            "spark-master + 2 workers, nifi, dremio, airflow (init/web/scheduler), "
                            "prometheus, grafana, node-exporter, cadvisor"),
        ("Le réseau", "un seul bridge « lakehouse » — montrer un endpoint : "
                      "MINIO_ENDPOINT=http://minio:9000, jamais localhost"),
        ("Les volumes", "les 7 volumes NiFi : flowfile, content, provenance, database, "
                        "state, conf, logs — l'état survit à un restart"),
        ("Les healthchecks", "montrer depends_on / condition: service_healthy sur nessie "
                             "et airflow-webserver : l'ordre de démarrage est maîtrisé"),
        ("Les ports", "8080 NiFi, 8085 Airflow, 8090 Spark, 9001 MinIO, 9047 Dremio — "
                      "le master Spark est décalé pour laisser 8080 à NiFi"),
    ]
    y = Inches(2.0)
    for libelle, detail in points:
        rect(slide, MARGE, y, Inches(11.85), Inches(0.82), PANNEAU)
        rect(slide, MARGE, y, Inches(0.07), Inches(0.82), ORANGE, MSO_SHAPE.RECTANGLE)
        texte(slide, libelle, MARGE + Inches(0.32), y + Inches(0.12), Inches(2.3),
              Inches(0.3), 15, TEXTE, True)
        texte(slide, detail, MARGE + Inches(2.75), y + Inches(0.1), Inches(8.85),
              Inches(0.65), 13, TEXTE_DOUX, interligne=1.15)
        y = Emu(y + Inches(0.94))

    texte(slide, "docker compose ps      →  tous les services en « healthy »",
          MARGE, Inches(6.75), Inches(11.8), Inches(0.35), 15, VERT, True, police=POLICE_MONO)

    notes(slide, """
[03:00 — 04:30]  DOCKER-COMPOSE — À L'ÉCRAN

Basculer sur VS Code, docker-compose.yml déjà ouvert. Faire défiler CALMEMENT.

« Onze services sur un seul réseau bridge. Trois choses sur lesquelles je veux
insister. D'abord les endpoints : tout est désigné par nom de service Docker,
jamais par localhost — c'est ce qui permet à NiFi d'écrire dans MinIO. Ensuite
les volumes : NiFi en a sept à lui seul, parce que son état interne — le
repository de flowfiles, la provenance — doit survivre à un redémarrage,
c'est une exigence de la Partie 2. Enfin l'ordre de démarrage : je n'utilise
pas depends_on seul, mais condition: service_healthy. Nessie migre son schéma
au boot, donc PostgreSQL doit être réellement prêt, pas seulement démarré. »

Puis terminal : docker compose ps — montrer les healthy.

NE PAS lire le fichier ligne à ligne. Deux minutes maximum.
""")


def d07_bascule_nifi(prs):
    slide = diapo(prs)
    bandeau_ecran(slide, "Séquence 3 · 3 min · 4 pts")
    texte(slide, "Le flow d'ingestion NiFi", MARGE, Inches(0.7), Inches(11.8),
          Inches(0.6), 32, TEXTE, True)
    texte(slide, "http://localhost:8080/nifi — Process Group « FakeStoreAPI Ingestion »",
          MARGE, Inches(1.3), Inches(11.8), Inches(0.4), 15, ORANGE)

    chaine = [
        ("1", "ListenHTTP", "point d'entrée\n{snapshot_date, domains}"),
        ("2", "EvaluateJsonPath", "extrait la date\nAVANT l'éclatement"),
        ("3", "SplitJson", "1 FlowFile\npar domaine"),
        ("4", "InvokeHTTP", "GET #{api.base.url}\n/${domain}"),
        ("5", "RouteOnAttribute", "contrôle minimal\n200 + non vide + JSON"),
        ("6", "DetectDuplicate", "idempotence\ndomaine::date"),
        ("7", "UpdateAttribute", "construit la clé\nS3 et l'horodatage"),
        ("8", "PutS3Object", "dépôt MinIO\npath-style access"),
    ]

    largeur = Inches(1.42)
    ecart = Inches(1.485)
    x = Inches(0.55)
    y = Inches(2.05)
    for numero, nom, detail in chaine:
        rect(slide, x, y, largeur, Inches(1.5), PANNEAU, bordure=ORANGE, epaisseur=Pt(1.3))
        texte(slide, numero, x, y + Inches(0.1), largeur, Inches(0.25), 12, ORANGE,
              True, align=PP_ALIGN.CENTER)
        texte(slide, nom, x + Inches(0.06), y + Inches(0.38), largeur - Inches(0.12),
              Inches(0.32), 11, TEXTE, True, align=PP_ALIGN.CENTER)
        texte(slide, detail, x + Inches(0.06), y + Inches(0.78), largeur - Inches(0.12),
              Inches(0.6), 9, TEXTE_DOUX, align=PP_ALIGN.CENTER, interligne=1.1)
        if numero != "8":
            fleche(slide, Emu(x + largeur), y + Inches(0.75),
                   Emu(x + ecart), y + Inches(0.75), ORANGE, Pt(1.4))
        x = Emu(x + ecart)

    rect(slide, Inches(0.55), Inches(3.85), Inches(11.9), Inches(0.9), PANNEAU_CLAIR)
    texte(slide, "Trois chemins de reprise, aucune perte silencieuse",
          Inches(0.85), Inches(3.98), Inches(11.3), Inches(0.3), 15, ROUGE, True)
    texte(slide, "appel API en échec → RetryFlowFile ×3 → rebut     ·     "
                 "réponse non conforme → rebut     ·     dépôt S3 en échec → RetryFlowFile ×3 → rebut"
                 "     ·     doublon → journalisé (ce n'est pas une erreur)",
          Inches(0.85), Inches(4.3), Inches(11.3), Inches(0.4), 12, TEXTE_DOUX)

    texte(slide, "À MONTRER DANS L'INTERFACE", MARGE, Inches(5.0), Inches(11.8),
          Inches(0.3), 14, TEXTE, True)
    a_montrer = [
        "ouvrir la configuration de PutS3Object : Endpoint Override URL = http://minio:9000 — insister sur « pas localhost »",
        "montrer Use Path Style Access = true : MinIO n'implémente pas le virtual-hosted style d'AWS",
        "ouvrir le Parameter Context « fakestore-ingestion » : les secrets sont marqués sensibles et chiffrés",
        "montrer les relations sortantes d'InvokeHTTP et où elles vont (RetryFlowFile, puis file de rebut)",
        "ouvrir la console MinIO : lakehouse-raw/fakestore/products/ingest_date=… — la partition Hive",
    ]
    y = Inches(5.38)
    for ligne in a_montrer:
        puce_carree(slide, MARGE + Inches(0.05), y + Inches(0.08), ORANGE)
        texte(slide, ligne, MARGE + Inches(0.32), y, Inches(11.4), Inches(0.3), 12, TEXTE_DOUX)
        y = Emu(y + Inches(0.33))

    notes(slide, """
[04:30 — 07:30]  FLOW NIFI — SÉQUENCE LA PLUS NOTÉE (4 points)

Basculer sur l'interface NiFi, entrer dans le Process Group.

Parcourir la chaîne de gauche à droite en expliquant POURQUOI chaque
processeur est là :

« ListenHTTP reçoit la demande d'Airflow. EvaluateJsonPath extrait la date
logique — et c'est important qu'il soit AVANT le SplitJson : les attributs
d'un FlowFile sont hérités par tous ses fragments, la date suit donc les trois
branches sans être relue. SplitJson éclate le tableau des domaines, ce qui fait
partir les trois appels API en parallèle : un échec sur /carts n'empêche pas
/products d'aboutir. InvokeHTTP est le seul point de sortie vers Internet, et
son URL est paramétrée. »

Sur le contrôle : « RouteOnAttribute fait le contrôle minimal exigé par le
sujet, et rien de plus : code 200, charge utile non vide, type MIME JSON. Pas
de parsing, pas de calcul, pas de jointure — la transformation métier appartient
à Spark, c'est une contrainte explicite du sujet. »

Sur l'idempotence : « DetectDuplicate, avec la clé domaine + date. Rejouer une
ingestion déjà faite n'écrit pas un second objet dans MinIO. »

OUVRIR PutS3Object et montrer les deux propriétés critiques :
   Endpoint Override URL = http://minio:9000  ← « et surtout pas localhost »
   Use Path Style Access = true               ← « MinIO ne fait pas de
                                                  virtual-hosted style »

OUVRIR le Parameter Context : « tout ce qui est spécifique à FakeStoreAPI est
là. Changer api.base.url suffit à brancher ce flow sur une autre API — c'est
le bonus 7.4. Et les secrets sont marqués sensibles, donc chiffrés par NiFi. »

Terminer sur MinIO : montrer l'arborescence réelle avec ingest_date=.

C'est la séquence à ne pas bâcler. Trois minutes pleines.
""")


def d08_medaillon(prs):
    slide = diapo(prs)
    bandeau_titre(slide, "Le contrat de chaque couche",
                  "Ce que chaque couche garantit — et ce qu'elle s'interdit", BLEU)

    colonnes = [
        (BRONZE_C, "BRONZE", "mémoire fidèle",
         ["copie 1:1 de l'API, structures imbriquées conservées",
          "8 colonnes de traçabilité jusqu'à l'objet MinIO",
          "les rejets sont MARQUÉS, jamais supprimés",
          "une seule entorse : le mot de passe est haché",
          "partition : _ingest_date (jour)"],
         "raw_products · raw_users · raw_carts"),
        (ARGENT_C, "SILVER", "vérité métier",
         ["typé : prix en DECIMAL(10,2), pas en DOUBLE",
          "aplati : plus aucune structure imbriquée",
          "dédoublonné : 1 ligne par entité et par jour",
          "enrichi : segments, complétude, géo validée",
          "partition : months(snapshot_date)"],
         "dim_products · dim_customers · fct_order_items"),
        (OR_C, "GOLD", "réponses aux questions",
         ["agrégats prêts à lire — aucun calcul côté BI",
          "jointures inter-domaines matérialisées",
          "moyennes mobiles, RFM, tendances",
          "recalcul complet → nouveau snapshot Iceberg",
          "l'intégrité référentielle est un indicateur"],
         "catalog_kpi · price_trend · sales_daily · customer_360"),
    ]

    x = MARGE
    for couleur, nom, devise, regles, tables in colonnes:
        rect(slide, x, Inches(1.75), Inches(3.85), Inches(4.75), PANNEAU)
        rect(slide, x, Inches(1.75), Inches(3.85), Inches(0.72), couleur)
        texte(slide, nom, x + Inches(0.25), Inches(1.85), Inches(3.3), Inches(0.35),
              20, TEXTE_SOMBRE, True)
        texte(slide, devise, x + Inches(0.25), Inches(2.16), Inches(3.3), Inches(0.25),
              12, TEXTE_SOMBRE)

        y = Inches(2.65)
        for regle in regles:
            puce_carree(slide, x + Inches(0.25), y + Inches(0.09), couleur, Inches(0.09))
            texte(slide, regle, x + Inches(0.48), y, Inches(3.15), Inches(0.55),
                  12, TEXTE_DOUX, interligne=1.15)
            y = Emu(y + Inches(0.62))

        rect(slide, x + Inches(0.2), Inches(5.85), Inches(3.45), Inches(0.5), PANNEAU_CLAIR)
        texte(slide, tables, x + Inches(0.35), Inches(5.95), Inches(3.15), Inches(0.35),
              10, couleur, True, police=POLICE_MONO, interligne=1.05)
        x = Emu(x + Inches(4.0))

    pied(slide, "Idempotence à tous les étages : comparaison d'ensembles de dates + overwritePartitions — un rejeu ne duplique rien.",
         "spark/jobs/ · docs/architecture.md §5")

    notes(slide, """
[07:30 — 08:00]  LE MÉDAILLON — CADRAGE AVANT L'ÉCRAN

« Avant de montrer le code, le contrat de chaque couche.

Bronze est la mémoire : copie fidèle, structures imbriquées conservées, et
surtout les rejets ne sont pas supprimés — ils sont marqués avec un motif. Un
rejet doit être analysable, pas invisible. Une seule entorse à la fidélité :
le champ password de /users est haché dès l'écriture. Aucun argument de
fidélité ne justifie de stocker un secret en clair.

Silver est la vérité métier : typé, aplati, dédoublonné, enrichi. Les prix sont
en DECIMAL, pas en DOUBLE — agrégés sur cinq mille lignes, les flottants
produisent des écarts au centime qui font échouer les réconciliations.

Gold répond aux questions : les agrégats sont matérialisés, l'outil de
restitution n'a plus qu'à lire. »
""")


def d09_historique_probleme(prs):
    slide = diapo(prs)
    bandeau_titre(slide, "Reconstituer 6 mois d'historique",
                  "La contrainte la plus intéressante du sujet — §4.1", ROUGE)

    rect(slide, MARGE, Inches(1.8), Inches(11.85), Inches(1.55), PANNEAU)
    rect(slide, MARGE, Inches(1.8), Inches(0.07), Inches(1.55), ROUGE, MSO_SHAPE.RECTANGLE)
    texte(slide, "LE PROBLÈME", MARGE + Inches(0.35), Inches(1.98), Inches(4.0),
          Inches(0.3), 16, ROUGE, True)
    texte(slide,
          "FakeStoreAPI ne renvoie que l'état courant : aucun champ de date sur /products ni /users, "
          "et seulement 7 paniers figés sur /carts.\n"
          "Ingérer 180 fois la même réponse produit un historique techniquement conforme, mais "
          "analytiquement mort — toutes les courbes seraient plates.",
          MARGE + Inches(0.35), Inches(2.38), Inches(11.2), Inches(0.85), 15, TEXTE_DOUX,
          interligne=1.3)

    rect(slide, MARGE, Inches(3.65), Inches(5.75), Inches(2.9), PANNEAU)
    texte(slide, "Deux réponses faciles, et pourquoi je les ai écartées",
          MARGE + Inches(0.3), Inches(3.85), Inches(5.2), Inches(0.3), 16, TEXTE, True)
    mauvaises = [
        ("Ingérer 180 fois à l'identique",
         "conforme à la lettre, sans aucune valeur analytique — aucune tendance à observer en Gold"),
        ("Générer des données de toutes pièces",
         "trahit la contrainte « données issues de FakeStoreAPI » et rend le résultat indéfendable"),
    ]
    y = Inches(4.3)
    for titre_m, detail in mauvaises:
        texte(slide, "✕  " + titre_m, MARGE + Inches(0.3), y, Inches(5.2), Inches(0.3),
              14, ROUGE, True)
        texte(slide, detail, MARGE + Inches(0.62), y + Inches(0.32), Inches(4.9),
              Inches(0.6), 12, TEXTE_DOUX, interligne=1.15)
        y = Emu(y + Inches(1.1))

    x2 = Inches(6.85)
    rect(slide, x2, Inches(3.65), Inches(5.75), Inches(2.9), PANNEAU)
    texte(slide, "Les deux propriétés que je me suis imposées",
          x2 + Inches(0.3), Inches(3.85), Inches(5.2), Inches(0.3), 16, VERT, True)
    bonnes = [
        ("Déterminisme absolu",
         "aucune fonction aléatoire — tout dérive de hash(clé | date | graine). Deux exécutions "
         "produisent le même historique, le pipeline reste idempotent."),
        ("Ancrage sur le réel",
         "la valeur renvoyée aujourd'hui est le POINT D'ARRIVÉE ; le modèle rétro-projette le passé. "
         "Sur la dernière date, le facteur vaut exactement 1,0."),
    ]
    y = Inches(4.3)
    for titre_b, detail in bonnes:
        texte(slide, "✓  " + titre_b, x2 + Inches(0.3), y, Inches(5.2), Inches(0.3),
              14, VERT, True)
        texte(slide, detail, x2 + Inches(0.62), y + Inches(0.32), Inches(4.9),
              Inches(0.75), 12, TEXTE_DOUX, interligne=1.15)
        y = Emu(y + Inches(1.1))

    notes(slide, """
[08:00 — 08:40]  HISTORISATION — POSER LE PROBLÈME

C'est le point où un jury attend une réponse honnête. Ne pas l'esquiver.

« La contrainte 4.1 demande environ six mois d'historique, alors que l'API ne
renvoie que l'état courant. Deux réponses faciles s'offraient à moi, et je les
ai écartées toutes les deux : ingérer 180 fois la même chose donne un
historique conforme mais mort ; générer des données de toutes pièces trahit la
contrainte de source.

Je me suis imposé deux propriétés. D'abord le déterminisme : aucune fonction
aléatoire, tout dérive d'un hash de la clé, de la date et d'une graine. Relancer
le backfill reproduit exactement le même historique. Ensuite l'ancrage sur le
réel : la valeur que l'API renvoie aujourd'hui est traitée comme le point
d'arrivée, et le modèle rétro-projette le passé. Sur la date la plus récente, le
facteur vaut exactement un : la donnée observée n'est jamais altérée. »
""")


def d10_historique_solution(prs):
    slide = diapo(prs)
    bandeau_titre(slide, "Trois niveaux, du plus réel au plus reconstruit",
                  "Le lakehouse ne ment jamais sur la nature d'une donnée", VERT)

    niveaux = [
        (VERT, "NIVEAU 1 — HISTORISATION RÉELLE", "toujours actif",
         "NiFi est déclenché une fois par date logique sur 180 jours. Chaque itération est un "
         "VRAI appel HTTP, horodaté, rangé dans sa propre partition ingest_date=.",
         "180 snapshots authentiques en Bronze — c'est ce niveau qui satisfait littéralement la contrainte."),
        (TURQUOISE, "NIVEAU 2 — HORODATAGE REDISTRIBUÉ", "toujours actif",
         "Les 7 paniers de /carts portent une vraie date, mais figée en 2019-2020. Elle est "
         "projetée de façon déterministe sur la fenêtre des 6 mois.",
         "Les enregistrements restent réels ; seule leur position dans le temps est recalculée."),
        (OR_C, "NIVEAU 3 — DÉRIVE SYNTHÉTIQUE", "désactivable",
         "Un modèle déterministe applique une dérive aux mesures : indice de prix (tendance + "
         "saisonnalité trimestrielle + bruit produit) et croissance du cumul d'avis.",
         "SYNTHETIC_HISTORY_ENABLED=false coupe ce niveau : le pipeline tourne, les courbes s'aplatissent."),
    ]

    y = Inches(1.72)
    for couleur, titre_n, statut, corps, consequence in niveaux:
        rect(slide, MARGE, y, Inches(11.85), Inches(1.32), PANNEAU)
        rect(slide, MARGE, y, Inches(0.07), Inches(1.32), couleur, MSO_SHAPE.RECTANGLE)
        texte(slide, titre_n, MARGE + Inches(0.32), y + Inches(0.13), Inches(6.0),
              Inches(0.3), 15, couleur, True)
        pastille = rect(slide, Inches(10.9), y + Inches(0.13), Inches(1.6), Inches(0.28),
                        PANNEAU_CLAIR)
        texte(slide, statut, Inches(10.9), y + Inches(0.17), Inches(1.6), Inches(0.22),
              10, couleur, True, align=PP_ALIGN.CENTER)
        texte(slide, corps, MARGE + Inches(0.32), y + Inches(0.48), Inches(11.2),
              Inches(0.45), 13, TEXTE, interligne=1.15)
        texte(slide, consequence, MARGE + Inches(0.32), y + Inches(0.98), Inches(11.2),
              Inches(0.3), 12, TEXTE_DOUX, italique=True)
        y = Emu(y + Inches(1.45))

    rect(slide, MARGE, Inches(6.1), Inches(11.85), Inches(0.85), PANNEAU_CLAIR)
    texte(slide, "La donnée porte toujours sa nature", MARGE + Inches(0.3), Inches(6.2),
          Inches(4.5), Inches(0.3), 14, TEXTE, True)
    texte(slide,
          "is_reconstructed  ·  unit_price_observed (mesure brute) à côté de unit_price (mesure historisée)  "
          "·  template_cart_id (le panier réel d'origine)",
          MARGE + Inches(0.3), Inches(6.53), Inches(11.2), Inches(0.3), 12, TURQUOISE,
          police=POLICE_MONO)

    notes(slide, """
[08:40 — 09:20]  HISTORISATION — LA RÉPONSE

« Ma réponse tient en trois niveaux, du plus réel au plus reconstruit.

Le niveau un est toujours actif : NiFi est déclenché une fois par date logique
sur cent quatre-vingts jours. Chaque itération est un vrai appel HTTP. La zone
brute contient donc cent quatre-vingts snapshots authentiques — c'est ce niveau
qui satisfait littéralement la contrainte du sujet.

Le niveau deux concerne les paniers : ils portent une vraie date, mais de 2019.
Je la projette de façon déterministe sur la fenêtre courante. Les
enregistrements restent réels, seule leur position dans le temps change.

Le niveau trois est le seul vraiment synthétique, et il est désactivable : un
modèle déterministe applique une dérive de prix et une croissance du cumul
d'avis, pour que les analyses temporelles aient du sens.

Et le point qui compte : toute ligne concernée porte un booléen
is_reconstructed, et la mesure brute reste disponible à côté de la mesure
historisée. Le lakehouse ne ment jamais sur la nature d'une donnée. »
""")


def d11_bascule_spark(prs):
    slide = diapo(prs)
    bandeau_ecran(slide, "Séquence 4 · 2 à 3 min — 4 pts")
    texte(slide, "Les jobs Spark du médaillon", MARGE, Inches(0.7), Inches(11.8),
          Inches(0.6), 32, TEXTE, True)
    texte(slide, "VS Code — spark/jobs/", MARGE, Inches(1.3), Inches(11.8),
          Inches(0.4), 15, BLEU)

    fichiers = [
        ("common/", "session · schemas · history · incremental · writer",
         "le socle : rien n'est dupliqué d'un job à l'autre", TURQUOISE),
        ("bronze/bronze_ingest.py", "un seul job, paramétré par --domain",
         "schéma explicite, PERMISSIVE, colonnes techniques, overwritePartitions", BRONZE_C),
        ("silver/silver_products.py", "dédoublonnage + typage + historisation",
         "montrer la fenêtre row_number() et le facteur de prix", ARGENT_C),
        ("silver/silver_orders.py", "génération du flux de commandes",
         "le job le plus délicat : réel vs reconstruit, traçabilité conservée", ARGENT_C),
        ("gold/gold_sales_daily.py", "JOINTURE INTER-DOMAINES",
         "la jointure est TEMPORELLE — c'est le point à expliquer", OR_C),
        ("maintenance/", "qualité · compaction · démo Iceberg",
         "7 contrôles dont la réconciliation croisée entre deux tables Gold", VIOLET),
    ]

    y = Inches(1.9)
    for chemin, resume, detail, couleur in fichiers:
        rect(slide, MARGE, y, Inches(11.85), Inches(0.66), PANNEAU)
        rect(slide, MARGE, y, Inches(0.07), Inches(0.66), couleur, MSO_SHAPE.RECTANGLE)
        texte(slide, chemin, MARGE + Inches(0.3), y + Inches(0.07), Inches(3.2),
              Inches(0.26), 12.5, couleur, True, police=POLICE_MONO)
        texte(slide, resume, MARGE + Inches(0.3), y + Inches(0.35), Inches(3.4),
              Inches(0.26), 10.5, TEXTE_DOUX)
        texte(slide, detail, MARGE + Inches(4.0), y + Inches(0.19), Inches(7.6),
              Inches(0.32), 12.5, TEXTE)
        y = Emu(y + Inches(0.72))

    rect(slide, MARGE, Inches(6.35), Inches(11.85), Inches(0.66), PANNEAU_CLAIR)
    texte(slide, "LE POINT À NE PAS MANQUER", MARGE + Inches(0.3), Inches(6.44),
          Inches(3.5), Inches(0.26), 12.5, OR_C, True)
    texte(slide, "p.snapshot_date = o.order_date  —  chaque ligne de commande est valorisée "
                 "au prix EN VIGUEUR LE JOUR DE LA COMMANDE, pas au prix d'aujourd'hui.",
          MARGE + Inches(0.3), Inches(6.72), Inches(11.2), Inches(0.28), 12, TEXTE,
          police=POLICE_MONO)

    notes(slide, """
[09:20 — 11:00]  JOBS SPARK — À L'ÉCRAN

Ne PAS lire le code ligne à ligne. Ouvrir trois fichiers et expliquer une idée
dans chacun.

1) bronze/bronze_ingest.py
   « Un seul job pour les trois domaines, paramétré par --domain. Deux choses :
   le schéma est EXPLICITE, jamais inféré — une inférence changerait toute
   seule le type d'une colonne le jour où l'API renvoie un entier au lieu d'un
   décimal, et casserait Silver en silence. Et l'écriture se fait en
   overwritePartitions : retraiter une date remplace exactement sa partition,
   donc rejouer un DAG run ne duplique rien. »

2) silver/silver_products.py
   Montrer le row_number() de dédoublonnage, puis le facteur de prix.
   « Ici on voit l'historisation à l'œuvre : unit_price_observed garde la
   mesure brute, unit_price porte la mesure historisée. »

3) gold/gold_sales_daily.py — LE POINT FORT
   Montrer la clause de jointure.
   « La jointure entre les commandes et les produits est TEMPORELLE :
   p.snapshot_date = o.order_date. Valoriser une commande de mars avec le prix
   du catalogue d'aujourd'hui serait une erreur classique. Et c'est un LEFT
   JOIN volontairement : une ligne sans produit correspondant n'est pas perdue,
   elle est comptée dans nb_unmatched_lines, un indicateur d'intégrité
   référentielle que la porte qualité vérifie. »

Si le temps le permet : montrer la réconciliation croisée dans
data_quality_checks.py — deux tables Gold construites par deux jobs
indépendants dont le total de CA doit coïncider.
""")


def d12_bascule_dremio(prs):
    slide = diapo(prs)
    bandeau_ecran(slide, "Séquence 5 · 2 à 3 min — 3 pts")
    texte(slide, "Test du moteur de requête Dremio", MARGE, Inches(0.7), Inches(11.8),
          Inches(0.6), 32, TEXTE, True)
    texte(slide, "http://localhost:9047 — source « lakehouse » (Nessie)", MARGE,
          Inches(1.3), Inches(11.8), Inches(0.4), 15, VIOLET)

    lignes = [
        ("Q0", "Inventaire du catalogue", "les 3 couches visibles sans déclaration manuelle", TURQUOISE),
        ("Q1", "BRONZE — raw_products", "structure brute + traçabilité jusqu'à l'objet MinIO", BRONZE_C),
        ("Q2", "SILVER — dim_products", "typé, aplati, enrichi — mesure brute conservée", ARGENT_C),
        ("Q3", "GOLD — gold_catalog_daily_kpi", "30 jours d'indicateurs, aucun calcul dans la requête", OR_C),
        ("Q4", "JOINTURE — commandes × produits × clients", "exigence 5.3 — jointure temporelle", VIOLET),
        ("Q5", "JOINTURE + agrégation par ville", "quelles villes achètent quelles catégories", VIOLET),
        ("Q6", "AGRÉGATION — CA mensuel par catégorie", "exigence 5.4 — les 6 mois d'historique", VERT),
        ("Q7", "AGRÉGATION — segmentation RFM", "seconde table Gold à jointure inter-domaines", VERT),
        ("Q8", "BONUS — tendance tarifaire", "impossible sans les 180 snapshots", ORANGE),
    ]

    y = Inches(1.85)
    for code, titre_q, detail, couleur in lignes:
        rect(slide, MARGE, y, Inches(11.85), Inches(0.46), PANNEAU)
        texte(slide, code, MARGE + Inches(0.22), y + Inches(0.1), Inches(0.55),
              Inches(0.26), 12.5, couleur, True, police=POLICE_MONO)
        texte(slide, titre_q, MARGE + Inches(0.95), y + Inches(0.09), Inches(4.6),
              Inches(0.28), 13.5, TEXTE)
        texte(slide, detail, MARGE + Inches(5.8), y + Inches(0.11), Inches(5.8),
              Inches(0.26), 11.5, TEXTE_DOUX)
        y = Emu(y + Inches(0.5))

    rect(slide, MARGE, Inches(6.45), Inches(11.85), Inches(0.56), PANNEAU_CLAIR)
    texte(slide, "make dremio-test", MARGE + Inches(0.3), Inches(6.58), Inches(2.5),
          Inches(0.28), 14, VERT, True, police=POLICE_MONO)
    texte(slide, "joue les 10 requêtes, affiche les résultats et rend un verdict — "
                 "la preuve est industrialisée, pas improvisée",
          MARGE + Inches(2.9), Inches(6.6), Inches(8.7), Inches(0.28), 12.5, TEXTE_DOUX)

    notes(slide, """
[11:00 — 13:00]  DREMIO — À L'ÉCRAN

Deux façons de procéder ; choisir selon le temps restant.

OPTION RAPIDE (recommandée) : lancer `make dremio-test` dans le terminal. Les
dix requêtes défilent avec leurs résultats. Commenter au fil de l'eau.

OPTION DÉTAILLÉE : ouvrir l'éditeur SQL de Dremio et jouer Q1, Q2, Q3, Q4, Q6
en copier-coller depuis dremio/sql/validation_dremio.sql.

Dans les deux cas, MONTRER D'ABORD l'arborescence de la source dans Dremio :
lakehouse > bronze / silver / gold. « Ces tables ne sont déclarées nulle part
dans Dremio : c'est Nessie qui les décrit, Dremio ne fait que lire le catalogue. »

Sur Q1 : « en Bronze, la colonne rating est encore un STRUCT, et chaque ligne
sait de quel objet MinIO elle provient et à quel instant réel l'appel a eu
lieu. »

Sur Q2 : « en Silver, plus aucune structure imbriquée, un prix décimal, et la
mesure brute conservée à côté de la mesure historisée. »

Sur Q4, LA JOINTURE : « trois domaines réunis — commandes, produits, clients.
Et la jointure sur les produits est temporelle. »

Sur Q6, L'AGRÉGATION : « six mois agrégés par mois et par catégorie. C'est la
démonstration directe que la contrainte d'historisation produit de la valeur. »

Si le temps le permet, montrer une vue de l'espace analytics.
""")


def d13_bascule_bout_en_bout(prs):
    slide = diapo(prs)
    bandeau_ecran(slide, "Séquence 6 · 2 à 3 min — 3 pts")
    texte(slide, "Démonstration de bout en bout", MARGE, Inches(0.7), Inches(11.8),
          Inches(0.6), 32, TEXTE, True)
    texte(slide, "Un appel API par NiFi jusqu'à une table Gold interrogeable dans Dremio",
          MARGE, Inches(1.3), Inches(11.8), Inches(0.4), 15, VERT)

    etapes = [
        ("1", "Airflow", "déclencher fakestore_daily_ingestion — montrer le graphe du DAG", BLEU),
        ("2", "NiFi", "les compteurs des processeurs s'incrémentent en direct", ORANGE),
        ("3", "MinIO", "rafraîchir : le nouvel objet JSON apparaît dans ingest_date=<aujourd'hui>", TURQUOISE),
        ("4", "Airflow", "lakehouse_medallion démarre automatiquement (TriggerDagRunOperator)", BLEU),
        ("5", "Spark", "l'interface du master montre l'application en cours d'exécution", BLEU),
        ("6", "Airflow", "les tâches passent au vert : bronze → silver → gold → contrôles qualité", BLEU),
        ("7", "Dremio", "rejouer Q3 ou Q6 : la nouvelle date est là", VIOLET),
    ]

    y = Inches(1.95)
    for numero, service, action, couleur in etapes:
        rect(slide, MARGE, y, Inches(11.85), Inches(0.6), PANNEAU)
        cercle = rect(slide, MARGE + Inches(0.18), y + Inches(0.13), Inches(0.34),
                      Inches(0.34), couleur, MSO_SHAPE.OVAL)
        texte(slide, numero, MARGE + Inches(0.18), y + Inches(0.18), Inches(0.34),
              Inches(0.25), 13, TEXTE_SOMBRE, True, align=PP_ALIGN.CENTER)
        texte(slide, service, MARGE + Inches(0.72), y + Inches(0.16), Inches(1.5),
              Inches(0.3), 15, couleur, True)
        texte(slide, action, MARGE + Inches(2.35), y + Inches(0.18), Inches(9.2),
              Inches(0.3), 13, TEXTE_DOUX)
        y = Emu(y + Inches(0.7))

    rect(slide, MARGE, Inches(6.85), Inches(11.85), Inches(0.02), PANNEAU_CLAIR,
         MSO_SHAPE.RECTANGLE)
    texte(slide, "Filet de sécurité : si le temps manque, déclencher l'ingestion au curl "
                 "et montrer directement MinIO puis Dremio.",
          MARGE, Inches(6.98), Inches(11.8), Inches(0.3), 12, TEXTE_DOUX, italique=True)

    notes(slide, """
[13:00 — 14:15]  BOUT EN BOUT — LA SÉQUENCE QUI PROUVE TOUT

C'est la séquence que le jury attend le plus. Elle doit être RÉPÉTÉE avant
l'enregistrement, chronomètre en main.

Disposer les onglets à l'avance : Airflow, NiFi, MinIO, Dremio.

« Je déclenche maintenant le DAG d'ingestion quotidienne. »
   → Airflow, bouton Trigger. Montrer le graphe : vérifier NiFi, déclencher,
     attendre le dépôt, vérifier la volumétrie, déclencher le médaillon.

« Airflow ne récupère pas les données lui-même : il demande à NiFi de le faire.
NiFi reste l'unique point d'ingestion. »
   → basculer sur NiFi, montrer les compteurs qui bougent.

« Et voici le dépôt. »
   → MinIO, rafraîchir sur ingest_date d'aujourd'hui.

« La tâche attendre_depot ne suppose pas que NiFi a terminé : elle OBSERVE la
zone brute jusqu'à constater le dépôt. C'est l'option A. »

   → retour Airflow, le médaillon a démarré tout seul.
   → Spark master : l'application tourne.
   → tâches au vert jusqu'aux contrôles qualité.

« Et la donnée est immédiatement interrogeable. »
   → Dremio, rejouer Q3, montrer la date du jour.

SI ÇA COINCE : ne pas s'acharner. Passer au filet de sécurité (curl direct),
ou dire « le run précédent est visible ici » et montrer le résultat. Un incident
géré calmement est mieux perçu qu'un silence de trente secondes.
""")


def d14_bonus(prs):
    slide = diapo(prs)
    bandeau_titre(slide, "Bonus réalisés", "Les cinq propositions de la Partie 7", ORANGE)

    bonus = [
        (ORANGE, "Supervision NiFi",
         "PrometheusReportingTask créée par script, panneau « débit d'ingestion » dans "
         "le dashboard Grafana provisionné automatiquement."),
        (TURQUOISE, "Idempotence",
         "DetectDuplicate + DistributedMapCache côté NiFi (clé domaine::date), et "
         "overwritePartitions côté Spark. Double filet."),
        (BLEU, "Troisième domaine — commandes",
         "/carts exploité en fct_order_items, puis en gold_sales_by_category_daily "
         "et gold_customer_360."),
        (VIOLET, "Process Group paramétrable",
         "Parameter Context « fakestore-ingestion » : changer #{api.base.url} suffit "
         "à brancher le flow sur une autre API."),
        (OR_C, "Iceberg avancé",
         "demo_iceberg_features.py : time travel (VERSION AS OF / TIMESTAMP AS OF), "
         "schema evolution sans réécriture, et branches Nessie."),
    ]

    y = Inches(1.8)
    for couleur, titre_b, corps in bonus:
        rect(slide, MARGE, y, Inches(11.85), Inches(0.88), PANNEAU)
        rect(slide, MARGE, y, Inches(0.07), Inches(0.88), couleur, MSO_SHAPE.RECTANGLE)
        texte(slide, titre_b, MARGE + Inches(0.32), y + Inches(0.14), Inches(3.4),
              Inches(0.3), 15, couleur, True)
        texte(slide, corps, MARGE + Inches(3.9), y + Inches(0.12), Inches(7.7),
              Inches(0.7), 13, TEXTE_DOUX, interligne=1.18)
        y = Emu(y + Inches(1.0))

    rect(slide, MARGE, Inches(6.85), Inches(11.85), Inches(0.02), PANNEAU_CLAIR,
         MSO_SHAPE.RECTANGLE)
    texte(slide, "make demo-iceberg  —  joue les quatre démonstrations Iceberg / Nessie en une commande",
          MARGE, Inches(6.98), Inches(11.8), Inches(0.3), 12, VERT, police=POLICE_MONO)

    notes(slide, """
[14:15 — 14:35]  BONUS

Passer vite, en citant les cinq. Si le temps le permet (et seulement dans ce
cas), basculer sur le terminal et lancer `make demo-iceberg` pour montrer le
time travel : deux snapshots comparés, puis une branche Nessie créée, modifiée,
et main resté intact.

« Les cinq bonus proposés sont couverts. Le plus intéressant est le dernier :
Nessie apporte un vrai versionnement de branche, comme git. On peut recalculer
un agrégat sur une branche, le contrôler, et ne fusionner que si le résultat
convient. »
""")


def d15_difficultes(prs):
    slide = diapo(prs)
    bandeau_titre(slide, "Difficultés rencontrées", "Et comment je les ai traitées", ROUGE)

    y = tableau(
        slide, MARGE, Inches(1.75), Inches(11.85),
        ["Difficulté", "Résolution"],
        [
            ("NiFi 1.28 impose HTTPS + utilisateur généré",
             ("Bascule HTTP par NIFI_WEB_HTTP_PORT — justifiée ; procédure docker logs documentée", TEXTE_DOUX)),
            ("localhost dans l'endpoint S3 du flow",
             ("Nom de service Docker ; contrôle automatisé dans verifier_projet.py", TEXTE_DOUX)),
            ("MinIO refuse le virtual-hosted style S3",
             ("path-style access activé côté NiFi, Spark ET Dremio — les trois", TEXTE_DOUX)),
            ("Deux chemins d'accès à S3 dans Spark",
             ("s3a pour les JSON bruts, S3FileIO pour Iceberg — configurés séparément", TEXTE_DOUX)),
            ("Compatibilité Spark / Iceberg / Nessie",
             ("Matrice figée, JARs embarqués dans l'image plutôt que --packages", TEXTE_DOUX)),
            ("Driver Spark côté Airflow",
             ("Image Airflow custom : JRE 17 + distribution Spark identique au cluster", TEXTE_DOUX)),
            ("Historique plat sur une API sans dimension temporelle",
             ("Modèle d'historisation déterministe à trois niveaux", TEXTE_DOUX)),
            ("Multiplication de petits fichiers Parquet",
             ("Partition mensuelle en Silver + job de compaction Iceberg", TEXTE_DOUX)),
            ("180 DAG runs illisibles en démonstration",
             ("Boucle unique avec reprise sur les dates déjà ingérées", TEXTE_DOUX)),
        ],
        [38, 62],
        taille=12.5,
        hauteur_ligne=Inches(0.44),
    )

    notes(slide, """
[14:35 — 14:50]  DIFFICULTÉS

Ne PAS lire le tableau. En développer deux, celles qui montrent le mieux la
compréhension :

« Deux difficultés m'ont vraiment occupé. La première : dans Spark, il y a deux
chemins d'accès à S3 qui n'ont rien à voir. Lire les JSON bruts passe par le
connecteur s3a de Hadoop ; écrire les tables Iceberg passe par S3FileIO, qui
utilise le SDK AWS v2. Les deux doivent être configurés séparément — tant que
je n'avais configuré que l'un, la moitié du pipeline échouait sans message
clair.

La seconde : le driver Spark tourne dans le conteneur Airflow, en mode client.
Il fallait donc y installer non seulement un JRE, mais la même version de Spark
et les mêmes JARs que le cluster — le protocole RPC entre master et worker n'est
pas garanti compatible entre versions mineures. »
""")


def d16_bilan(prs):
    slide = diapo(prs)
    bandeau_titre(slide, "Bilan", "Ce que la plateforme démontre", VERT)

    chiffres = [
        ("11", "services\nconteneurisés", ORANGE),
        ("12", "processeurs\nNiFi", ORANGE),
        ("10", "tables\nIceberg", TURQUOISE),
        ("3", "domaines\nfonctionnels", BLEU),
        ("180", "snapshots\nquotidiens", OR_C),
        ("10", "requêtes de\nvalidation", VIOLET),
    ]
    x = MARGE
    for valeur, libelle, couleur in chiffres:
        rect(slide, x, Inches(1.75), Inches(1.85), Inches(1.5), PANNEAU)
        texte(slide, valeur, x, Inches(1.95), Inches(1.85), Inches(0.55), 34, couleur,
              True, align=PP_ALIGN.CENTER)
        texte(slide, libelle, x, Inches(2.55), Inches(1.85), Inches(0.6), 12, TEXTE_DOUX,
              align=PP_ALIGN.CENTER, interligne=1.1)
        x = Emu(x + Inches(2.0))

    rect(slide, MARGE, Inches(3.5), Inches(5.75), Inches(3.05), PANNEAU)
    texte(slide, "Ce dont je suis satisfait", MARGE + Inches(0.3), Inches(3.68),
          Inches(5.2), Inches(0.3), 17, VERT, True)
    forces = [
        "l'idempotence est réelle à tous les étages, pas seulement affichée",
        "la donnée porte toujours sa nature : observée ou reconstruite",
        "aucune perte silencieuse — rejets marqués en Bronze, file de rebut en NiFi",
        "la plateforme se reconstruit entièrement par script, sans un clic",
    ]
    y = Inches(4.12)
    for ligne in forces:
        puce_carree(slide, MARGE + Inches(0.32), y + Inches(0.08), VERT, Inches(0.09))
        texte(slide, ligne, MARGE + Inches(0.55), y, Inches(4.95), Inches(0.5), 13,
              TEXTE_DOUX, interligne=1.15)
        y = Emu(y + Inches(0.58))

    x2 = Inches(6.85)
    rect(slide, x2, Inches(3.5), Inches(5.75), Inches(3.05), PANNEAU)
    texte(slide, "Ce que je ferais différemment à l'échelle", x2 + Inches(0.3),
          Inches(3.68), Inches(5.2), Inches(0.3), 17, OR_C, True)
    limites = [
        "Gold recalculé intégralement → MERGE incrémental au-delà de quelques millions de lignes",
        "un seul PostgreSQL → séparer Airflow et Nessie (profils de charge distincts)",
        "NiFi en HTTP → HTTPS avec certificat interne et jetons d'accès",
        "Spark en mode client depuis Airflow → cluster mode ou Kubernetes",
    ]
    y = Inches(4.12)
    for ligne in limites:
        puce_carree(slide, x2 + Inches(0.32), y + Inches(0.08), OR_C, Inches(0.09))
        texte(slide, ligne, x2 + Inches(0.55), y, Inches(4.95), Inches(0.5), 13,
              TEXTE_DOUX, interligne=1.15)
        y = Emu(y + Inches(0.58))

    notes(slide, """
[14:50 — 15:10]  BILAN ET CLÔTURE

« Pour conclure : onze services, douze processeurs NiFi, dix tables Iceberg,
trois domaines fonctionnels, cent quatre-vingts snapshots quotidiens.

Ce dont je suis le plus satisfait, c'est que l'idempotence soit réelle à tous
les étages, et que la donnée porte toujours sa nature — on sait toujours si une
mesure est observée ou reconstruite.

Ce que je ferais différemment à plus grande échelle : Gold est recalculé
intégralement, ce qui ne tiendrait pas au-delà de quelques millions de lignes ;
et j'ai fait des compromis assumés sur la sécurité de NiFi, adaptés à un
environnement local mais pas à une production.

Merci de votre attention. »

ARRÊTER L'ENREGISTREMENT ICI. Viser 15 min ± 1 min : le respect du format fait
partie de la notation.
""")


def d17_annexe(prs):
    slide = diapo(prs)
    bandeau_titre(slide, "Annexe — matrice de versions et accès",
                  "Diapositive de secours, à ne montrer que si une question l'appelle",
                  TEXTE_DOUX)

    tableau(
        slide, MARGE, Inches(1.7), Inches(5.75),
        ["Composant", "Version"],
        [
            ("Apache NiFi", ("1.28.1", TEXTE, POLICE_MONO)),
            ("Apache Spark", ("3.5.1 (Scala 2.12, Java 17)", TEXTE, POLICE_MONO)),
            ("Apache Iceberg", ("1.5.2", TEXTE, POLICE_MONO)),
            ("Project Nessie", ("0.77.1 — API v2", TEXTE, POLICE_MONO)),
            ("Apache Airflow", ("2.9.3 — LocalExecutor", TEXTE, POLICE_MONO)),
            ("Dremio OSS", ("25.0", TEXTE, POLICE_MONO)),
            ("MinIO", ("RELEASE.2024-09-13", TEXTE, POLICE_MONO)),
            ("PostgreSQL", ("15", TEXTE, POLICE_MONO)),
            ("Prometheus / Grafana", ("2.54.1 / 11.2.0", TEXTE, POLICE_MONO)),
        ],
        [48, 52],
        taille=12.5,
        hauteur_ligne=Inches(0.42),
    )

    x2 = Inches(6.85)
    tableau(
        slide, x2, Inches(1.7), Inches(5.75),
        ["Interface", "Adresse et identifiants"],
        [
            ("MinIO console", ("localhost:9001 — lakehouse / lakehouse123", TEXTE, POLICE_MONO)),
            ("Apache NiFi", ("localhost:8080/nifi", TEXTE, POLICE_MONO)),
            ("Apache Airflow", ("localhost:8085 — admin / admin", TEXTE, POLICE_MONO)),
            ("Spark master", ("localhost:8090", TEXTE, POLICE_MONO)),
            ("Dremio", ("localhost:9047 — dremio / dremio123", TEXTE, POLICE_MONO)),
            ("Nessie API", ("localhost:19120/api/v2", TEXTE, POLICE_MONO)),
            ("Prometheus", ("localhost:9090", TEXTE, POLICE_MONO)),
            ("Grafana", ("localhost:3001 — admin / admin", TEXTE, POLICE_MONO)),
            ("Ingestion (curl)", ("POST localhost:9095/ingest", TEXTE, POLICE_MONO)),
        ],
        [32, 68],
        taille=12,
        hauteur_ligne=Inches(0.42),
    )

    rect(slide, MARGE, Inches(6.2), Inches(11.85), Inches(0.75), PANNEAU_CLAIR)
    texte(slide, "Remise en route complète depuis zéro", MARGE + Inches(0.3),
          Inches(6.32), Inches(4.5), Inches(0.28), 13, TEXTE, True)
    texte(slide, "make demarrage-complet      (build → up → flow NiFi → source Dremio → backfill 6 mois)",
          MARGE + Inches(0.3), Inches(6.62), Inches(11.2), Inches(0.28), 13, VERT,
          police=POLICE_MONO)

    notes(slide, """
DIAPOSITIVE DE SECOURS — ne pas la montrer dans le déroulé normal.

À utiliser si l'enseignant pose une question sur les versions, la compatibilité
entre Iceberg et Nessie, ou l'accès aux interfaces.

Point de compatibilité à connaître par cœur :
  Spark 3.5.1 → iceberg-spark-runtime-3.5_2.12:1.5.2
              → nessie-spark-extensions-3.5_2.12:0.77.1
              → hadoop-aws 3.3.4 + aws-java-sdk-bundle 1.12.262
Les JARs sont embarqués dans l'image, pas résolus par --packages : le pipeline
est reproductible et fonctionne sans accès Internet.
""")


# --------------------------------------------------------------------------- #
def main() -> int:
    prs = nouvelle_presentation()

    for constructeur in (
        d01_titre,
        d02_plan,
        d03_cahier_des_charges,
        d04_architecture,
        d05_choix,
        d06_bascule_compose,
        d07_bascule_nifi,
        d08_medaillon,
        d09_historique_probleme,
        d10_historique_solution,
        d11_bascule_spark,
        d12_bascule_dremio,
        d13_bascule_bout_en_bout,
        d14_bonus,
        d15_difficultes,
        d16_bilan,
        d17_annexe,
    ):
        constructeur(prs)

    SORTIE.parent.mkdir(parents=True, exist_ok=True)
    prs.save(SORTIE)

    print(f"Support généré : {SORTIE}")
    print(f"  {len(prs.slides.__iter__.__self__._sldIdLst)} diapositives")
    print("  Notes de présentateur : minutage + texte + éléments à montrer à l'écran")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
