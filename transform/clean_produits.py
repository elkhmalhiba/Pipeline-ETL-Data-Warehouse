# transform/clean_produits.py — Nettoyage des produits Mexora

import logging
import pandas as pd

logger = logging.getLogger("mexora_etl")

# Normalisation des catégories (casse incohérente dans la source)
MAPPING_CATEGORIES = {
    "electronique":  "Electronique",
    "ELECTRONIQUE":  "Electronique",
    "électronique":  "Electronique",
    "Electronique":  "Electronique",
    "mode":          "Mode",
    "MODE":          "Mode",
    "fashion":       "Mode",
    "Fashion":       "Mode",
    "alimentation":  "Alimentation",
    "ALIMENTATION":  "Alimentation",
    "food":          "Alimentation",
    "Food":          "Alimentation",
}


def transform_produits(df: pd.DataFrame) -> pd.DataFrame:
    """
    Règles de nettoyage sur les produits Mexora :
      R1 - Normalisation de la casse des catégories
      R2 - Remplacement des prix_catalogue NULL par la médiane de la catégorie
      R3 - Standardisation du champ actif en booléen
      R4 - Normalisation des noms de colonnes
    """
    nb_initial = len(df)
    logger.info(f"[TRANSFORM produits] Début : {nb_initial} lignes")

    # ── R1 : Normalisation catégories ──────────────────────────
    df["categorie"] = df["categorie"].replace(MAPPING_CATEGORIES)
    categories_inconnues = ~df["categorie"].isin(["Electronique", "Mode", "Alimentation"])
    nb_inconnues = categories_inconnues.sum()
    if nb_inconnues > 0:
        logger.warning(f"[R1 categories] {nb_inconnues} catégories non reconnues → 'Autre'")
        df.loc[categories_inconnues, "categorie"] = "Autre"
    logger.info(f"[R1 categories] Normalisation terminée")

    # ── R2 : Prix catalogue NULL → médiane par catégorie ───────
    df["prix_catalogue"] = pd.to_numeric(df["prix_catalogue"], errors="coerce")
    nb_prix_null = df["prix_catalogue"].isna().sum()
    if nb_prix_null > 0:
        mediane_par_cat = df.groupby("categorie")["prix_catalogue"].transform("median")
        df["prix_catalogue"] = df["prix_catalogue"].fillna(mediane_par_cat)
        logger.info(f"[R2 prix] {nb_prix_null} prix NULL remplacés par médiane de la catégorie")

    # ── R3 : Standardisation booléen actif ─────────────────────
    if df["actif"].dtype == object:
        df["actif"] = df["actif"].map(
            {"true": True, "false": False, "True": True, "False": False}
        ).fillna(True)
    df["actif"] = df["actif"].astype(bool)

    # ── R4 : Renommage pour cohérence avec le DWH ──────────────
    df = df.rename(columns={
        "nom":           "nom_produit",
        "prix_catalogue":"prix_standard",
    })

    nb_final = len(df)
    logger.info(
        f"[TRANSFORM produits] Terminé : {nb_initial} → {nb_final} lignes"
    )
    return df.reset_index(drop=True)