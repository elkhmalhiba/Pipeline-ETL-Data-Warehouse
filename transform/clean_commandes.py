# transform/clean_commandes.py — Nettoyage des commandes Mexora

import logging
import pandas as pd

logger = logging.getLogger("mexora_etl")

# Mapping statuts non-standards → valeurs officielles
MAPPING_STATUTS = {
    "livré":    "livré",  "livre":   "livré",  "LIVRE":   "livré",  "DONE": "livré",
    "annulé":   "annulé", "annule":  "annulé", "KO":      "annulé",
    "en_cours": "en_cours","OK":     "en_cours","expédié": "en_cours",
    "retourné": "retourné","retourne":"retourné",
}

STATUTS_VALIDES = {"livré", "annulé", "en_cours", "retourné"}


def charger_mapping_villes(df_regions: pd.DataFrame) -> dict:
    """
    Construit un dictionnaire de mapping depuis le référentiel régions.
    Clé = variante en minuscule, valeur = nom standard officiel.
    """
    mapping = {}
    for _, row in df_regions.iterrows():
        nom_std = row["nom_ville_standard"].strip()
        # Variantes connues pour chaque ville
        variantes = [
            nom_std.lower(),
            nom_std.upper(),
            nom_std,
            row["code_ville"].lower(),
            row["code_ville"].upper(),
            row["code_ville"],
        ]
        for v in variantes:
            mapping[v.strip()] = nom_std

    # Variantes manuelles supplémentaires
    extras = {
        "tnja": "Tanger", "tng": "Tanger",
        "casa": "Casablanca", "casablanca ": "Casablanca",
        "fès": "Fès", "fes": "Fès",
        "mrakech": "Marrakech",
        "meknes": "Meknès", "meknes": "Meknès",
    }
    mapping.update(extras)
    return mapping


def transform_commandes(df: pd.DataFrame, df_regions: pd.DataFrame) -> pd.DataFrame:
    """
    Applique toutes les règles de nettoyage sur les commandes Mexora.

    Règles :
      R1 - Suppression des doublons sur id_commande (garder dernière occurrence)
      R2 - Standardisation des dates (format cible : YYYY-MM-DD)
      R3 - Harmonisation des noms de villes via référentiel régions_maroc
      R4 - Standardisation des statuts de commande
      R5 - Suppression des lignes avec quantite <= 0
      R6 - Suppression des lignes avec prix_unitaire = 0 (commandes test)
      R7 - Remplacement des id_livreur manquants par '-1' (livreur inconnu)
    """
    nb_initial = len(df)
    logger.info(f"[TRANSFORM commandes] Début : {nb_initial} lignes")

    # ── R1 : Suppression des doublons ──────────────────────────
    avant = len(df)
    df = df.drop_duplicates(subset=["id_commande"], keep="last")
    logger.info(f"[R1 doublons] {avant - len(df)} lignes supprimées → {len(df)} restantes")

    # ── R2 : Standardisation des dates ─────────────────────────
    avant = len(df)
    for col in ["date_commande", "date_livraison"]:
        df[col] = pd.to_datetime(
            df[col], format="mixed", dayfirst=True, errors="coerce"
        )
    nb_dates_invalides = df["date_commande"].isna().sum()
    df = df.dropna(subset=["date_commande"])
    df["date_commande"]  = df["date_commande"].dt.strftime("%Y-%m-%d")
    df["date_livraison"] = df["date_livraison"].dt.strftime("%Y-%m-%d")
    logger.info(f"[R2 dates] {nb_dates_invalides} dates invalides supprimées → {len(df)} restantes")

    # ── R3 : Harmonisation des villes ──────────────────────────
    mapping_villes = charger_mapping_villes(df_regions)
    df["ville_livraison"] = (
        df["ville_livraison"]
        .str.strip()
        .str.lower()
        .map(mapping_villes)
        .fillna("Non renseignée")
    )
    nb_non_reconnues = (df["ville_livraison"] == "Non renseignée").sum()
    logger.info(f"[R3 villes] {nb_non_reconnues} villes non reconnues → 'Non renseignée'")

    # ── R4 : Standardisation des statuts ───────────────────────
    df["statut"] = df["statut"].str.strip().replace(MAPPING_STATUTS)
    invalides = ~df["statut"].isin(STATUTS_VALIDES)
    nb_invalides = invalides.sum()
    df.loc[invalides, "statut"] = "inconnu"
    logger.warning(f"[R4 statuts] {nb_invalides} valeurs non reconnues → 'inconnu'")

    # ── R5 : Quantités invalides ────────────────────────────────
    avant = len(df)
    df["quantite"] = pd.to_numeric(df["quantite"], errors="coerce")
    df = df[df["quantite"] > 0]
    logger.info(f"[R5 quantites] {avant - len(df)} lignes supprimées (quantite <= 0)")

    # ── R6 : Prix nuls (commandes test) ────────────────────────
    avant = len(df)
    df["prix_unitaire"] = pd.to_numeric(df["prix_unitaire"], errors="coerce")
    df = df[df["prix_unitaire"] > 0]
    logger.info(f"[R6 prix] {avant - len(df)} commandes test supprimées (prix = 0)")

    # ── R7 : Livreurs manquants ─────────────────────────────────
    nb_manquants = df["id_livreur"].isna().sum()
    df["id_livreur"] = df["id_livreur"].fillna("-1")
    logger.info(f"[R7 livreurs] {nb_manquants} valeurs manquantes → '-1' (livreur inconnu)")

    # ── Calcul montant_ttc ──────────────────────────────────────
    df["montant_ttc"] = (df["quantite"] * df["prix_unitaire"]).round(2)

    nb_final = len(df)
    logger.info(
        f"[TRANSFORM commandes] Terminé : {nb_initial} → {nb_final} lignes "
        f"({nb_initial - nb_final} supprimées au total)"
    )
    return df.reset_index(drop=True)