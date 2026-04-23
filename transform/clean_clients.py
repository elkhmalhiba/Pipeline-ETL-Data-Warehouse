# transform/clean_clients.py — Nettoyage des clients Mexora

import re
import logging
import pandas as pd
from datetime import date

logger = logging.getLogger("mexora_etl")

PATTERN_EMAIL = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

MAPPING_SEXE = {
    "m": "m", "f": "f",
    "1": "m", "0": "f",
    "homme": "m", "femme": "f",
    "male": "m", "female": "f",
    "h": "m",
}


def transform_clients(df: pd.DataFrame, df_regions: pd.DataFrame) -> pd.DataFrame:
    """
    Règles de nettoyage sur les clients Mexora :
      R1 - Déduplication sur email normalisé (garder inscription la plus récente)
      R2 - Standardisation du sexe → 'm' / 'f' / 'inconnu'
      R3 - Validation des dates de naissance (âge entre 16 et 100 ans)
      R4 - Validation du format email
      R5 - Harmonisation des villes (même référentiel que commandes)
      R6 - Calcul de la tranche d'âge
    """
    nb_initial = len(df)
    logger.info(f"[TRANSFORM clients] Début : {nb_initial} lignes")

    # ── R1 : Déduplication sur email ───────────────────────────
    avant = len(df)
    df["email_norm"] = df["email"].str.lower().str.strip()
    df["date_inscription"] = pd.to_datetime(df["date_inscription"], errors="coerce")
    df = df.sort_values("date_inscription").drop_duplicates(
        subset=["email_norm"], keep="last"
    )
    logger.info(f"[R1 doublons clients] {avant - len(df)} doublons supprimés → {len(df)} restants")

    # ── R2 : Standardisation du sexe ───────────────────────────
    df["sexe"] = (
        df["sexe"].str.lower().str.strip()
        .map(MAPPING_SEXE)
        .fillna("inconnu")
    )
    logger.info(f"[R2 sexe] Standardisation terminée")

    # ── R3 : Validation dates de naissance ─────────────────────
    df["date_naissance"] = pd.to_datetime(df["date_naissance"], errors="coerce")
    today = pd.Timestamp(date.today())
    df["age"] = ((today - df["date_naissance"]).dt.days / 365).fillna(-1).astype(float)
    masque_invalide = (df["age"] < 16) | (df["age"] > 100) | df["age"].isna()
    nb_invalides = masque_invalide.sum()
    df.loc[masque_invalide, "date_naissance"] = pd.NaT
    df.loc[masque_invalide, "age"] = pd.NA
    logger.info(f"[R3 naissance] {nb_invalides} dates de naissance invalidées (âge hors [16-100])")

    # ── R4 : Validation email ───────────────────────────────────
    masque_email_ko = ~df["email"].str.match(PATTERN_EMAIL, na=False)
    nb_emails_ko = masque_email_ko.sum()
    df.loc[masque_email_ko, "email"] = None
    logger.info(f"[R4 email] {nb_emails_ko} emails invalides → NULL")

    # ── R5 : Harmonisation des villes clients ──────────────────
    mapping_villes = _build_mapping_villes(df_regions)
    df["ville"] = (
        df["ville"].str.strip().str.lower()
        .map(mapping_villes)
        .fillna("Non renseignée")
    )
    logger.info(f"[R5 villes] Harmonisation terminée")

    # ── R6 : Tranche d'âge ─────────────────────────────────────
    age_pour_cut = df["age"].fillna(0).astype(float).astype(int)
    df["tranche_age"] = pd.cut(
        age_pour_cut,
        bins=[0, 18, 25, 35, 45, 55, 65, 200],
        labels=["<18", "18-24", "25-34", "35-44", "45-54", "55-64", "65+"],
        right=False
    ).astype(str)
    df.loc[df["age"].isna(), "tranche_age"] = "inconnu"

    nb_final = len(df)
    logger.info(
        f"[TRANSFORM clients] Terminé : {nb_initial} → {nb_final} lignes "
        f"({nb_initial - nb_final} supprimées)"
    )
    return df.reset_index(drop=True)


def calculer_segments_clients(df_commandes: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule le segment Gold / Silver / Bronze pour chaque client
    selon le CA cumulé sur les 12 derniers mois (commandes livrées uniquement).

    Règles métier Mexora :
      Gold   : CA 12 mois >= 15 000 MAD
      Silver : CA 12 mois >=  5 000 MAD
      Bronze : CA 12 mois <   5 000 MAD
    """
    from datetime import timedelta
    date_limite = pd.Timestamp(date.today() - timedelta(days=365))

    df_recents = df_commandes[
        (pd.to_datetime(df_commandes["date_commande"]) >= date_limite) &
        (df_commandes["statut"] == "livré")
    ].copy()

    df_recents["montant_ttc"] = (
        df_recents["quantite"].astype(float) *
        df_recents["prix_unitaire"].astype(float)
    )

    ca_par_client = (
        df_recents.groupby("id_client")["montant_ttc"]
        .sum()
        .reset_index()
        .rename(columns={"montant_ttc": "ca_12m"})
    )

    def _segmenter(ca):
        if ca >= 15000: return "Gold"
        if ca >= 5000:  return "Silver"
        return "Bronze"

    ca_par_client["segment_client"] = ca_par_client["ca_12m"].apply(_segmenter)
    logger.info(
        f"[SEGMENT] {len(ca_par_client)} clients segmentés — "
        f"Gold:{(ca_par_client.segment_client=='Gold').sum()} / "
        f"Silver:{(ca_par_client.segment_client=='Silver').sum()} / "
        f"Bronze:{(ca_par_client.segment_client=='Bronze').sum()}"
    )
    return ca_par_client[["id_client", "segment_client", "ca_12m"]]


def _build_mapping_villes(df_regions: pd.DataFrame) -> dict:
    mapping = {}
    extras = {
        "tnja": "Tanger", "tng": "Tanger",
        "casa": "Casablanca", "fès": "Fès", "fes": "Fès",
        "mrakech": "Marrakech", "meknes": "Meknès",
    }
    for _, row in df_regions.iterrows():
        nom_std = row["nom_ville_standard"].strip()
        for v in [nom_std.lower(), nom_std.upper(), nom_std,
                  row["code_ville"].lower(), row["code_ville"].upper()]:
            mapping[v.strip()] = nom_std
    mapping.update(extras)
    return mapping