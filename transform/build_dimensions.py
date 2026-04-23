# transform/build_dimensions.py — Construction des tables de dimensions

import logging
import pandas as pd
from datetime import date

logger = logging.getLogger("mexora_etl")

# ── Jours fériés marocains ──────────────────────────────────────
FERIES_MAROC = {
    "2020-01-01","2020-01-11","2020-05-01","2020-07-30",
    "2020-08-14","2020-11-06","2020-11-18",
    "2021-01-01","2021-01-11","2021-05-01","2021-07-30",
    "2021-08-14","2021-11-06","2021-11-18",
    "2022-01-01","2022-01-11","2022-05-01","2022-07-30",
    "2022-08-14","2022-11-06","2022-11-18",
    "2023-01-01","2023-01-11","2023-05-01","2023-07-30",
    "2023-08-14","2023-11-06","2023-11-18",
    "2024-01-01","2024-01-11","2024-05-01","2024-07-30",
    "2024-08-14","2024-11-06","2024-11-18",
    "2025-01-01","2025-01-11","2025-05-01","2025-07-30",
    "2025-08-14","2025-11-06","2025-11-18",
    "2026-01-01","2026-01-11","2026-05-01","2026-07-30",
    "2026-08-14","2026-11-06","2026-11-18",
}

# ── Périodes Ramadan (approximatives) ──────────────────────────
RAMADAN_PERIODES = [
    ("2020-04-23", "2020-05-23"),
    ("2021-04-12", "2021-05-12"),
    ("2022-04-02", "2022-05-01"),
    ("2023-03-22", "2023-04-20"),
    ("2024-03-10", "2024-04-09"),
    ("2025-03-01", "2025-03-29"),
    ("2026-02-18", "2026-03-19"),
]


def build_dim_temps(date_debut: str, date_fin: str) -> pd.DataFrame:
    dates = pd.date_range(start=date_debut, end=date_fin, freq="D")

    # Jours et mois en ASCII pur — pas de caractères spéciaux
    jours = ["Lundi","Mardi","Mercredi","Jeudi","Vendredi","Samedi","Dimanche"]
    mois  = ["","Janvier","Fevrier","Mars","Avril","Mai","Juin",
             "Juillet","Aout","Septembre","Octobre","Novembre","Decembre"]

    df = pd.DataFrame({
        "id_date":         dates.strftime("%Y%m%d").astype(int),
        "date_complete":   dates.strftime("%Y-%m-%d"),
        "jour":            dates.day,
        "mois":            dates.month,
        "trimestre":       dates.quarter,
        "annee":           dates.year,
        "semaine":         dates.isocalendar().week.astype(int),
        "libelle_jour":    [jours[d] for d in dates.dayofweek],
        "libelle_mois":    [mois[m] for m in dates.month],
        "est_weekend":     dates.dayofweek >= 5,
        "est_ferie_maroc": dates.strftime("%Y-%m-%d").isin(FERIES_MAROC),
        "periode_ramadan": False,
    })

    for debut, fin in RAMADAN_PERIODES:
        masque = (df["date_complete"] >= debut) & (df["date_complete"] <= fin)
        df.loc[masque, "periode_ramadan"] = True

    logger.info(f"[DIM_TEMPS] {len(df)} jours generés ({date_debut} -> {date_fin})")
    return df

def build_dim_region(df_regions: pd.DataFrame) -> pd.DataFrame:
    """Construit DIM_REGION depuis le référentiel officiel."""
    df = df_regions.copy()
    df = df.rename(columns={"nom_ville_standard": "ville"})
    df.insert(0, "id_region", range(1, len(df) + 1))
    logger.info(f"[DIM_REGION] {len(df)} régions chargées")
    return df


def build_dim_produit(df_produits: pd.DataFrame) -> pd.DataFrame:
    """
    Construit DIM_PRODUIT avec colonnes SCD Type 2.
    Chaque produit a une version active (date_fin = 9999-12-31).
    """
    df = df_produits.copy()
    df = df.rename(columns={"id_produit": "id_produit_nk"})
    df.insert(0, "id_produit_sk", range(1, len(df) + 1))
    df["date_debut"] = date.today().strftime("%Y-%m-%d")
    df["date_fin"]   = "9999-12-31"
    df["est_actif"]  = df["actif"]
    df = df.drop(columns=["actif"], errors="ignore")
    logger.info(f"[DIM_PRODUIT] {len(df)} produits — SCD Type 2 initialisé")
    return df


def build_dim_client(df_clients: pd.DataFrame,
                     df_segments: pd.DataFrame) -> pd.DataFrame:
    """
    Construit DIM_CLIENT avec segmentation et SCD Type 2.
    Fusionne les données clients nettoyées avec les segments calculés.
    """
    df = df_clients.copy()

    # Fusion avec segments
    df = df.merge(df_segments[["id_client", "segment_client"]], on="id_client", how="left")
    df["segment_client"] = df["segment_client"].fillna("Bronze")

    # Nom complet
    df["nom_complet"] = df["prenom"].str.strip() + " " + df["nom"].str.strip()

    # Colonnes DWH
    df = df.rename(columns={"id_client": "id_client_nk"})
    df.insert(0, "id_client_sk", range(1, len(df) + 1))
    df["date_debut"] = date.today().strftime("%Y-%m-%d")
    df["date_fin"]   = "9999-12-31"
    df["est_actif"]  = True

    cols = ["id_client_sk", "id_client_nk", "nom_complet", "tranche_age",
            "sexe", "ville", "segment_client", "canal_acquisition",
            "date_debut", "date_fin", "est_actif"]
    df = df[[c for c in cols if c in df.columns]]

    logger.info(f"[DIM_CLIENT] {len(df)} clients — SCD Type 2 initialisé")
    return df


def build_dim_livreur(df_commandes: pd.DataFrame) -> pd.DataFrame:
    """Construit DIM_LIVREUR depuis les id_livreur uniques des commandes."""
    livreurs = df_commandes["id_livreur"].dropna().unique()
    livreurs = sorted(livreurs)

    rows = []
    for nk in livreurs:
        rows.append({
            "id_livreur_nk":  str(nk),
            "nom_livreur":    f"Livreur {nk}" if nk != "-1" else "Inconnu",
            "type_transport": "moto" if nk != "-1" else "inconnu",
            "zone_couverture":"Maroc",
            "actif":          nk != "-1",
        })

    df = pd.DataFrame(rows)
    df.insert(0, "id_livreur", range(1, len(df) + 1))
    logger.info(f"[DIM_LIVREUR] {len(df)} livreurs construits")
    return df


def build_fait_ventes(df_commandes: pd.DataFrame,
                      dim_temps:    pd.DataFrame,
                      dim_client:   pd.DataFrame,
                      dim_produit:  pd.DataFrame,
                      dim_region:   pd.DataFrame,
                      dim_livreur:  pd.DataFrame) -> pd.DataFrame:
    """
    Construit la table de faits FAIT_VENTES en résolvant
    toutes les clés étrangères (surrogate keys).
    """
    df = df_commandes.copy()

    # ── Résolution FK date ──────────────────────────────────────
    df["id_date"] = pd.to_datetime(df["date_commande"]).dt.strftime("%Y%m%d").astype(int)

    # ── Résolution FK client ────────────────────────────────────
    client_idx = dim_client.set_index("id_client_nk")["id_client_sk"].to_dict()
    df["id_client"] = df["id_client"].map(client_idx)

    # ── Résolution FK produit ───────────────────────────────────
    produit_idx = dim_produit.set_index("id_produit_nk")["id_produit_sk"].to_dict()
    df["id_produit"] = df["id_produit"].map(produit_idx)

    # ── Résolution FK region ────────────────────────────────────
    region_idx = dim_region.set_index("ville")["id_region"].to_dict()
    df["id_region"] = df["ville_livraison"].map(region_idx).fillna(1).astype(int)

    # ── Résolution FK livreur ───────────────────────────────────
    livreur_idx = dim_livreur.set_index("id_livreur_nk")["id_livreur"].to_dict()
    df["id_livreur"] = df["id_livreur"].map(livreur_idx)

    # ── Calcul délai livraison ──────────────────────────────────
    df["delai_livraison_jours"] = (
        pd.to_datetime(df["date_livraison"]) -
        pd.to_datetime(df["date_commande"])
    ).dt.days

    # ── Table de faits finale ───────────────────────────────────
    fait = pd.DataFrame({
        "id_date":               df["id_date"],
        "id_produit":            df["id_produit"],
        "id_client":             df["id_client"],
        "id_region":             df["id_region"],
        "id_livreur":            df["id_livreur"],
        "quantite_vendue":       df["quantite"].astype(int),
        "montant_ht":            (df["montant_ttc"] / 1.20).round(2),
        "montant_ttc":           df["montant_ttc"],
        "delai_livraison_jours": df["delai_livraison_jours"],
        "prix_unitaire":         df["prix_unitaire"],
        "statut_commande":       df["statut"],
    })

    fait = fait.dropna(subset=["id_date", "id_produit", "id_client"])
    fait.insert(0, "id_vente", range(1, len(fait) + 1))

    logger.info(f"[FAIT_VENTES] {len(fait)} lignes construites")
    return fait.reset_index(drop=True)