# extract/extractor.py — Phase EXTRACT du pipeline ETL Mexora

import json
import logging
import pandas as pd

logger = logging.getLogger("mexora_etl")


def extract_commandes(filepath: str) -> pd.DataFrame:
    """
    Extrait les commandes depuis le fichier CSV source.
    Tout est lu en string pour éviter les conversions implicites de pandas.
    """
    df = pd.read_csv(filepath, encoding="utf-8-sig", dtype=str)
    df.columns = df.columns.str.strip()
    logger.info(f"[EXTRACT] commandes : {len(df)} lignes extraites depuis {filepath}")
    return df


def extract_clients(filepath: str) -> pd.DataFrame:
    """Extrait les clients depuis le fichier CSV source."""
    df = pd.read_csv(filepath, encoding="utf-8-sig", dtype=str)
    df.columns = df.columns.str.strip()
    logger.info(f"[EXTRACT] clients : {len(df)} lignes extraites depuis {filepath}")
    return df


def extract_produits(filepath: str) -> pd.DataFrame:
    """Extrait les produits depuis le fichier JSON source."""
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    df = pd.DataFrame(data["produits"])
    logger.info(f"[EXTRACT] produits : {len(df)} lignes extraites depuis {filepath}")
    return df


def extract_regions(filepath: str) -> pd.DataFrame:
    """
    Extrait le référentiel géographique officiel.
    Ce fichier est propre — pas de nettoyage nécessaire.
    """
    df = pd.read_csv(filepath, encoding="utf-8-sig", dtype=str)
    df.columns = df.columns.str.strip()
    logger.info(f"[EXTRACT] regions : {len(df)} lignes extraites depuis {filepath}")
    return df