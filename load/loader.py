# load/loader.py — Phase LOAD du pipeline ETL Mexora

import logging
import pandas as pd

logger = logging.getLogger("mexora_etl")


def charger_dimension(df: pd.DataFrame, table_name: str,
                       engine, schema: str = "dwh_mexora") -> None:
    """
    Charge une table de dimension dans PostgreSQL.
    Stratégie : replace (truncate + reload complet).
    """
    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000,
    )
    logger.info(f"[LOAD] {schema}.{table_name} : {len(df)} lignes chargées")


def charger_faits(df: pd.DataFrame, engine, schema: str = "dwh_mexora") -> None:
    """
    Charge la table de faits par chunks de 5000 lignes.
    Stratégie : replace (rechargement complet à chaque exécution).
    """
    total = len(df)
    df.to_sql(
        name="fait_ventes",
        con=engine,
        schema=schema,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=5000,
    )
    logger.info(f"[LOAD] {schema}.fait_ventes : {total} lignes chargées")


def exporter_csv_local(df: pd.DataFrame, nom: str,
                        dossier: str = "data/output") -> None:
    """
    Exporte un DataFrame en CSV local (alternative sans PostgreSQL).
    Utile pour tester sans base de données.
    """
    import os
    os.makedirs(dossier, exist_ok=True)
    chemin = f"{dossier}/{nom}.csv"
    df.to_csv(chemin, index=False, encoding="utf-8-sig")
    logger.info(f"[EXPORT CSV] {chemin} : {len(df)} lignes exportées")