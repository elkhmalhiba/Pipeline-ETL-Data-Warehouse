import sys
import argparse
import logging
from datetime import datetime

from utils.logger import setup_logger
from config.settings import (
    FICHIER_COMMANDES, FICHIER_CLIENTS, FICHIER_PRODUITS, FICHIER_REGIONS,
    DIM_TEMPS_DEBUT, DIM_TEMPS_FIN
)
from extract.extractor import (
    extract_commandes, extract_clients, extract_produits, extract_regions
)
from transform.clean_commandes import transform_commandes
from transform.clean_clients import transform_clients, calculer_segments_clients
from transform.clean_produits import transform_produits
from transform.build_dimensions import (
    build_dim_temps, build_dim_region, build_dim_produit,
    build_dim_client, build_dim_livreur, build_fait_ventes
)
from load.loader import charger_dimension, charger_faits, exporter_csv_local


def run_pipeline(mode: str = "csv"):
    logger = setup_logger("logs")
    start = datetime.now()
    logger.info("DÉMARRAGE PIPELINE ETL MEXORA")

    try:
        # EXTRACT
        df_cmd_raw  = extract_commandes(str(FICHIER_COMMANDES))
        df_cli_raw  = extract_clients(str(FICHIER_CLIENTS))
        df_prod_raw = extract_produits(str(FICHIER_PRODUITS))
        df_regions  = extract_regions(str(FICHIER_REGIONS))

        # TRANSFORM
        df_commandes = transform_commandes(df_cmd_raw, df_regions)
        df_clients   = transform_clients(df_cli_raw, df_regions)
        df_produits  = transform_produits(df_prod_raw)
        df_segments  = calculer_segments_clients(df_commandes)

        dim_temps   = build_dim_temps(DIM_TEMPS_DEBUT, DIM_TEMPS_FIN)
        dim_region  = build_dim_region(df_regions)
        dim_produit = build_dim_produit(df_produits)
        dim_client  = build_dim_client(df_clients, df_segments)
        dim_livreur = build_dim_livreur(df_commandes)
        fait_ventes = build_fait_ventes(
            df_commandes, dim_temps, dim_client,
            dim_produit, dim_region, dim_livreur
        )

        # LOAD
        if mode == "postgres":
            import sqlalchemy
            from config.settings import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME
            url = f"postgresql+pg8000://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
            engine = sqlalchemy.create_engine(url)
            charger_dimension(dim_temps,   "dim_temps",   engine)
            charger_dimension(dim_region,  "dim_region",  engine)
            charger_dimension(dim_produit, "dim_produit", engine)
            charger_dimension(dim_client,  "dim_client",  engine)
            charger_dimension(dim_livreur, "dim_livreur", engine)
            charger_faits(fait_ventes, engine)
        else:
            exporter_csv_local(dim_temps,   "dim_temps")
            exporter_csv_local(dim_region,  "dim_region")
            exporter_csv_local(dim_produit, "dim_produit")
            exporter_csv_local(dim_client,  "dim_client")
            exporter_csv_local(dim_livreur, "dim_livreur")
            exporter_csv_local(fait_ventes, "fait_ventes")

        duree = (datetime.now() - start).seconds
        logger.info(f"PIPELINE TERMINÉ EN {duree} secondes")
        logger.info(f"fait_ventes : {len(fait_ventes)} lignes")

    except Exception as e:
        logger.error(f"ERREUR : {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["csv", "postgres"], default="csv")
    args = parser.parse_args()
    run_pipeline(mode=args.mode)