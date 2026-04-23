# utils/logger.py — Configuration du logging ETL Mexora

import logging
import sys
from datetime import datetime
from pathlib import Path

def setup_logger(log_dir: str = "logs") -> logging.Logger:
    """
    Configure et retourne le logger principal du pipeline ETL.
    Écrit dans un fichier horodaté ET dans la console.
    """
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    timestamp  = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file   = Path(log_dir) / f"etl_{timestamp}.log"

    logger = logging.getLogger("mexora_etl")
    logger.setLevel(logging.DEBUG)

    # Format des messages
    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Handler fichier
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    # Handler console
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)

    logger.info(f"Logger initialisé — fichier : {log_file}")
    return logger


def log_etape(logger, etape: str, nb_avant: int, nb_apres: int, detail: str = ""):
    """Log standardisé pour chaque transformation avec comptage avant/après."""
    supprimees = nb_avant - nb_apres
    logger.info(
        f"[{etape}] {nb_avant} → {nb_apres} lignes "
        f"({supprimees} supprimées) {detail}"
    )