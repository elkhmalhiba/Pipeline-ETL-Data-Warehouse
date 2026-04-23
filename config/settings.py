# config/settings.py — Paramètres globaux du pipeline ETL Mexora

import os
from pathlib import Path

# ── Chemins ────────────────────────────────────────────────────
BASE_DIR  = Path(__file__).resolve().parent.parent
DATA_DIR  = BASE_DIR / "data"
LOG_DIR   = BASE_DIR / "logs"

FICHIER_COMMANDES = DATA_DIR / "commandes_mexora.csv"
FICHIER_CLIENTS   = DATA_DIR / "clients_mexora.csv"
FICHIER_PRODUITS  = DATA_DIR / "produits_mexora.json"
FICHIER_REGIONS   = DATA_DIR / "regions_maroc.csv"

# ── Base de données PostgreSQL ──────────────────────────────────
DB_HOST     = os.getenv("DB_HOST",     "localhost")
DB_PORT     = os.getenv("DB_PORT",     "5432")
DB_NAME     = os.getenv("DB_NAME",     "mexora_dwh")
DB_USER     = os.getenv("DB_USER",     "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ── Schémas PostgreSQL ──────────────────────────────────────────
SCHEMA_DWH       = "dwh_mexora"
SCHEMA_STAGING   = "staging_mexora"
SCHEMA_REPORTING = "reporting_mexora"

# ── Règles métier segmentation client ──────────────────────────
SEUIL_GOLD   = 15000   # MAD — CA 12 mois >= 15000
SEUIL_SILVER = 5000    # MAD — CA 12 mois >= 5000

# ── Règles validation clients ───────────────────────────────────
AGE_MIN = 16
AGE_MAX = 100

# ── Dates dimension temporelle ──────────────────────────────────
DIM_TEMPS_DEBUT = "2020-01-01"
DIM_TEMPS_FIN   = "2026-12-31"