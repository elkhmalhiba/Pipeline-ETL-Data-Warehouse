# Mexora ETL — Pipeline Data Warehouse

## Structure du projet

```
mexora_etl/
├── config/
│   └── settings.py          # Paramètres de connexion et chemins
├── extract/
│   └── extractor.py         # Fonctions d'extraction (CSV, JSON)
├── transform/
│   ├── clean_commandes.py   # Nettoyage des commandes (7 règles)
│   ├── clean_clients.py     # Nettoyage des clients (6 règles)
│   ├── clean_produits.py    # Nettoyage des produits (4 règles)
│   └── build_dimensions.py  # Construction des 5 dimensions + faits
├── load/
│   └── loader.py            # Chargement PostgreSQL ou export CSV
├── utils/
│   └── logger.py            # Logging structuré
├── sql/
│   ├── create_dwh.sql       # Création schéma PostgreSQL + vues
│   ├── check_integrity.sql  # Vérification intégrité référentielle
│   └── dashboard_kpis.sql   # Requêtes analytiques dashboard
├── data/                    # Mettre ici les 4 fichiers sources
├── logs/                    # Logs générés automatiquement
├── main.py                  # Point d'entrée du pipeline
└── requirements.txt
```

## Installation

```bash
# 1. Cloner le dépôt
git clone https://github.com/VOTRE_USERNAME/mexora-etl.git
cd mexora-etl

# 2. Installer les dépendances
pip install -r requirements.txt

# 3. Placer les fichiers sources dans data/
cp commandes_mexora.csv  data/
cp clients_mexora.csv    data/
cp produits_mexora.json  data/
cp regions_maroc.csv     data/
```

## Lancer le pipeline

### Mode CSV (sans PostgreSQL — pour tester)
```bash
python main.py --mode csv
```
Les tables sont exportées dans `data/output/`

### Mode PostgreSQL
```bash
# Créer la base d'abord
psql -U postgres -c "CREATE DATABASE mexora_dwh;"
psql -U postgres -d mexora_dwh -f sql/create_dwh.sql

# Lancer le pipeline
python main.py --mode postgres

# Vérifier l'intégrité
psql -U postgres -d mexora_dwh -f sql/check_integrity.sql
```

## Variables d'environnement (optionnel)

```bash
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=mexora_dwh
export DB_USER=postgres
export DB_PASSWORD=votre_mot_de_passe
```

## Résultats attendus

```
dim_temps    : ~2557 lignes (2020-2026)
dim_region   :   20 lignes
dim_produit  :  120 lignes
dim_client   : ~1000 lignes
dim_livreur  :   51 lignes
fait_ventes  : ~48000 lignes (après nettoyage)
```
