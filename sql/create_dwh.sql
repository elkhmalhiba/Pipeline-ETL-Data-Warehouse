-- ============================================================
-- create_dwh.sql — Création du Data Warehouse Mexora
-- PostgreSQL 15+
-- Usage : psql -U postgres -d mexora_dwh -f create_dwh.sql
-- ============================================================

-- ── Schémas ──────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS staging_mexora;
CREATE SCHEMA IF NOT EXISTS dwh_mexora;
CREATE SCHEMA IF NOT EXISTS reporting_mexora;

-- ============================================================
-- DIMENSIONS
-- ============================================================

-- DIM_TEMPS
DROP TABLE IF EXISTS dwh_mexora.dim_temps CASCADE;
CREATE TABLE dwh_mexora.dim_temps (
    id_date           INTEGER      PRIMARY KEY,
    date_complete     DATE         NOT NULL,
    jour              SMALLINT     NOT NULL CHECK (jour BETWEEN 1 AND 31),
    mois              SMALLINT     NOT NULL CHECK (mois BETWEEN 1 AND 12),
    trimestre         SMALLINT     NOT NULL CHECK (trimestre BETWEEN 1 AND 4),
    annee             SMALLINT     NOT NULL,
    semaine           SMALLINT,
    libelle_jour      VARCHAR(20),
    libelle_mois      VARCHAR(20),
    est_weekend       BOOLEAN      DEFAULT FALSE,
    est_ferie_maroc   BOOLEAN      DEFAULT FALSE,
    periode_ramadan   BOOLEAN      DEFAULT FALSE
);

-- DIM_REGION
DROP TABLE IF EXISTS dwh_mexora.dim_region CASCADE;
CREATE TABLE dwh_mexora.dim_region (
    id_region         SERIAL       PRIMARY KEY,
    code_ville        VARCHAR(10)  NOT NULL,
    ville             VARCHAR(100) NOT NULL,
    province          VARCHAR(100),
    region_admin      VARCHAR(100),
    zone_geo          VARCHAR(50),
    population        INTEGER,
    code_postal       VARCHAR(10),
    pays              VARCHAR(50)  DEFAULT 'Maroc'
);

-- DIM_PRODUIT (SCD Type 2)
DROP TABLE IF EXISTS dwh_mexora.dim_produit CASCADE;
CREATE TABLE dwh_mexora.dim_produit (
    id_produit_sk     SERIAL       PRIMARY KEY,
    id_produit_nk     VARCHAR(20)  NOT NULL,
    nom_produit       VARCHAR(200) NOT NULL,
    categorie         VARCHAR(100),
    sous_categorie    VARCHAR(100),
    marque            VARCHAR(100),
    fournisseur       VARCHAR(100),
    prix_standard     DECIMAL(10,2),
    origine_pays      VARCHAR(50),
    date_debut        DATE         NOT NULL DEFAULT CURRENT_DATE,
    date_fin          DATE         NOT NULL DEFAULT '9999-12-31',
    est_actif         BOOLEAN      NOT NULL DEFAULT TRUE
);
COMMENT ON TABLE dwh_mexora.dim_produit IS
    'SCD Type 2 : chaque changement de categorie ou statut crée une nouvelle ligne';

-- DIM_CLIENT (SCD Type 2)
DROP TABLE IF EXISTS dwh_mexora.dim_client CASCADE;
CREATE TABLE dwh_mexora.dim_client (
    id_client_sk      SERIAL       PRIMARY KEY,
    id_client_nk      VARCHAR(20)  NOT NULL,
    nom_complet       VARCHAR(200),
    tranche_age       VARCHAR(10),
    sexe              CHAR(1)      CHECK (sexe IN ('m','f','i')),
    ville             VARCHAR(100),
    region_admin      VARCHAR(100),
    segment_client    VARCHAR(10)  CHECK (segment_client IN ('Gold','Silver','Bronze')),
    canal_acquisition VARCHAR(50),
    date_debut        DATE         NOT NULL DEFAULT CURRENT_DATE,
    date_fin          DATE         NOT NULL DEFAULT '9999-12-31',
    est_actif         BOOLEAN      NOT NULL DEFAULT TRUE
);
COMMENT ON TABLE dwh_mexora.dim_client IS
    'SCD Type 2 : changement de segment conservé en historique';

-- DIM_LIVREUR
DROP TABLE IF EXISTS dwh_mexora.dim_livreur CASCADE;
CREATE TABLE dwh_mexora.dim_livreur (
    id_livreur        SERIAL       PRIMARY KEY,
    id_livreur_nk     VARCHAR(20),
    nom_livreur       VARCHAR(100),
    type_transport    VARCHAR(50),
    zone_couverture   VARCHAR(100),
    actif             BOOLEAN      DEFAULT TRUE
);
COMMENT ON COLUMN dwh_mexora.dim_livreur.id_livreur_nk IS
    'id_livreur_nk = -1 réservé pour livreur inconnu';

-- ============================================================
-- TABLE DE FAITS
-- ============================================================
DROP TABLE IF EXISTS dwh_mexora.fait_ventes CASCADE;
CREATE TABLE dwh_mexora.fait_ventes (
    id_vente              BIGSERIAL    PRIMARY KEY,
    -- Clés étrangères
    id_date               INTEGER      NOT NULL REFERENCES dwh_mexora.dim_temps(id_date),
    id_produit            INTEGER      NOT NULL REFERENCES dwh_mexora.dim_produit(id_produit_sk),
    id_client             INTEGER      NOT NULL REFERENCES dwh_mexora.dim_client(id_client_sk),
    id_region             INTEGER      NOT NULL REFERENCES dwh_mexora.dim_region(id_region),
    id_livreur            INTEGER               REFERENCES dwh_mexora.dim_livreur(id_livreur),
    -- Mesures additives
    quantite_vendue       INTEGER      NOT NULL CHECK (quantite_vendue > 0),
    montant_ht            DECIMAL(12,2) NOT NULL,
    montant_ttc           DECIMAL(12,2) NOT NULL,
    -- Mesure semi-additive
    delai_livraison_jours SMALLINT,
    -- Mesures non-additives
    prix_unitaire         DECIMAL(10,2),
    remise_pct            DECIMAL(5,2) DEFAULT 0,
    -- Qualification
    statut_commande       VARCHAR(20)  CHECK (
                              statut_commande IN ('livré','annulé','en_cours','retourné','inconnu')
                          ),
    -- Métadonnée ETL
    date_chargement       TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- INDEX
-- ============================================================
CREATE INDEX idx_fv_date     ON dwh_mexora.fait_ventes(id_date);
CREATE INDEX idx_fv_produit  ON dwh_mexora.fait_ventes(id_produit);
CREATE INDEX idx_fv_client   ON dwh_mexora.fait_ventes(id_client);
CREATE INDEX idx_fv_region   ON dwh_mexora.fait_ventes(id_region);
CREATE INDEX idx_fv_livreur  ON dwh_mexora.fait_ventes(id_livreur);
CREATE INDEX idx_fv_statut   ON dwh_mexora.fait_ventes(statut_commande)
    WHERE statut_commande = 'livré';
CREATE INDEX idx_fv_date_region ON dwh_mexora.fait_ventes(id_date, id_region)
    INCLUDE (montant_ttc, quantite_vendue);

-- ============================================================
-- VUES MATÉRIALISÉES
-- ============================================================

-- Vue 1 — CA mensuel par région et catégorie
CREATE MATERIALIZED VIEW reporting_mexora.mv_ca_mensuel AS
SELECT
    t.annee,
    t.mois,
    t.libelle_mois,
    t.trimestre,
    t.periode_ramadan,
    r.region_admin,
    r.zone_geo,
    r.ville,
    p.categorie,
    p.sous_categorie,
    SUM(f.montant_ttc)           AS ca_ttc,
    SUM(f.montant_ht)            AS ca_ht,
    SUM(f.quantite_vendue)       AS volume_vendu,
    COUNT(DISTINCT f.id_vente)   AS nb_commandes,
    COUNT(DISTINCT f.id_client)  AS nb_clients_actifs,
    ROUND(AVG(f.montant_ttc),2)  AS panier_moyen
FROM dwh_mexora.fait_ventes     f
JOIN dwh_mexora.dim_temps       t ON f.id_date    = t.id_date
JOIN dwh_mexora.dim_region      r ON f.id_region  = r.id_region
JOIN dwh_mexora.dim_produit     p ON f.id_produit = p.id_produit_sk
WHERE f.statut_commande = 'livré'
GROUP BY t.annee, t.mois, t.libelle_mois, t.trimestre, t.periode_ramadan,
         r.region_admin, r.zone_geo, r.ville, p.categorie, p.sous_categorie
WITH DATA;

CREATE INDEX ON reporting_mexora.mv_ca_mensuel(annee, mois);
CREATE INDEX ON reporting_mexora.mv_ca_mensuel(region_admin);
CREATE INDEX ON reporting_mexora.mv_ca_mensuel(categorie);

-- Vue 2 — Top produits par trimestre
CREATE MATERIALIZED VIEW reporting_mexora.mv_top_produits AS
SELECT
    t.annee,
    t.trimestre,
    r.ville,
    p.id_produit_nk,
    p.nom_produit,
    p.categorie,
    p.marque,
    SUM(f.quantite_vendue)      AS qte_totale,
    SUM(f.montant_ttc)          AS ca_total,
    COUNT(DISTINCT f.id_client) AS nb_clients_distincts,
    RANK() OVER (
        PARTITION BY t.annee, t.trimestre, r.ville
        ORDER BY SUM(f.montant_ttc) DESC
    ) AS rang
FROM dwh_mexora.fait_ventes     f
JOIN dwh_mexora.dim_temps       t ON f.id_date    = t.id_date
JOIN dwh_mexora.dim_produit     p ON f.id_produit = p.id_produit_sk
JOIN dwh_mexora.dim_region      r ON f.id_region  = r.id_region
WHERE f.statut_commande = 'livré'
  AND p.est_actif = TRUE
GROUP BY t.annee, t.trimestre, r.ville, p.id_produit_nk, p.nom_produit, p.categorie, p.marque
WITH DATA;

-- Vue 3 — Performance livreurs
CREATE MATERIALIZED VIEW reporting_mexora.mv_performance_livreurs AS
SELECT
    l.nom_livreur,
    l.zone_couverture,
    t.annee,
    t.mois,
    COUNT(*)                                              AS nb_livraisons,
    ROUND(AVG(f.delai_livraison_jours),1)                AS delai_moyen_jours,
    COUNT(*) FILTER (WHERE f.delai_livraison_jours > 3)  AS nb_retards,
    ROUND(
        COUNT(*) FILTER (WHERE f.delai_livraison_jours > 3)
        * 100.0 / NULLIF(COUNT(*), 0), 2
    )                                                     AS taux_retard_pct
FROM dwh_mexora.fait_ventes     f
JOIN dwh_mexora.dim_livreur     l ON f.id_livreur = l.id_livreur
JOIN dwh_mexora.dim_temps       t ON f.id_date    = t.id_date
WHERE f.statut_commande IN ('livré','retourné')
  AND f.delai_livraison_jours IS NOT NULL
  AND l.id_livreur_nk != '-1'
GROUP BY l.nom_livreur, l.zone_couverture, t.annee, t.mois
WITH DATA;