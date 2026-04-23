-- ============================================================
-- check_integrity.sql — Vérification intégrité DWH Mexora
-- Lancer après le chargement ETL pour valider les données
-- Usage : psql -U postgres -d mexora_dwh -f check_integrity.sql
-- ============================================================

\echo '======================================================'
\echo ' VÉRIFICATION INTÉGRITÉ — DWH MEXORA'
\echo '======================================================'

-- ── 1. Comptage des lignes par table ──────────────────────
\echo ''
\echo '--- COMPTAGE LIGNES ---'

SELECT 'dim_temps'    AS table_name, COUNT(*) AS nb_lignes FROM dwh_mexora.dim_temps
UNION ALL
SELECT 'dim_region',    COUNT(*) FROM dwh_mexora.dim_region
UNION ALL
SELECT 'dim_produit',   COUNT(*) FROM dwh_mexora.dim_produit
UNION ALL
SELECT 'dim_client',    COUNT(*) FROM dwh_mexora.dim_client
UNION ALL
SELECT 'dim_livreur',   COUNT(*) FROM dwh_mexora.dim_livreur
UNION ALL
SELECT 'fait_ventes',   COUNT(*) FROM dwh_mexora.fait_ventes
ORDER BY table_name;

-- ── 2. Orphelins dans fait_ventes (FK manquantes) ─────────
\echo ''
\echo '--- VÉRIFICATION CLÉS ÉTRANGÈRES (doit être 0) ---'

SELECT 'FK id_date orphelines' AS verification,
       COUNT(*) AS nb_orphelins
FROM dwh_mexora.fait_ventes f
WHERE NOT EXISTS (
    SELECT 1 FROM dwh_mexora.dim_temps t WHERE t.id_date = f.id_date
)
UNION ALL
SELECT 'FK id_produit orphelines',
       COUNT(*)
FROM dwh_mexora.fait_ventes f
WHERE NOT EXISTS (
    SELECT 1 FROM dwh_mexora.dim_produit p WHERE p.id_produit_sk = f.id_produit
)
UNION ALL
SELECT 'FK id_client orphelines',
       COUNT(*)
FROM dwh_mexora.fait_ventes f
WHERE NOT EXISTS (
    SELECT 1 FROM dwh_mexora.dim_client c WHERE c.id_client_sk = f.id_client
)
UNION ALL
SELECT 'FK id_region orphelines',
       COUNT(*)
FROM dwh_mexora.fait_ventes f
WHERE NOT EXISTS (
    SELECT 1 FROM dwh_mexora.dim_region r WHERE r.id_region = f.id_region
);

-- ── 3. Valeurs nulles critiques ───────────────────────────
\echo ''
\echo '--- VALEURS NULLES CRITIQUES (doit être 0) ---'

SELECT 'montant_ttc NULL'  AS verification, COUNT(*) AS nb
FROM dwh_mexora.fait_ventes WHERE montant_ttc IS NULL
UNION ALL
SELECT 'quantite_vendue NULL', COUNT(*)
FROM dwh_mexora.fait_ventes WHERE quantite_vendue IS NULL
UNION ALL
SELECT 'quantite_vendue <= 0', COUNT(*)
FROM dwh_mexora.fait_ventes WHERE quantite_vendue <= 0
UNION ALL
SELECT 'montant_ttc <= 0', COUNT(*)
FROM dwh_mexora.fait_ventes WHERE montant_ttc <= 0;

-- ── 4. Statuts dans fait_ventes ───────────────────────────
\echo ''
\echo '--- RÉPARTITION STATUTS COMMANDES ---'

SELECT statut_commande, COUNT(*) AS nb,
       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM dwh_mexora.fait_ventes
GROUP BY statut_commande
ORDER BY nb DESC;

-- ── 5. Vérification SCD Type 2 ────────────────────────────
\echo ''
\echo '--- VÉRIFICATION SCD TYPE 2 ---'

SELECT 'Produits actifs'        AS info, COUNT(*) AS nb
FROM dwh_mexora.dim_produit WHERE est_actif = TRUE
UNION ALL
SELECT 'Produits inactifs (SCD)', COUNT(*)
FROM dwh_mexora.dim_produit WHERE est_actif = FALSE
UNION ALL
SELECT 'Clients actifs',          COUNT(*)
FROM dwh_mexora.dim_client WHERE est_actif = TRUE
UNION ALL
SELECT 'Clients inactifs (SCD)',   COUNT(*)
FROM dwh_mexora.dim_client WHERE est_actif = FALSE;

-- ── 6. Vérification période Ramadan ───────────────────────
\echo ''
\echo '--- JOURS RAMADAN DANS DIM_TEMPS ---'

SELECT annee,
       COUNT(*) FILTER (WHERE periode_ramadan = TRUE)  AS jours_ramadan,
       COUNT(*) FILTER (WHERE periode_ramadan = FALSE) AS jours_hors_ramadan
FROM dwh_mexora.dim_temps
GROUP BY annee
ORDER BY annee;

-- ── 7. CA total de contrôle ───────────────────────────────
\echo ''
\echo '--- KPIs DE CONTRÔLE ---'

SELECT
    COUNT(*)                                    AS nb_total_commandes,
    COUNT(*) FILTER (WHERE statut_commande = 'livré')     AS nb_livrees,
    COUNT(*) FILTER (WHERE statut_commande = 'retourné')  AS nb_retournees,
    ROUND(SUM(montant_ttc), 2)                  AS ca_ttc_total,
    ROUND(AVG(montant_ttc), 2)                  AS panier_moyen,
    ROUND(AVG(delai_livraison_jours), 1)        AS delai_moyen_jours
FROM dwh_mexora.fait_ventes;

\echo ''
\echo '======================================================'
\echo ' VÉRIFICATION TERMINÉE'
\echo '======================================================'