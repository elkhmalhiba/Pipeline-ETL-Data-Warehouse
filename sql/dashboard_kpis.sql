-- ============================================================
-- dashboard_kpis.sql — Requêtes analytiques pour le dashboard
-- Mexora Analytics — Étape 4
-- ============================================================

-- ── KPI 1 : Évolution CA mensuel par région ───────────────
-- Question : Quelle région génère le plus de CA ? Évolution 12 mois ?
SELECT
    annee,
    mois,
    libelle_mois,
    region_admin,
    ca_ttc,
    LAG(ca_ttc) OVER (PARTITION BY region_admin ORDER BY annee, mois) AS ca_mois_precedent,
    ROUND(
        (ca_ttc - LAG(ca_ttc) OVER (PARTITION BY region_admin ORDER BY annee, mois))
        / NULLIF(LAG(ca_ttc) OVER (PARTITION BY region_admin ORDER BY annee, mois), 0) * 100
    , 2) AS evolution_pct
FROM reporting_mexora.mv_ca_mensuel
WHERE annee >= EXTRACT(YEAR FROM CURRENT_DATE) - 1
ORDER BY region_admin, annee, mois;

-- ── KPI 2 : Top 10 produits à Tanger par trimestre ───────
-- Question : Quels sont les 10 produits les plus vendus à Tanger ?
SELECT *
FROM reporting_mexora.mv_top_produits
WHERE ville = 'Tanger'
  AND rang <= 10
ORDER BY annee DESC, trimestre DESC, rang;

-- ── KPI 3 : Panier moyen par segment client ───────────────
-- Question : Quel segment client a le panier moyen le plus élevé ?
SELECT
    c.segment_client,
    COUNT(DISTINCT f.id_vente)                              AS nb_commandes,
    ROUND(SUM(f.montant_ttc) / COUNT(DISTINCT f.id_vente), 2) AS panier_moyen,
    ROUND(SUM(f.montant_ttc), 2)                            AS ca_total,
    ROUND(SUM(f.montant_ttc) * 100.0
        / SUM(SUM(f.montant_ttc)) OVER (), 2)              AS pct_ca_total
FROM dwh_mexora.fait_ventes f
JOIN dwh_mexora.dim_client c ON f.id_client = c.id_client_sk
WHERE f.statut_commande = 'livré'
  AND c.est_actif = TRUE
GROUP BY c.segment_client
ORDER BY panier_moyen DESC;

-- ── KPI 4 : Taux de retour par catégorie ─────────────────
-- Question : Quel est le taux de retour par catégorie ?
SELECT
    p.categorie,
    COUNT(*)                                                        AS nb_total,
    COUNT(*) FILTER (WHERE f.statut_commande = 'retourné')          AS nb_retours,
    ROUND(
        COUNT(*) FILTER (WHERE f.statut_commande = 'retourné')
        * 100.0 / NULLIF(COUNT(*), 0), 2
    )                                                               AS taux_retour_pct,
    CASE
        WHEN COUNT(*) FILTER (WHERE f.statut_commande = 'retourné')
             * 100.0 / NULLIF(COUNT(*), 0) > 5  THEN 'ALERTE ROUGE'
        WHEN COUNT(*) FILTER (WHERE f.statut_commande = 'retourné')
             * 100.0 / NULLIF(COUNT(*), 0) > 3  THEN 'ORANGE'
        ELSE 'OK'
    END                                                             AS niveau_alerte
FROM dwh_mexora.fait_ventes f
JOIN dwh_mexora.dim_produit p ON f.id_produit = p.id_produit_sk
GROUP BY p.categorie
ORDER BY taux_retour_pct DESC;

-- ── KPI 5 : Effet Ramadan sur l'alimentation ─────────────
-- Question : Y a-t-il un effet Ramadan visible ?
SELECT
    t.annee,
    t.periode_ramadan,
    CASE WHEN t.periode_ramadan THEN 'Pendant Ramadan' ELSE 'Hors Ramadan' END AS periode,
    COUNT(DISTINCT t.id_date)                  AS nb_jours,
    SUM(f.quantite_vendue)                     AS volume_total,
    ROUND(SUM(f.montant_ttc), 2)               AS ca_total,
    ROUND(SUM(f.montant_ttc)
        / NULLIF(COUNT(DISTINCT t.id_date),0), 2) AS ca_moyen_par_jour,
    ROUND(SUM(f.quantite_vendue)
        / NULLIF(COUNT(DISTINCT t.id_date),0), 2) AS volume_moyen_par_jour
FROM dwh_mexora.fait_ventes f
JOIN dwh_mexora.dim_temps   t ON f.id_date    = t.id_date
JOIN dwh_mexora.dim_produit p ON f.id_produit = p.id_produit_sk
WHERE p.categorie = 'Alimentation'
  AND f.statut_commande = 'livré'
  AND t.annee BETWEEN 2022 AND 2024
GROUP BY t.annee, t.periode_ramadan
ORDER BY t.annee, t.periode_ramadan;