import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

random.seed(42)
np.random.seed(42)

N = 50000

# ─── Référentiels ───────────────────────────────────────────────
id_clients   = [f"C{str(i).zfill(4)}" for i in range(1, 1001)]
id_produits  = [f"P{str(i).zfill(3)}" for i in range(1, 201)]
id_livreurs  = [f"L{str(i).zfill(3)}" for i in range(1, 51)]

# Villes avec variantes intentionnellement incohérentes
villes_variants = [
    "Tanger", "TNG", "TANGER", "tanger", "Tnja",
    "Casablanca", "CASA", "casa", "Casablanca ",
    "Rabat", "RABAT", "rabat",
    "Fes", "FES", "Fès", "fes",
    "Marrakech", "MARRAKECH", "Mrakech", "marrakech",
    "Agadir", "AGADIR", "agadir",
    "Oujda", "OUJDA", "oujda",
    "Kenitra", "KENITRA", "kenitra",
    "Tetouan", "TETOUAN", "tetouan",
    "Meknes", "MEKNES", "meknes",
]

modes_paiement = ["carte_bancaire", "virement", "cash", "PayPal", "chèque"]

# Statuts : la plupart corrects, quelques-uns non-standards (problème intentionnel)
statuts_normaux      = ["livré", "en_cours", "annulé", "retourné", "expédié"]
statuts_nonstandards = ["OK", "KO", "DONE"]

def random_date(start, end):
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))

start_date = datetime(2023, 1, 1)
end_date   = datetime(2024, 12, 31)

# ─── Génération des données de base ─────────────────────────────
ids_commandes = [f"CMD{str(i).zfill(6)}" for i in range(1, N + 1)]

data = {
    "id_commande"     : ids_commandes,
    "id_client"       : [random.choice(id_clients)  for _ in range(N)],
    "id_produit"      : [random.choice(id_produits) for _ in range(N)],
    "date_commande"   : [random_date(start_date, end_date) for _ in range(N)],
    "quantite"        : [random.randint(1, 20) for _ in range(N)],
    "prix_unitaire"   : [round(random.uniform(50, 15000), 2) for _ in range(N)],
    "statut"          : [random.choice(statuts_normaux) for _ in range(N)],
    "ville_livraison" : [random.choice(villes_variants) for _ in range(N)],
    "mode_paiement"   : [random.choice(modes_paiement) for _ in range(N)],
    "id_livreur"      : [random.choice(id_livreurs) for _ in range(N)],
    "date_livraison"  : [None] * N,
}

df = pd.DataFrame(data)

# Calculer date_livraison = date_commande + 1 à 10 jours
df["date_livraison"] = df["date_commande"].apply(
    lambda d: d + timedelta(days=random.randint(1, 10))
)

# ─── PROBLÈME 1 : Doublons (~3%) ────────────────────────────────
n_doublons = int(N * 0.03)
idx_doublons = np.random.choice(df.index, size=n_doublons, replace=False)
doublons_df = df.loc[idx_doublons].copy()
df = pd.concat([df, doublons_df], ignore_index=True)
df = df.sample(frac=1, random_state=42).reset_index(drop=True)

# ─── PROBLÈME 2 : Dates en formats mixtes ───────────────────────
def format_date_mixte(date, idx):
    r = idx % 3
    if r == 0:
        return date.strftime("%d/%m/%Y")          # 15/11/2024
    elif r == 1:
        return date.strftime("%Y-%m-%d")           # 2024-11-15
    else:
        return date.strftime("%b %d %Y")           # Nov 15 2024

df["date_commande"]  = [format_date_mixte(d, i) for i, d in enumerate(df["date_commande"])]
df["date_livraison"] = [format_date_mixte(d, i) for i, d in enumerate(df["date_livraison"])]

# ─── PROBLÈME 3 : Valeurs manquantes sur id_livreur (7%) ────────
idx_nan = np.random.choice(df.index, size=int(len(df) * 0.07), replace=False)
df.loc[idx_nan, "id_livreur"] = np.nan

# ─── PROBLÈME 4 : Quantités négatives (quelques lignes) ─────────
idx_neg = np.random.choice(df.index, size=random.randint(30, 80), replace=False)
df.loc[idx_neg, "quantite"] = df.loc[idx_neg, "quantite"].apply(lambda x: -abs(x))

# ─── PROBLÈME 5 : prix_unitaire à 0 (commandes test) ────────────
idx_zero = np.random.choice(df.index, size=random.randint(40, 100), replace=False)
df.loc[idx_zero, "prix_unitaire"] = 0.0

# ─── PROBLÈME 6 : Statuts non-standards (~5%) ────────────────────
idx_bad_statut = np.random.choice(df.index, size=int(len(df) * 0.05), replace=False)
df.loc[idx_bad_statut, "statut"] = [
    random.choice(statuts_nonstandards) for _ in range(len(idx_bad_statut))
]

# ─── Export CSV ──────────────────────────────────────────────────
output_path = "commandes_mexora.csv"
df.to_csv(output_path, index=False, encoding="utf-8-sig")

print(f"Fichier genere : {output_path}")
print(f"Nombre total de lignes : {len(df)}")
print(f"\nResume des problemes injectes :")
print(f"  Doublons              : ~{n_doublons} lignes dupliquees")
print(f"  Dates formats mixtes  : 3 formats differents (dd/mm/yyyy, yyyy-mm-dd, Mon dd yyyy)")
print(f"  id_livreur manquant   : {df['id_livreur'].isna().sum()} lignes ({df['id_livreur'].isna().mean()*100:.1f}%)")
print(f"  Quantites negatives   : {(df['quantite'] < 0).sum()} lignes")
print(f"  Prix unitaire = 0     : {(df['prix_unitaire'] == 0).sum()} lignes")
print(f"  Statuts non-standards : {df['statut'].isin(statuts_nonstandards).sum()} lignes")
print(f"\nApercu des 5 premieres lignes :")
print(df.head())

