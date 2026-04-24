# VERSION DE Konate (dans feature/clean-nulls) :
# =============================================================
# transformations.py — Module de nettoyage et transformation
# Version : 1.0.0 | Date : 2026-04-25
# =============================================================


# (konatemoussa123 repart du même header que celui d'Alice,
#  qui était sur develop au moment du branching.)
 
# ─────────────────────────────────────────────────────────
# FONCTION 2 (konatemoussa123) : clean_nulls
# ─────────────────────────────────────────────────────────
def clean_nulls(df: DataFrame) -> DataFrame:
    """
    Supprime les lignes qui contiennent au moins une valeur
    nulle dans les colonnes critiques du pipeline.
 
    POURQUOI filtrer les nulls ?
    Les valeurs nulles (None / NaN / NULL) sont piégeuses dans
    Spark : elles se propagent silencieusement dans les calculs.
    ex: 1500 + NULL = NULL. Il vaut mieux les détecter tôt.
 
    STRATÉGIE CHOISIE : suppression des lignes incomplètes.
    Alternative possible : imputation (remplacer par la moyenne,
    la médiane, ou une valeur par défaut).
 
    Args:
        df: DataFrame potentiellement avec des nulls.
 
    Returns:
        DataFrame sans lignes nulles sur les colonnes critiques.
    """
    print("🧹 clean_nulls : nettoyage des valeurs manquantes...")
 
    # Compter les lignes avant le nettoyage pour le reporting
    count_avant = df.count()
 
    # Colonnes que l'on juge critiques pour notre pipeline.
    # Si une de ces colonnes est null, la ligne est inutilisable.
    colonnes_critiques = ["region", "produit", "ventes"]
 
    # dropna() supprime les lignes avec des nulls.
    # subset : on ne vérifie que les colonnes listées.
    # how='any' : supprime si AU MOINS UNE colonne est nulle.
    # how='all' aurait supprimé seulement si TOUTES étaient nulles.
    df_clean = df.dropna(
        how="any",
        subset=colonnes_critiques
    )
 
    count_apres = df_clean.count()
    lignes_supprimees = count_avant - count_apres
 
    print(f"   Lignes avant : {count_avant}")
    print(f"   Lignes après : {count_apres}")
    print(f"   Lignes supprimées : {lignes_supprimees}")
 
    # Bonus : afficher les lignes supprimées pour le débogage.
    # except() retourne les lignes dans df mais PAS dans df_clean.
    # C'est l'équivalent d'un LEFT ANTI JOIN en SQL.
    if lignes_supprimees > 0:
        print("   ⚠️  Lignes avec nulls supprimées :")
        df.exceptAll(df_clean).show(truncate=False)
 
    return df_clean
