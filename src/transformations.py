# VERSION RÉSOLUE (combinaison des deux) :
# =============================================================
# transformations.py — Pipeline ETL Ventes v1.0
# Auteurs : Ouattara Solo, Esdras Kouassi, Konate Moussa | Équipe Data Engineering
# Version : 1.0.0 | Date : 2024-01-20
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

# VERSION D'ALICE (dans feature/rename-columns) :
# =============================================================
# transformations.py — Pipeline ETL Ventes v1.0
# Auteurs : Ouattara Solo, Esdras Kouassi, Konaté Moussa | Équipe Data Engineering
# =============================================================


# =============================================================
# transformations.py — Bibliothèque de transformations PySpark
# =============================================================
# Ce fichier centralise toutes les fonctions de transformation
# de données du pipeline. Chaque fonction prend un DataFrame
# en entrée et retourne un DataFrame transformé.
#
# Convention : les fonctions ne modifient JAMAIS le DataFrame
# original (immutabilité). Elles retournent toujours un
# NOUVEAU DataFrame — principe fondamental de Spark.
# =============================================================
 
# DataFrame et fonctions Spark SQL
from pyspark.sql import DataFrame
 
# 'functions' contient toutes les fonctions intégrées de Spark :
# col(), lit(), when(), coalesce(), cast()…
# Convention courante : importer sous l'alias 'F'
from pyspark.sql import functions as F
 
# Types de données Spark pour le cast explicite
from pyspark.sql.types import DoubleType, DateType, StringType

  
# ─────────────────────────────────────────────────────────
# FONCTION 1 (Ouattara Honan Solo) : rename_and_cast
# ─────────────────────────────────────────────────────────
def rename_and_cast(df: DataFrame) -> DataFrame:
    """
    Renomme les colonnes pour respecter la convention snake_case
    et convertit les types de données vers des types appropriés.
 
    POURQUOI cette étape est-elle importante ?
    Quand on charge un CSV avec inferSchema, Spark fait de son
    mieux pour deviner les types mais se trompe parfois
    (ex: une date lue comme string). Cette fonction corrige ça.
 
    Args:
        df: DataFrame brut chargé depuis le CSV.
 
    Returns:
        DataFrame avec colonnes renommées et types corrigés.
    """
    print("rename_and_cast : renommage des colonnes...")
 
    # withColumnRenamed(ancien_nom, nouveau_nom) renomme une colonne.
    # On peut enchaîner plusieurs appels (méthode fluent / chainable).
    # Spark ne recalcule rien ici (lazy evaluation) : il note
    # juste la transformation dans le plan d'exécution.
    df_renamed = (
        df
        .withColumnRenamed("region",  "region_vente")
        .withColumnRenamed("produit", "nom_produit")
        .withColumnRenamed("ventes",  "montant_vente")
        .withColumnRenamed("date",    "date_vente")
    )
 
    # withColumn(nom_colonne, nouvelle_expression) REMPLACE une colonne
    # existante ou en CRÉE une nouvelle si le nom n'existe pas.
    #
    # col("montant_vente") : référence la colonne par son nom
    # .cast(DoubleType()) : convertit en nombre décimal (Double 64-bit)
    # Pourquoi Double et pas Integer ? Les ventes peuvent avoir des centimes.
    df_typed = (
        df_renamed
        # Cast du montant en nombre décimal
        .withColumn("montant_vente",
                    F.col("montant_vente").cast(DoubleType()))
        # Cast de la date : from string 'YYYY-MM-DD' vers type DateType
        # Spark reconnaît automatiquement ce format ISO 8601
        .withColumn("date_vente",
                    F.col("date_vente").cast(DateType()))
    )
 
    print(f"   Colonnes après renommage : {df_typed.columns}")
    return df_typed


