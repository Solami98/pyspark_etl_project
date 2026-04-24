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
    print("🔄 rename_and_cast : renommage des colonnes...")
 
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
