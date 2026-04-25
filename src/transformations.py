# =============================================================
# transformations.py — Pipeline ETL Ventes v1.0
# Auteurs : Ouattara Solo, Konaté Moussa, Kouassi Esdras
# =============================================================

# Import du type DataFrame (structure principale en PySpark)
from pyspark.sql import DataFrame

# Import des fonctions Spark SQL (sum, avg, col, etc.)
# Convention : on utilise l'alias F pour simplifier l'écriture
from pyspark.sql import functions as F

# Import des types de données pour faire des conversions explicites
from pyspark.sql.types import DoubleType, DateType


# ─────────────────────────────────────────────────────────
# FONCTION 1 — rename_and_cast (Ouattara Solo)
# ─────────────────────────────────────────────────────────
def rename_and_cast(df: DataFrame) -> DataFrame:
    """
    Cette fonction prépare les données brutes pour le pipeline.

    Elle réalise 2 opérations importantes :
    1. Renommer les colonnes (meilleure lisibilité)
    2. Convertir les types (éviter erreurs de calcul)

    IMPORTANT :
    Spark ne modifie jamais le DataFrame original.
    Chaque transformation retourne un NOUVEAU DataFrame.
    """

    print("rename_and_cast...")

    # ── Étape 1 : renommage des colonnes ───────────────────
    # withColumnRenamed permet de changer le nom d'une colonne.
    # On passe d'un nom brut à un nom plus explicite.
    df = (
        df
        .withColumnRenamed("region", "region_vente")
        .withColumnRenamed("produit", "nom_produit")
        .withColumnRenamed("ventes", "montant_vente")
        .withColumnRenamed("date", "date_vente")
    )

    # ── Étape 2 : conversion des types ─────────────────────
    # cast() permet de transformer le type d'une colonne
    # Exemple : string → double (nombre)
    df = (
        df
        # Conversion du montant en nombre décimal
        .withColumn("montant_vente", F.col("montant_vente").cast(DoubleType()))

        # Conversion de la date en type Date
        .withColumn("date_vente", F.col("date_vente").cast(DateType()))
    )

    return df


# ─────────────────────────────────────────────────────────
# FONCTION 2 — clean_nulls (Konaté Moussa)
# ─────────────────────────────────────────────────────────
def clean_nulls(df: DataFrame) -> DataFrame:
    """
    Cette fonction supprime les lignes contenant des valeurs nulles.

    Pourquoi ?
    En Spark, les nulls sont dangereux :
    - Ils peuvent casser les calculs
    - Exemple : 1000 + NULL = NULL

    Stratégie :
    On supprime toute ligne qui contient un null
    dans les colonnes critiques.
    """

    print("clean_nulls...")

    # Nombre de lignes avant nettoyage
    count_avant = df.count()

    # Colonnes essentielles pour notre analyse
    colonnes_critiques = ["region_vente", "nom_produit", "montant_vente"]

    # dropna supprime les lignes contenant des valeurs nulles
    # subset = on ne regarde que ces colonnes
    df_clean = df.dropna(subset=colonnes_critiques)

    # Nombre de lignes après nettoyage
    count_apres = df_clean.count()

    print(f"Lignes supprimées : {count_avant - count_apres}")

    return df_clean


# ─────────────────────────────────────────────────────────
# FONCTION 3 — aggregate_sales (Kouassi Esdras)
# ─────────────────────────────────────────────────────────
def aggregate_sales(df: DataFrame) -> DataFrame:
    """
    Cette fonction regroupe les données par région
    et calcule des statistiques.

    Concept clé : groupBy + agg

    - groupBy("colonne") → crée des groupes
    - agg() → applique des calculs sur chaque groupe

    Résultat :
    1 ligne par région
    """

    print("aggregate_sales...")

    df_agrege = (
        df

        # groupBy : on regroupe les lignes par région
        .groupBy("region_vente")

        # agg : on applique plusieurs calculs
        .agg(
            # Somme totale des ventes
            F.sum("montant_vente").alias("total_ventes"),

            # Moyenne des ventes
            F.round(F.avg("montant_vente"), 2).alias("moyenne_ventes"),

            # Vente maximale
            F.max("montant_vente").alias("vente_max"),

            # Nombre de transactions
            F.count("montant_vente").alias("nb_transactions"),
        )

        # Tri décroissant
        .orderBy(F.desc("total_ventes"))
    )

    return df_agrege