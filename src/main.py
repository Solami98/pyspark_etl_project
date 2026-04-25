# =============================================================
# main.py — Pipeline ETL PySpark FINAL
# =============================================================

from pyspark.sql import SparkSession

# Import des fonctions développées par l'équipe
from transformations import rename_and_cast, clean_nulls, aggregate_sales


def create_spark_session(app_name: str = "Pipeline_ETL_Ventes") -> SparkSession:
    """
    Crée une SparkSession.

    SparkSession = point d'entrée de PySpark.
    C'est l'équivalent d'une connexion à un moteur de calcul.
    """

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")  # utilise tous les cœurs CPU
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print(f"SparkSession démarrée — Spark {spark.version}")

    return spark


def load_csv(spark: SparkSession, path: str):
    """
    Charge un fichier CSV en DataFrame.

    Un DataFrame = table distribuée (comme SQL)
    """

    df = (
        spark.read
        .option("header", "true")       # première ligne = colonnes
        .option("inferSchema", "true")  # Spark devine les types
        .csv(path)
    )

    print(f"{df.count()} lignes chargées depuis {path}")

    return df


def main():
    """
    Pipeline ETL complet.

    ETL = Extract → Transform → Load
    """

    print("="*55)
    print("Pipeline ETL PySpark — v1.0.0")
    print("="*55)

    # ── EXTRACT ─────────────────────────────────────────
    spark = create_spark_session()

    df_brut = load_csv(spark, "data/ventes.csv")

    print("\nDonnées brutes :")
    df_brut.show()

    # ── TRANSFORM ───────────────────────────────────────
    # Étape 1 : nettoyage des noms + types
    print("\nÉtape 1 — Rename & Cast")
    df_renomme = rename_and_cast(df_brut)

    # Étape 2 : suppression des valeurs nulles
    print("\nÉtape 2 — Clean Nulls")
    df_propre = clean_nulls(df_renomme)

    # Étape 3 : agrégation
    print("\nÉtape 3 — Aggregation")
    df_final = aggregate_sales(df_propre)

    # ── LOAD ────────────────────────────────────────────
    print("\n" + "="*55)
    print("RÉSULTAT FINAL")
    print("="*55)

    df_final.show()

    # Affichage du schéma
    print("\nSchéma final :")
    df_final.printSchema()

    spark.stop()

    print("\nPipeline terminé avec succès !")


if __name__ == "__main__":
    main()