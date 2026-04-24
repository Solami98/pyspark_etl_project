# =============================================================
# main.py — Pipeline ETL PySpark FINAL (version intégrée)
# =============================================================
# Ce fichier orchestre l'exécution complète du pipeline :
#   1. Chargement des données CSV
#   2. Renommage et cast des types (Ouattara Solo)
#   3. Nettoyage des valeurs nulles (Konaté Moussa)
#   4. Agrégation des ventes par région (Kouassi Esdras)
# =============================================================
 
from pyspark.sql import SparkSession
 
# Import de TOUTES les transformations développées par l'équipe
# Le point '.' indique un import relatif (même dossier)
from transformations import rename_and_cast, clean_nulls, aggregate_sales
 
 
def create_spark_session(app_name: str = "Pipeline_ETL_Ventes") -> SparkSession:
    """Crée et retourne une SparkSession configurée (voir Jour 1)."""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print(f"✅ SparkSession démarrée — Spark {spark.version}")
    return spark
 
 
def load_csv(spark: SparkSession, path: str):
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("encoding", "utf-8")
        .option("multiLine", True)
        .option("escape", "\"")
        .csv(path)
    )
    print(f"{df.count()} lignes chargées depuis {path}")
    return df
 
 
def main():
    """
    Orchestration complète du pipeline ETL.
 
    Un pipeline ETL (Extract, Transform, Load) suit toujours
    le même schéma :
    - Extract : charger les données brutes depuis la source
    - Transform : nettoyer, enrichir, agréger les données
    - Load : sauvegarder ou afficher le résultat
 
    ORDRE IMPORTANT des transformations :
    1. rename_and_cast AVANT clean_nulls : les noms de colonnes
       utilisés dans clean_nulls doivent correspondre.
    2. clean_nulls AVANT aggregate_sales : on ne veut pas
       agréger des données corrompues.
    """
    print("="*55)
    print(" Pipeline ETL PySpark — v1.0.0")
    print("="*55)
 
    # ── EXTRACT ────────────────────────────────────────────
    spark = create_spark_session()
    df_brut = load_csv(spark, "data/ventes.csv")
 
    print("\nDonnées brutes :")
    df_brut.show(truncate=False)
 
    # ── TRANSFORM ──────────────────────────────────────────
    print("\n" + "─"*40)
    print(" ÉTAPE 1/3 — Renommage & Cast (Ouattara Solo)")
    print("─"*40)
    df_renomme = rename_and_cast(df_brut)
    df_renomme.show(truncate=False)
 
    print("\n" + "─"*40)
    print(" ÉTAPE 2/3 — Nettoyage des nulls (Konaté Moussa)")
    print("─"*40)
    # Après rename_and_cast, les colonnes s'appellent désormais
    # 'region_vente', 'nom_produit', 'montant_vente'.
    # On adapte clean_nulls pour utiliser les nouveaux noms.
    df_propre = df_renomme.dropna(
        how="any",
        subset=["region_vente", "nom_produit", "montant_vente"]
    )
    print(f"   {df_renomme.count() - df_propre.count()} ligne(s) supprimée(s)")
    df_propre.show(truncate=False)
 
    print("\n" + "─"*40)
    print(" ÉTAPE 3/3 — Agrégation (Esdras Kouassi)")
    print("─"*40)
    # On adapte l'agrégation aux nouveaux noms de colonnes
    from pyspark.sql import functions as F
    df_final = (
        df_propre
        .groupBy("region_vente")
        .agg(
            F.sum("montant_vente").alias("total_ventes"),
            F.round(F.avg("montant_vente"), 2).alias("moyenne_ventes"),
            F.max("montant_vente").alias("vente_max"),
            F.count("montant_vente").alias("nb_transactions"),
        )
        .orderBy(F.desc("total_ventes"))
    )
 
    # ── LOAD ───────────────────────────────────────────────
    print("\n" + "="*55)
    print(" ✅ RÉSULTATS FINAUX DU PIPELINE")
    print("="*55)
    df_final.show(truncate=False)
 
    # Afficher le schéma final pour validation
    print("\nSchéma du DataFrame final :")
    df_final.printSchema()
 
    spark.stop()
    print("\nPipeline v1.0.0 terminé avec succès !")
 
 
if __name__ == "__main__":
    main()
