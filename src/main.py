# =============================================================
# main.py — Point d'entrée du pipeline ETL PySpark
# =============================================================
# Ce fichier est le chef d'orchestre : il initialise Spark,
# charge les données brutes, appelle les fonctions de
# transformation dans l'ordre, et affiche le résultat final.
# =============================================================
 
# PySpark est la bibliothèque Python d'Apache Spark.
# SparkSession est le point d'entrée unique vers toutes les
# fonctionnalités Spark depuis la version 2.x.
from pyspark.sql import SparkSession
 
# On importe les transformations définies par l'équipe.
# Ces imports seront complétés au Jour 3 lors de l'intégration.
# from transformations import clean_nulls, aggregate_sales, rename_columns
 
 
def create_spark_session(app_name: str = "Pipeline_ETL_Ventes") -> SparkSession:
    """
    Crée et retourne une SparkSession configurée.
 
    Une SparkSession est le point d'entrée vers Spark.
    'builder' utilise le pattern Builder pour configurer Spark
    de façon déclarative avant de le démarrer.
 
    Args:
        app_name: Nom affiché dans l'interface Spark UI (port 4040).
 
    Returns:
        Une SparkSession prête à l'emploi.
    """
    spark = (
        SparkSession.builder
        # Nom de l'application visible dans Spark UI
        .appName(app_name)
        # 'local[*]' = mode local, utilise tous les cœurs CPU disponibles.
        # En production, on indiquerait l'URL du cluster Spark ici.
        .master("local[*]")
        # Réduit la verbosité des logs Spark (trop bavard par défaut)
        .config("spark.sql.shuffle.partitions", "2")
        # Crée la session (ou récupère une existante)
        .getOrCreate()
    )
 
    # Définir le niveau de log sur WARN pour n'afficher que l'essentiel.
    # Les niveaux disponibles sont : ALL, DEBUG, INFO, WARN, ERROR, FATAL, OFF
    spark.sparkContext.setLogLevel("WARN")
 
    print(f"✅ SparkSession créée : {spark.version}")
    return spark
 
 
def load_csv(spark: SparkSession, path: str):
    """
    Charge un fichier CSV dans un DataFrame Spark.
 
    Un DataFrame Spark est une table distribuée en mémoire,
    similaire à un DataFrame pandas mais capable de traiter
    des téraoctets de données sur un cluster.
 
    Args:
        spark: La SparkSession active.
        path:  Chemin vers le fichier CSV.
 
    Returns:
        Un DataFrame Spark contenant les données du CSV.
    """
    df = (
        spark.read
        # header=True : la 1ère ligne contient les noms de colonnes
        .option("header", "true")
        # inferSchema=True : Spark devine automatiquement les types
        # (string, int, double…) en lisant les données. Plus lent mais
        # pratique. En production, on préférerait définir le schéma.
        .option("inferSchema", "true")
        .option("encoding", "UTF-8")
        .csv(path)
    )
 
    # count() est une action qui déclenche vraiment le calcul.
    # Les transformations Spark sont « lazy » : elles ne s'exécutent
    # que quand une action (count, show, write…) est appelée.
    print(f"📂 Données chargées : {df.count()} lignes, {len(df.columns)} colonnes")
    return df
 
 
def main():
    """Fonction principale : orchestre l'exécution du pipeline."""
    print("="*55)
    print(" 🚀 Démarrage du Pipeline ETL PySpark")
    print("="*55)
 
    # Étape 1 : Initialiser Spark
    spark = create_spark_session()
 
    # Étape 2 : Charger les données brutes
    df_raw = load_csv(spark, "data/ventes.csv")
 
    # Étape 3 : Afficher un aperçu des données brutes
    print("\n📊 Aperçu des données brutes :")
    # show() affiche les n premières lignes sous forme de tableau ASCII.
    # truncate=False évite de couper les valeurs longues.
    df_raw.show(10, truncate=False)
 
    # Étape 4 : Afficher le schéma inféré
    print("\n🗂️  Schéma inféré automatiquement par Spark :")
    # printSchema() montre les types de chaque colonne.
    # C'est essentiel pour déboguer les erreurs de type.
    df_raw.printSchema()
 
    print("\n✅ Pipeline Jour 1 terminé avec succès !")
    print("   Les transformations seront ajoutées au Jour 3.")
 
    # Toujours arrêter la SparkSession proprement à la fin.
    spark.stop()
 
 
# Point d'entrée Python standard.
# Ce bloc ne s'exécute que si on lance ce fichier directement
# (pas si on l'importe depuis un autre module).
if __name__ == "__main__":
    main()
