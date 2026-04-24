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

 
