# ─────────────────────────────────────────────────────────
# FONCTION 3 (Charlie) : aggregate_sales
# ─────────────────────────────────────────────────────────
def aggregate_sales(df: DataFrame) -> DataFrame:
    """
    Calcule des statistiques agrégées des ventes par région.
 
    CONCEPT CLÉ — Le groupBy/agg en Spark :
    C'est l'équivalent du GROUP BY en SQL.
    groupBy(colonne) crée des groupes de lignes partageant
    la même valeur pour cette colonne.
    agg() calcule ensuite des fonctions d'agrégation sur chaque
    groupe : somme, moyenne, max, min, count...
 
    IMPORTANT : Après groupBy, le DataFrame résultant a
    UNE LIGNE PAR VALEUR UNIQUE de la colonne groupée.
 
    Args:
        df: DataFrame nettoyé (idéalement après clean_nulls).
 
    Returns:
        DataFrame agrégé : une ligne par région avec les stats.
    """
    print("📊 aggregate_sales : agrégation des ventes par région...")
 
    df_agrege = (
        df
        # groupBy(colonne) : groupe les lignes par valeur unique.
        # Ici on veut UNE LIGNE RÉSUMÉE par région.
        .groupBy("region")
 
        # agg() accepte plusieurs fonctions d'agrégation à la fois.
        # Chaque fonction retourne une valeur par groupe.
        .agg(
            # Somme totale des ventes pour cette région
            F.sum("ventes").alias("total_ventes"),
 
            # Moyenne des ventes (utile pour détecter les outliers)
            F.avg("ventes").alias("moyenne_ventes"),
 
            # Vente maximum (identifier le meilleur jour)
            F.max("ventes").alias("vente_max"),
 
            # Nombre de transactions dans cette région
            F.count("ventes").alias("nb_transactions"),
        )
 
        # Trier par total décroissant : la meilleure région en premier.
        # F.desc() = ordre décroissant (DESC en SQL).
        # F.asc()  = ordre croissant  (ASC en SQL).
        .orderBy(F.desc("total_ventes"))
    )
 
    # round() arrondit à 2 décimales pour l'affichage.
    # withColumn remplace la colonne existante par sa version arrondie.
    df_agrege = df_agrege.withColumn(
        "moyenne_ventes",
        F.round(F.col("moyenne_ventes"), 2)
    )
 
    print("   Résultats de l'agrégation :")
    df_agrege.show(truncate=False)
    return df_agrege
