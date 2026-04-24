# Pipeline ETL PySpark — pyspark_etl_project
 
## Description
Pipeline de traitement de données de ventes construit avec Apache PySpark.
Ce projet illustre les bonnes pratiques de versionnage Git avec Git Flow.
 
## Architecture
```
pyspark_etl_project/
├── src/
│   ├── main.py            # Point d'entrée : orchestration du pipeline
│   └── transformations.py # Fonctions de transformation des données
├── data/
│   └── ventes.csv         # Données source de test
├── .gitignore
└── README.md
```
 
## Équipe
- Ouattara Honan Solo — Lead / DevOps
- Bob   — Data Engineer (feature/clean-nulls)
- Charlie — Data Analyst (feature/aggregate-sales)
 
## Lancer le pipeline
```bash
pip install pyspark
python src/main.py
```
