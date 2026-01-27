import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

## Clear console
os.system('cls' if os.name == 'nt' else 'clear')

## Add Title
print("=" * 100)
print(" " * 10 + "TP - Analyse de la qualité de l'air - Etape 2: Nettoyage Spark et agrégation" + " " * 10)
print("=" * 100 + "\n")

## Creation d'une session spark
try:
    spark = SparkSession.builder \
        .appName("TP - Analyse de la qualité de l'air - Etape 2: Nettoyage Spark et agrégation") \
        .master("local[*]") \
        .getOrCreate()
    print(f"\n[ok]: Spark local session creation successful.\n")
except Exception as e:
    print(f"\n[ko]: Spark local session creation failed: {e}\n")
    sys.exit(1)

spark.sparkContext.setLogLevel("ERROR") # keep only error+

## Chargement du fichier
data_file_air_quality = "./data/air_quality_raw.csv"
try:
    df_air = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("sep", ",") \
            .csv(data_file_air_quality)
    print(f"\n[ok]: read source data file successfull: '{data_file_air_quality}'\n")
except Exception as e:
    print(f"\n[ko]: read source data file failed: {e}\n")
    sys.exit(1)

## Show initial df_air:
print("─" * 70)
print("df_air initial:")
print("─" * 70)
df_air.show()

## Show initial schema of df_air
print("─" * 70)
print("initial schema of df_air:")
print("─" * 70)
df_air.printSchema()

#--------------------------------------------------
# Etape 2 – Nettoyage Spark et agrégation
#--------------------------------------------------

## Parser les timestamps multi-formats avec une UDF Spark:
df_air = df_air.withColumn(
    "timestamp",
    F.coalesce(
        F.to_timestamp("timestamp", "MM/dd/yyyy HH:mm:ss"),   # e.g. 05/14/2024 04:00:00
        F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss"), # e.g. 2024-06-05T01:00:00
        F.to_timestamp("timestamp", "dd/MM/yyyy HH:mm"),       # e.g. 18/03/2024 12:00
        F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss")    # e.g. 2024-05-23 11:00:00 (to keep the right format)
    )
)

## Convertir les valeurs avec virgule décimale en float:
df_air = df_air.withColumn(
    "value",
    F.regexp_replace("value", ",", ".").cast(T.FloatType())
)

print("─" * 70)
print("Parser les timestamps multi-formats avec une UDF Spark")
print("&")
print("Convertir les valeurs avec virgule décimale en float")
print("=> Nouvelle schema & Nouvelle df_air :")
print("─" * 70)
df_air.printSchema()
df_air.show()

## Supprimer les valeurs négatives et les outliers (>1000 ug/m3):
print("─" * 70)
print("Supprimer les valeurs négatives et les outliers (>1000 ug/m3):")
print("─" * 70)

# Supprimer les valeurs négatives
df_air.filter(F.col("value") < 0) # probleme
df_air = df_air.filter(F.col("value") >= 0) # solution
# df_air.show()

# Supprimer les outliers (>1000 ug/m3)
df_air.filter(F.col("value") > 1000) # problem
df_air = df_air.filter(F.col("value") <= 1000) # solution
df_air.show()

## Dédupliquer sur `(station_id, timestamp, pollutant)`:
print("─" * 70)
print("Dédupliquer sur `(station_id, timestamp, pollutant)`:")
print("─" * 70)

nbr_enregistrements_init = df_air.count()

# # before drop duplicates
# dup_keys = (
#     df_air
#     .groupBy("station_id", "timestamp", "pollutant")
#     .count()
#     .filter("count > 1")
# ).show()

# ==> drop duplicates
df_air = df_air.dropDuplicates(
    ["station_id", "timestamp", "pollutant"]
)
df_air.show()
# # after drop duplicates
# dup_keys = (
#     df_air
#     .groupBy("station_id", "timestamp", "pollutant")
#     .count()
#     .filter("count > 1")
# ).show()
print("- Nombre d'enregistrement avant déduplication:", nbr_enregistrements_init)
print("- Nombre d'enregistrement après déduplication:", df_air.count())
print("- Nombre de lignes supprimées:", nbr_enregistrements_init - df_air.count()) # 23773

## Calculer les moyennes horaires par station et polluant:
print("─" * 70)
print("Calculer les moyennes horaires par station et polluant:")
print("─" * 70)

# 1. convert timestamp to long (seconds since epoch)
df_air = df_air.withColumn("ts_long", F.col("timestamp").cast("long"))

# 2. group and compute average in seconds
gp_st_pl = df_air.groupBy("station_id", "pollutant").agg(
    F.avg("ts_long").alias("avg_ts_long")
)

# 3. convert back to timestamp
gp_st_pl = gp_st_pl.withColumn(
    "avg_timestamp",
    F.from_unixtime("avg_ts_long").cast(T.TimestampType())
)

# 4. show result
gp_st_pl.select("station_id", "pollutant", "avg_timestamp").show(truncate=False)

## Sauvegarder en Parquet partitionné par `date`:
print("─" * 70)
print("Sauvegarder en Parquet partitionné par `date`:")
print("─" * 70)

# try:
#     # extraire la date yyyy-MM-dd
#     df_air = df_air.withColumn("date", F.to_date("timestamp"))
#     # df_air.show()

#     output_path = "./output/air_quality_clean"

#     df_air.write.mode("overwrite").partitionBy("date").parquet(output_path)
#     # mode("overwrite"): écrase si le dossier existe
#     # partitionBy("date"): partition par date
# except Exception as e:
#     print("[KO]: erreur lors de la sauvegarde en parquet partitionné par `date`:", e)








## Release resources 
spark.stop()
# Force kill the Py4J gateway (Windows-safe)
spark.sparkContext._gateway.shutdown()
print("─" * 70)
print("[info]: entire spark session is stopped successfully.\n")
# print("─" * 70)
os._exit(0)

