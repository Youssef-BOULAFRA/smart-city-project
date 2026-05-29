import os
import csv
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, hour, col, avg, count as _count, max as _max, min as _min, sum as _sum, when
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from dotenv import load_dotenv

# ============================================
# 1. Configuration et Initialisation
# ============================================

# Charger les variables d'environnement (compte de stockage, clé)
env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'iot_simulation', '.env')
load_dotenv(env_path)

STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME", "smartcitylake")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")
LOG_FILE = os.path.join(os.path.dirname(__file__), "batch_logs.csv")

if not STORAGE_ACCOUNT_KEY:
    print("⚠️ Attention : STORAGE_ACCOUNT_KEY non trouvée dans le fichier .env.")

# Création de la session Spark avec Delta
builder = SparkSession.builder \
    .appName("SmartCityBatchETL") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config(f"fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net", STORAGE_ACCOUNT_KEY)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ============================================
# 2. Logique ETL
# ============================================

def process_sensor_data(sensor_type, id_columns, dropna_columns):
    """
    Lit les données brutes d'un capteur, nettoie et écrit en Delta Lake.
    """
    print(f"--- Début du traitement pour : {sensor_type} ---")
    start_time = datetime.now()
    status = "SUCCESS"
    rows_processed = 0

    raw_path = f"abfss://raw@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{sensor_type}/*"
    processed_path = f"abfss://processed@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{sensor_type}/"
    aggregated_path = f"abfss://aggregated@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{sensor_type}/"

    try:
        # Vérifier si le chemin raw existe/contient des données (gestion d'erreur Spark)
        try:
            df_raw = spark.read.json(raw_path)
            # S'il n'y a pas de données, df_raw sera vide ou l'inférence de schéma échouera
            if df_raw.rdd.isEmpty():
                print(f"Aucune donnée trouvée pour {sensor_type}.")
                status = "SKIPPED_NO_DATA"
                return status, rows_processed
        except Exception as read_err:
            print(f"Information: Impossible de lire depuis {raw_path}. Dossier potentiellement vide.")
            status = "SKIPPED_NO_DATA"
            return status, rows_processed

        # Nettoyage et Enrichissement
        df_clean = df_raw.dropDuplicates(id_columns) \
                         .dropna(subset=dropna_columns) \
                         .withColumn('date_partition', to_date('timestamp')) \
                         .withColumn('heure', hour('timestamp'))

        df_new = filter_new_rows(df_clean, processed_path, id_columns)

        # On compte les lignes à écrire
        rows_processed = df_new.count()
        print(f"Lignes à écrire pour {sensor_type} : {rows_processed}")

        if rows_processed > 0:
            # Écriture en mode append avec partitionnement
            df_new.write \
                .format("delta") \
                .mode("append") \
                .partitionBy("date_partition", "zone") \
                .save(processed_path)
            print(f"✅ Données {sensor_type} écrites avec succès dans {processed_path}")

            update_aggregated_snapshot(sensor_type, processed_path, aggregated_path)
        else:
            print(f"Aucune nouvelle ligne à écrire pour {sensor_type}.")

    except Exception as e:
        status = f"FAILED: {str(e)[:100]}"
        print(f"❌ Erreur lors du traitement de {sensor_type}: {e}")

    finally:
        # Logging
        end_time = datetime.now()
        log_execution(start_time, end_time, sensor_type, status, rows_processed)

    return status, rows_processed


def filter_new_rows(df_clean, processed_path, id_columns):
    """Retire les lignes déjà présentes dans la couche processed pour garder un traitement idempotent."""
    try:
        if DeltaTable.isDeltaTable(spark, processed_path):
            existing_keys = spark.read.format("delta").load(processed_path).select(*id_columns).dropDuplicates(id_columns)
            return df_clean.join(existing_keys, on=id_columns, how="left_anti")
    except Exception:
        pass

    return df_clean


def update_aggregated_snapshot(sensor_type, processed_path, aggregated_path):
    """Construit une vue agrégée persistée dans la zone aggregated."""
    try:
        if not DeltaTable.isDeltaTable(spark, processed_path):
            return

        df_history = spark.read.format("delta").load(processed_path)
        if df_history.rdd.isEmpty():
            return

        if sensor_type == "pollution":
            df_agg = df_history.groupBy("date_partition", "zone").agg(
                _count("*").alias("nb_mesures"),
                avg("co2_ppm").alias("avg_co2_ppm"),
                avg("pm25").alias("avg_pm25"),
                avg("air_quality_index").alias("avg_aqi"),
                _max("co2_ppm").alias("max_co2_ppm"),
                _max("air_quality_index").alias("max_aqi")
            )
        elif sensor_type == "traffic":
            df_agg = df_history.groupBy("date_partition", "zone").agg(
                _count("*").alias("nb_mesures"),
                avg("vehicles_per_min").alias("avg_vehicles_per_min"),
                avg("avg_speed_kmh").alias("avg_speed_kmh"),
                _max("vehicles_per_min").alias("max_vehicles_per_min"),
                _min("avg_speed_kmh").alias("min_speed_kmh")
            )
        elif sensor_type == "eclairage":
            df_agg = df_history.groupBy("date_partition", "zone").agg(
                _count("*").alias("nb_mesures"),
                avg("luminosity_lux").alias("avg_luminosity_lux"),
                avg("power_kw").alias("avg_power_kw"),
                _sum(when(col("status") == "faulty", 1).otherwise(0)).alias("nb_faulty")
            )
        elif sensor_type == "dechets":
            df_agg = df_history.groupBy("date_partition", "zone").agg(
                _count("*").alias("nb_mesures"),
                avg("fill_percent").alias("avg_fill_percent"),
                avg("weight_kg").alias("avg_weight_kg"),
                _max("fill_percent").alias("max_fill_percent"),
                _max("weight_kg").alias("max_weight_kg")
            )
        else:
            return

        df_agg.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("date_partition", "zone") \
            .save(aggregated_path)

        print(f"✅ Vue agrégée mise à jour dans {aggregated_path}")
    except Exception as e:
        print(f"⚠️ Impossible de mettre à jour la couche aggregated pour {sensor_type}: {e}")

def log_execution(start_time, end_time, sensor_type, status, rows_processed):
    """ Écrit les métriques d'exécution dans le fichier CSV. """
    file_exists = os.path.isfile(LOG_FILE)
    with open(LOG_FILE, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(['timestamp', 'sensor_type', 'status', 'rows_processed', 'duration_seconds'])
        
        duration = (end_time - start_time).total_seconds()
        writer.writerow([end_time.isoformat(), sensor_type, status, rows_processed, round(duration, 2)])

# ============================================
# 3. Exécution
# ============================================

if __name__ == "__main__":
    print(f"Début du Batch ETL à {datetime.now().isoformat()}")
    
    # Traitement des 4 types de capteurs
    # (type, colonnes_pour_deduplication, colonnes_qui_ne_doivent_pas_etre_nulles)
    sensors = [
        ("pollution", ["timestamp", "device_id"], ["co2_ppm", "pm25"]),
        ("traffic", ["timestamp", "device_id"], ["vehicles_per_min", "avg_speed_kmh"]),
        ("eclairage", ["timestamp", "device_id"], ["luminosity_lux"]),
        ("dechets", ["timestamp", "device_id"], ["fill_percent"])
    ]

    for s_type, id_cols, notnull_cols in sensors:
        process_sensor_data(s_type, id_cols, notnull_cols)

    print("Fin du Batch ETL.")
    spark.stop()
