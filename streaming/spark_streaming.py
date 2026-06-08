# ── 1. Environnement Windows ────────────────────────────────
import sys, os
from pyspark.sql.functions import to_timestamp
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from spark_env import get_jars_string, add_streaming_to_path
add_streaming_to_path()

jars = get_jars_string()

# ── 2. Imports ──────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, when, lit, sum as _sum
)
# plus besoin de TimestampType, on utilise to_timestamp
# from pyspark.sql.types import TimestampType   # commenté car inutile
# from configuration.config import KAFKA_BOOTSTRAP
# print(f"🔧 KAFKA_BOOTSTRAP from config = {KAFKA_BOOTSTRAP}")
from configuration.config import (
    EVENT_HUB_CONNECTION_STRING_SEND,
    EVENT_HUB_CONNECTION_STRING_LISTEN,
    KAFKA_BOOTSTRAP_READ,
    KAFKA_BOOTSTRAP_WRITE,
    CHECKPOINT_DIR,
    TOPIC_POLLUTION, TOPIC_TRAFFIC, TOPIC_ECLAIRAGE, TOPIC_DECHETS,
    TOPIC_ALERTS,
    # Pollution
    SEUIL_CO2_ORANGE_MIN, SEUIL_CO2_ROUGE_MIN,
    SEUIL_PM25_ORANGE_MIN, SEUIL_PM25_ROUGE_MIN,
    SEUIL_AQI_ORANGE_MIN, SEUIL_AQI_ROUGE_MIN,
    # Trafic
    SEUIL_VEHICULES_ORANGE_MIN, SEUIL_VEHICULES_ROUGE_MIN,
    SEUIL_VITESSE_ORANGE_MAX, SEUIL_VITESSE_ROUGE_MAX,
    # Déchets
    SEUIL_REMPLISSAGE_ORANGE_MIN, SEUIL_REMPLISSAGE_ROUGE_MIN,
    SEUIL_POIDS_ORANGE_MIN, SEUIL_POIDS_ROUGE_MIN,
    # Éclairage
    SEUIL_LUMINOSITE_ORANGE_MAX, SEUIL_LUMINOSITE_ROUGE_MAX,
    SEUIL_CONSO_ORANGE_MIN, SEUIL_CONSO_ROUGE_MIN,
)
from utils.utils import (
    get_pollution_schema, get_traffic_schema,
    get_eclairage_schema, get_dechets_schema
)
from writers.influxdb_writer import write_batch_to_influx
from writers.kafka_alert_writer import write_alerts_to_kafka

# ── 3. Spark Session ────────────────────────────────────────
spark = SparkSession.builder \
    .appName("SmartCity_SparkStreaming_P3") \
    .master("local[*]") \
    .config("spark.jars", jars) \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.ui.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("  Smart City — Spark Streaming P3")
print("  Speed Layer opérationnel")
print(f"  Kafka        : {KAFKA_BOOTSTRAP_READ}")
print(f"  Checkpoint   : {CHECKPOINT_DIR}")
print("=" * 60)


# ══════════════════════════════════════════════════════════════
# HELPER — construit les deux niveaux de fenêtres + alertes
# ══════════════════════════════════════════════════════════════
def build_alert_pipeline(df_parsed, agg_exprs, alert_fn,
                         type_label, valeur_col):
    # Fenêtre 5 min (alertes)
    df_5min = df_parsed \
        .withWatermark("event_time", "5 minutes") \
        .groupBy(
            window("event_time", "5 minutes", "1 minute"),
            "zone"
        ).agg(*agg_exprs)

    df_alerts = alert_fn(df_5min) \
        .withColumn("type_capteur", lit(type_label)) \
        .withColumn("valeur_detectee", col(valeur_col))

    df_alerts_filtered = df_alerts.filter(col("alert_level") != "VERT")

    # Fenêtre 15 min (tendances)
    df_15min = df_parsed \
        .withWatermark("event_time", "15 minutes") \
        .groupBy(
            window("event_time", "15 minutes", "5 minutes"),
            "zone"
        ).agg(*agg_exprs) \
        .withColumn("type_capteur", lit(type_label)) \
        .withColumn("valeur_detectee", col(valeur_col)) \
        .withColumn("alert_level", lit("TENDANCE"))

    return df_alerts_filtered, df_15min

# Options pour les consumers (lecture)
kafka_read_options = {
    "kafka.bootstrap.servers": KAFKA_BOOTSTRAP_READ,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": f'org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{EVENT_HUB_CONNECTION_STRING_LISTEN}";',
    "startingOffsets": "latest"
}

# Helper pour lire un topic
def read_kafka_topic(topic):
    return spark.readStream.format("kafka") \
        .options(**kafka_read_options) \
        .option("subscribe", topic) \
        .load()

# ══════════════════════════════════════════════════════════════
#  CAPTEUR 1 — POLLUTION
# ══════════════════════════════════════════════════════════════
print("\n[1/4] Initialisation stream POLLUTION...")

df_poll_raw = read_kafka_topic(TOPIC_POLLUTION)

df_poll = df_poll_raw \
    .select(from_json(col("value").cast("string"),
                      get_pollution_schema()).alias("d")) \
    .select("d.*") \
    .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"))

def pollution_alert_fn(df):
    return df.withColumn("alert_level",
        when(
            (col("avg_co2")  >= SEUIL_CO2_ROUGE_MIN)  |
            (col("avg_pm25") >= SEUIL_PM25_ROUGE_MIN)  |
            (col("avg_aqi")  >= SEUIL_AQI_ROUGE_MIN),
            "ROUGE"
        ).when(
            (col("avg_co2")  >= SEUIL_CO2_ORANGE_MIN)  |
            (col("avg_pm25") >= SEUIL_PM25_ORANGE_MIN) |
            (col("avg_aqi")  >= SEUIL_AQI_ORANGE_MIN),
            "ORANGE"
        ).otherwise("VERT")
    ).withColumn("alert_message",
        when(col("alert_level") == "ROUGE",
             "CRITIQUE pollution - CO2 eleve 5min - Service Environnement")
        .when(col("alert_level") == "ORANGE",
              "Elevation pollution - Surveillance renforcee")
        .otherwise("Normal")
    )

df_poll_alerts, df_poll_15min = build_alert_pipeline(
    df_poll,
    [avg("co2_ppm").alias("avg_co2"),
     avg("pm25").alias("avg_pm25"),
     avg("air_quality_index").alias("avg_aqi")],
    pollution_alert_fn,
    "pollution", "avg_co2"
)


# ══════════════════════════════════════════════════════════════
#  CAPTEUR 2 — TRAFIC
# ══════════════════════════════════════════════════════════════
print("[2/4] Initialisation stream TRAFIC...")

df_traf_raw = read_kafka_topic(TOPIC_TRAFFIC)

df_traf = df_traf_raw \
    .select(from_json(col("value").cast("string"),
                      get_traffic_schema()).alias("d")) \
    .select("d.*") \
    .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"))

def traffic_alert_fn(df):
    return df.withColumn("alert_level",
        when(
            (col("avg_vehicules") >= SEUIL_VEHICULES_ROUGE_MIN) |
            (col("avg_vitesse")   <= SEUIL_VITESSE_ROUGE_MAX),
            "ROUGE"
        ).when(
            (col("avg_vehicules") >= SEUIL_VEHICULES_ORANGE_MIN) |
            (col("avg_vitesse")   <= SEUIL_VITESSE_ORANGE_MAX),
            "ORANGE"
        ).otherwise("VERT")
    ).withColumn("alert_message",
        when(col("alert_level") == "ROUGE",
             "Chaos routier soutenu 5min - Notification Police municipale")
        .when(col("alert_level") == "ORANGE",
              "Embouteillage - Ralentissement detecte")
        .otherwise("Circulation normale")
    )

df_traf_alerts, df_traf_15min = build_alert_pipeline(
    df_traf,
    [avg("vehicles_per_min").alias("avg_vehicules"),
     avg("avg_speed_kmh").alias("avg_vitesse")],
    traffic_alert_fn,
    "traffic", "avg_vehicules"
)


# ══════════════════════════════════════════════════════════════
#  CAPTEUR 3 — ÉCLAIRAGE
# ══════════════════════════════════════════════════════════════
print("[3/4] Initialisation stream ECLAIRAGE...")

df_ecl_raw = read_kafka_topic(TOPIC_ECLAIRAGE)

df_ecl = df_ecl_raw \
    .select(from_json(col("value").cast("string"),
                      get_eclairage_schema()).alias("d")) \
    .select("d.*") \
    .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"))

def eclairage_alert_fn(df):
    return df.withColumn("alert_level",
        when(
            (col("nb_faulty") >= 1)                         |
            (col("avg_luminosite") <= SEUIL_LUMINOSITE_ROUGE_MAX) |
            (col("avg_conso")      >= SEUIL_CONSO_ROUGE_MIN),
            "ROUGE"
        ).when(
            (col("avg_luminosite") <= SEUIL_LUMINOSITE_ORANGE_MAX) |
            (col("avg_conso")      >= SEUIL_CONSO_ORANGE_MIN),
            "ORANGE"
        ).otherwise("VERT")
    ).withColumn("alert_message",
        when(col("alert_level") == "ROUGE",
             "Panne eclairage detectee (status=faulty) - Intervention technique urgente")
        .when(col("alert_level") == "ORANGE",
              "Anomalie eclairage - Surveillance necessaire")
        .otherwise("Fonctionnement normal")
    )

df_ecl_alerts, df_ecl_15min = build_alert_pipeline(
    df_ecl,
    [
        avg("luminosity_lux").alias("avg_luminosite"),
        avg("power_kw").alias("avg_conso"),
        _sum(when(col("status") == "faulty", 1).otherwise(0)).alias("nb_faulty")
    ],
    eclairage_alert_fn,
    "eclairage", "avg_luminosite"
)


# ══════════════════════════════════════════════════════════════
#  CAPTEUR 4 — DÉCHETS
# ══════════════════════════════════════════════════════════════
print("[4/4] Initialisation stream DECHETS...")

df_dech_raw = read_kafka_topic(TOPIC_DECHETS)

df_dech = df_dech_raw \
    .select(from_json(col("value").cast("string"),
                      get_dechets_schema()).alias("d")) \
    .select("d.*") \
    .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"))

def dechets_alert_fn(df):
    return df.withColumn("alert_level",
        when(
            (col("avg_remplissage") >= SEUIL_REMPLISSAGE_ROUGE_MIN) |
            (col("avg_poids")       >= SEUIL_POIDS_ROUGE_MIN),
            "ROUGE"
        ).when(
            (col("avg_remplissage") >= SEUIL_REMPLISSAGE_ORANGE_MIN) |
            (col("avg_poids")       >= SEUIL_POIDS_ORANGE_MIN),
            "ORANGE"
        ).otherwise("VERT")
    ).withColumn("alert_message",
        when(col("alert_level") == "ROUGE",
             "Poubelle saturee - Collecte immediate - Notification Voirie")
        .when(col("alert_level") == "ORANGE",
              "Poubelle presque pleine - Planifier collecte sous 2h")
        .otherwise("Normal")
    )

df_dech_alerts, df_dech_15min = build_alert_pipeline(
    df_dech,
    [avg("fill_percent").alias("avg_remplissage"),
     avg("weight_kg").alias("avg_poids")],
    dechets_alert_fn,
    "dechets", "avg_remplissage"
)


# ══════════════════════════════════════════════════════════════
#  SINKS — Écriture des alertes (Kafka + InfluxDB + Console)
# ══════════════════════════════════════════════════════════════
print("\nDémarrage des sinks...")
queries = []

# Pour chaque type, on écrit dans le topic smartcity-alerts (même Event Hub)
# Options pour les producers (écriture)
kafka_write_options = {
    "kafka.bootstrap.servers": KAFKA_BOOTSTRAP_WRITE,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": f'org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{EVENT_HUB_CONNECTION_STRING_SEND}";',
}

for label, df_alerts, df_trends in [
    ("pollution",  df_poll_alerts,  df_poll_15min),
    ("traffic",    df_traf_alerts,  df_traf_15min),
    ("eclairage",  df_ecl_alerts,   df_ecl_15min),
    ("dechets",    df_dech_alerts,  df_dech_15min),
]:
    # Sink 1 — Kafka smartcity-alerts (alertes ORANGE/ROUGE uniquement)
    q_kafka = write_alerts_to_kafka(
        df_alerts, KAFKA_BOOTSTRAP_WRITE, TOPIC_ALERTS,
        f"{CHECKPOINT_DIR}/{label}-kafka",
        kafka_options=kafka_write_options
    ).start()
    queries.append(q_kafka)

    # Sink 2 — InfluxDB : alertes 5min
    q_influx_alerts = df_alerts.writeStream \
        .foreachBatch(write_batch_to_influx) \
        .outputMode("append") \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/{label}-influx-alerts") \
        .start()
    queries.append(q_influx_alerts)

    # Sink 3 — InfluxDB : tendances 15min (pour Grafana P5)
    q_influx_trends = df_trends.writeStream \
        .foreachBatch(write_batch_to_influx) \
        .outputMode("append") \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/{label}-influx-trends") \
        .start()
    queries.append(q_influx_trends)

    # Sink 4 — Console (debug / soutenance)
    q_console = df_alerts.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "20") \
        .start()
    queries.append(q_console)

    print(f"  ✅ {label.upper():12} — {len([q_kafka, q_influx_alerts, q_influx_trends, q_console])} sinks actifs")

print(f"\n{'=' * 60}")
print(f"  {len(queries)} queries actives — pipeline connecté à Azure Event Hubs")
print(f"  En attente de messages Kafka...")
print(f"  CTRL+C pour arrêter")
print(f"{'=' * 60}\n")

spark.streams.awaitAnyTermination()
