# ============================================================
# configuration/config.py — Smart City P3 (version finale)
# ============================================================

# --- Kafka ---
KAFKA_BOOTSTRAP = "localhost:9093"   # PLAINTEXT_HOST du docker-compose P2

# Topics sources (P2)
TOPIC_POLLUTION = "smartcity-pollution"
TOPIC_TRAFFIC   = "smartcity-traffic"
TOPIC_ECLAIRAGE = "smartcity-eclairage"
TOPIC_DECHETS   = "smartcity-dechets"

# Topic sortie (P3 → P5)
TOPIC_ALERTS    = "smartcity-alerts"

# ============================================================
# SEUILS POLLUTION  (sources : OMS 2021, EPA AQI)
# ============================================================
SEUIL_CO2_ORANGE_MIN  = 601
SEUIL_CO2_ORANGE_MAX  = 800
SEUIL_CO2_ROUGE_MIN   = 801

SEUIL_PM25_ORANGE_MIN = 36
SEUIL_PM25_ORANGE_MAX = 75
SEUIL_PM25_ROUGE_MIN  = 76

SEUIL_NO2_ORANGE_MIN  = 101
SEUIL_NO2_ORANGE_MAX  = 200
SEUIL_NO2_ROUGE_MIN   = 201

SEUIL_AQI_ORANGE_MIN  = 151
SEUIL_AQI_ORANGE_MAX  = 300
SEUIL_AQI_ROUGE_MIN   = 301

# ============================================================
# SEUILS TRAFIC  (source : code de la route, standards urbains)
# ============================================================
SEUIL_VEHICULES_ORANGE_MIN = 41
SEUIL_VEHICULES_ORANGE_MAX = 60
SEUIL_VEHICULES_ROUGE_MIN  = 61

SEUIL_VITESSE_VERT_MIN     = 30
SEUIL_VITESSE_ORANGE_MIN   = 11
SEUIL_VITESSE_ORANGE_MAX   = 30
SEUIL_VITESSE_ROUGE_MAX    = 10

# ============================================================
# SEUILS DÉCHETS  (source : standards municipaux)
# ============================================================
SEUIL_REMPLISSAGE_ORANGE_MIN = 76
SEUIL_REMPLISSAGE_ORANGE_MAX = 90
SEUIL_REMPLISSAGE_ROUGE_MIN  = 91

SEUIL_POIDS_ORANGE_MIN = 61
SEUIL_POIDS_ORANGE_MAX = 80
SEUIL_POIDS_ROUGE_MIN  = 81

# ============================================================
# SEUILS ÉCLAIRAGE  (source : norme EN 13201 éclairage public)
# ============================================================
# Luminosité (lux) — simulateur génère 30-80 lux la nuit, 300-800 le jour
SEUIL_LUMINOSITE_VERT_MIN    = 10
SEUIL_LUMINOSITE_ORANGE_MIN  = 5
SEUIL_LUMINOSITE_ORANGE_MAX  = 10
SEUIL_LUMINOSITE_ROUGE_MAX   = 5

# Consommation (kW) — CORRIGÉ : simulateur génère 0.05-0.60 kW
# Anciens seuils (5 / 8 kW) jamais atteints car hors plage simulée
SEUIL_CONSO_ORANGE_MIN = 0.40   # > 0.40 kW = surconsommation anormale
SEUIL_CONSO_ORANGE_MAX = 0.54
SEUIL_CONSO_ROUGE_MIN  = 0.55   # > 0.55 kW = panne/court-circuit probable

# ============================================================
# SPARK
# ============================================================
# Préfixe file:/// obligatoire sur Windows pour les chemins locaux
CHECKPOINT_DIR = "file:///C:/tmp/spark-checkpoints"

# JARs locaux dans infra/jars/ (évite le téléchargement Maven au démarrage)
JARS_REQUIRED = [
    "org.apache.spark_spark-sql-kafka-0-10_2.12-3.5.1.jar",
    "org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.5.1.jar",
    "org.apache.kafka_kafka-clients-3.4.1.jar",
    "org.lz4_lz4-java-1.8.0.jar",
    "org.xerial.snappy_snappy-java-1.1.10.3.jar",
    "org.slf4j_slf4j-api-2.0.7.jar",
    "org.apache.commons_commons-pool2-2.11.1.jar",
    "commons-logging_commons-logging-1.1.3.jar",
    "com.google.code.findbugs_jsr305-3.0.0.jar",
]

# ============================================================
# INFLUXDB
# ============================================================
INFLUX_URL    = "http://localhost:8086"
INFLUX_TOKEN  = "smartcity-token"
INFLUX_ORG    = "smartcity"
INFLUX_BUCKET = "smartcity"