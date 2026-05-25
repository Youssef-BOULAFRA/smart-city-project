from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

def get_pollution_schema():
    """Schéma JSON des messages pollution (défini par P1)"""
    return StructType([
        StructField("timestamp", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("zone", StringType(), True),
        StructField("type", StringType(), True),
        StructField("co2_ppm", DoubleType(), True),
        StructField("pm25", DoubleType(), True),
        StructField("air_quality_index", IntegerType(), True)
    ])

def get_traffic_schema():
    """Schéma JSON des messages trafic"""
    return StructType([
        StructField("timestamp", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("zone", StringType(), True),
        StructField("type", StringType(), True),
        StructField("vehicles_per_min", IntegerType(), True),
        StructField("avg_speed_kmh", DoubleType(), True),
        StructField("congestion", StringType(), True)
    ])

def get_eclairage_schema():
    """Schéma JSON des messages éclairage"""
    return StructType([
        StructField("timestamp", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("zone", StringType(), True),
        StructField("type", StringType(), True),
        StructField("luminosity_lux", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("power_kw", DoubleType(), True)
    ])

def get_dechets_schema():
    """Schéma JSON des messages déchets"""
    return StructType([
        StructField("timestamp", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("zone", StringType(), True),
        StructField("type", StringType(), True),
        StructField("fill_percent", DoubleType(), True),
        StructField("weight_kg", DoubleType(), True),
        StructField("alert", StringType(), True)
    ])