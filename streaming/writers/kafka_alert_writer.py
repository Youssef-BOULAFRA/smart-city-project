from pyspark.sql import DataFrame
from pyspark.sql.functions import to_json, struct, col


def write_alerts_to_kafka(df: DataFrame, bootstrap: str, topic: str, checkpoint_dir: str):
    """
    Écrit les alertes dans le topic Kafka smartcity-alerts.
    Réutilisable pour les 4 types de capteurs.
    outputMode append : conforme cahier de charge P3.
    """
    df_out = df.select(
        to_json(struct(
            col("window.start").alias("debut_fenetre"),
            col("window.end").alias("fin_fenetre"),
            "zone",
            "alert_level",
            "alert_message",
            "type_capteur",
            "valeur_detectee"
        )).alias("value")
    )

    return df_out.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap) \
        .option("topic", topic) \
        .option("checkpointLocation", checkpoint_dir) \
        .outputMode("append")