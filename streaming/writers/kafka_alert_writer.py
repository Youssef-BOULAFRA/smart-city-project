from pyspark.sql import DataFrame
from pyspark.sql.functions import to_json, struct, col


def write_alerts_to_kafka(df: DataFrame, bootstrap: str, topic: str, checkpoint_dir: str, kafka_options: dict = None):
    df_out = df.select(
        to_json(struct(
            col("window.start").alias("debut_fenetre"),
            col("window.end").alias("fin_fenetre"),
            "zone", "alert_level", "alert_message", "type_capteur", "valeur_detectee"
        )).alias("value")
    )
    writer = df_out.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap) \
        .option("topic", topic) \
        .option("checkpointLocation", checkpoint_dir) \
        .outputMode("append")
    if kafka_options:
        for key, value in kafka_options.items():
            writer = writer.option(key, value)
    return writer
