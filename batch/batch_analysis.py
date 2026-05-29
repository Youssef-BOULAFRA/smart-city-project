import os
from datetime import datetime
import matplotlib.pyplot as plt
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from streaming.spark_env import add_streaming_to_path

add_streaming_to_path()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count as _count, corr
from delta import configure_spark_with_delta_pip
from dotenv import load_dotenv

# ============================================
# 1. Configuration
# ============================================

env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'iot_simulation', '.env')
load_dotenv(env_path)

STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME", "smartcitylake")
STORAGE_ACCOUNT_KEY = os.getenv("STORAGE_ACCOUNT_KEY")

builder = SparkSession.builder \
    .appName("SmartCityBatchAnalysis") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config(f"spark.hadoop.fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net", STORAGE_ACCOUNT_KEY)

spark = configure_spark_with_delta_pip(
    builder,
    extra_packages=["org.apache.hadoop:hadoop-azure:3.3.4"]
).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "reports")
os.makedirs(OUTPUT_DIR, exist_ok=True)
REPORT_FILE = os.path.join(OUTPUT_DIR, "historical_analysis_report.md")


def load_delta_frame(path):
    """Charge un DataFrame Delta si le chemin existe et contient des données."""
    try:
        df = spark.read.format("delta").load(path)
        if df.rdd.isEmpty():
            return None
        return df
    except Exception:
        return None


def write_summary_to_aggregated(summary_df, summary_name):
    """Persiste les résultats d'analyse dans la zone aggregated."""
    summary_path = f"abfss://aggregated@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/analysis/{summary_name}/"
    summary_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(summary_path)
    return summary_path


def format_markdown_table(pdf, columns, max_rows=10):
    if pdf.empty:
        return "_Aucune donnée disponible._"

    display_pdf = pdf[columns].head(max_rows)
    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join(["---"] * len(columns)) + " |"
    rows = []
    for _, row in display_pdf.iterrows():
        values = [str(row[col]) for col in columns]
        rows.append("| " + " | ".join(values) + " |")
    return "\n".join([header, separator] + rows)

# ============================================
# 2. Fonctions d'analyse
# ============================================

def analyze_pollution_by_zone():
    """ Analyse l'AQI moyen par zone """
    print("\n--- Analyse: Pollution par Zone ---")
    try:
        df_pollution = load_delta_frame(f"abfss://processed@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/pollution/")
        if df_pollution is None:
            print("Aucune donnée pollution disponible.")
            return None
        
        avg_aqi_df = df_pollution.groupBy("zone").agg(
            avg("air_quality_index").alias("avg_aqi"),
            avg("co2_ppm").alias("avg_co2"),
            avg("pm25").alias("avg_pm25")
        ).orderBy("avg_aqi", ascending=False)
        avg_aqi_df.show()
        write_summary_to_aggregated(avg_aqi_df, "pollution_by_zone")
        
        pdf = avg_aqi_df.toPandas()
        best_zone = pdf.sort_values("avg_aqi", ascending=True).iloc[0]
        worst_zone = pdf.iloc[0]
        
        plt.figure(figsize=(10, 6))
        plt.bar(pdf['zone'], pdf['avg_aqi'], color=['red', 'orange', 'green', 'blue'])
        plt.title('Indice de Qualité de l\'Air (AQI) Moyen par Zone')
        plt.xlabel('Zone')
        plt.ylabel('AQI Moyen')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plot_path = os.path.join(OUTPUT_DIR, 'aqi_by_zone.png')
        plt.savefig(plot_path)
        plt.close()
        print(f"Graphique sauvegardé dans {plot_path}")

        return {
            "summary_df": avg_aqi_df,
            "plot_path": plot_path,
            "best_zone": best_zone.to_dict(),
            "worst_zone": worst_zone.to_dict(),
        }
        
    except Exception as e:
        print(f"Erreur lors de l'analyse de la pollution: {e}")
        return None

def analyze_traffic_by_hour():
    """ Analyse le trafic moyen selon l'heure de la journée """
    print("\n--- Analyse: Trafic par Heure ---")
    try:
        df_traffic = load_delta_frame(f"abfss://processed@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/traffic/")
        if df_traffic is None:
            print("Aucune donnée trafic disponible.")
            return None
        
        avg_traffic_df = df_traffic.groupBy("heure").agg(
            avg("vehicles_per_min").alias("avg_vehicles"),
            avg("avg_speed_kmh").alias("avg_speed")
        ).orderBy("heure")
        avg_traffic_df.show(24)
        write_summary_to_aggregated(avg_traffic_df, "traffic_by_hour")
        
        pdf = avg_traffic_df.toPandas()
        peak_row = pdf.sort_values("avg_vehicles", ascending=False).iloc[0]
        
        plt.figure(figsize=(10, 6))
        plt.plot(pdf['heure'], pdf['avg_vehicles'], marker='o', linestyle='-', color='b')
        plt.title('Volume de Trafic Moyen au cours de la journée')
        plt.xlabel('Heure de la journée (0-23)')
        plt.ylabel('Véhicules par minute (Moyenne)')
        plt.grid(True)
        plt.xticks(range(0, 24))
        plt.tight_layout()
        plot_path = os.path.join(OUTPUT_DIR, 'traffic_by_hour.png')
        plt.savefig(plot_path)
        plt.close()
        print(f"Graphique sauvegardé dans {plot_path}")

        return {
            "summary_df": avg_traffic_df,
            "plot_path": plot_path,
            "peak_hour": peak_row.to_dict(),
        }

    except Exception as e:
        print(f"Erreur lors de l'analyse du trafic: {e}")
        return None


def analyze_traffic_pollution_correlation():
    """Mesure la corrélation historique entre trafic et pollution."""
    print("\n--- Analyse: Corrélation trafic / pollution ---")
    try:
        df_pollution = load_delta_frame(f"abfss://processed@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/pollution/")
        df_traffic = load_delta_frame(f"abfss://processed@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/traffic/")
        if df_pollution is None or df_traffic is None:
            print("Données insuffisantes pour calculer la corrélation.")
            return None

        pollution_daily = df_pollution.groupBy("date_partition", "zone").agg(
            avg("air_quality_index").alias("avg_aqi")
        )
        traffic_daily = df_traffic.groupBy("date_partition", "zone").agg(
            avg("vehicles_per_min").alias("avg_vehicles")
        )

        joined = pollution_daily.join(traffic_daily, ["date_partition", "zone"], "inner")
        correlation_value = joined.agg(corr("avg_vehicles", "avg_aqi").alias("traffic_pollution_corr")).collect()[0]["traffic_pollution_corr"]

        pdf = joined.toPandas()
        plt.figure(figsize=(8, 6))
        plt.scatter(pdf['avg_vehicles'], pdf['avg_aqi'], alpha=0.7, color='darkred')
        plt.title('Corrélation historique trafic / pollution')
        plt.xlabel('Trafic moyen (véhicules/min)')
        plt.ylabel('AQI moyen')
        plt.tight_layout()
        plot_path = os.path.join(OUTPUT_DIR, 'traffic_pollution_correlation.png')
        plt.savefig(plot_path)
        plt.close()

        write_summary_to_aggregated(joined, "traffic_pollution_correlation")

        return {
            "correlation": correlation_value,
            "plot_path": plot_path,
        }

    except Exception as e:
        print(f"Erreur lors de l'analyse de corrélation: {e}")
        return None


def build_report(pollution_result, traffic_result, correlation_result):
    """Rédige un rapport Markdown avec constats, graphiques et recommandations."""
    lines = []
    lines.append("# Rapport d'analyse historique Smart City")
    lines.append("")
    lines.append(f"Date de génération : {datetime.now().isoformat(timespec='seconds')}")
    lines.append("")
    lines.append("## Résumé exécutif")

    if pollution_result:
        worst_zone = pollution_result["worst_zone"]
        best_zone = pollution_result["best_zone"]
        lines.append(f"- Zone la plus exposée : **{worst_zone['zone']}** (AQI moyen {worst_zone['avg_aqi']:.2f}).")
        lines.append(f"- Zone la plus saine : **{best_zone['zone']}** (AQI moyen {best_zone['avg_aqi']:.2f}).")
    else:
        lines.append("- Données pollution insuffisantes pour conclure.")

    if traffic_result:
        peak_hour = traffic_result["peak_hour"]
        lines.append(f"- Heure de pointe trafic : **{int(peak_hour['heure'])}h** avec {peak_hour['avg_vehicles']:.2f} véhicules/min en moyenne.")
    else:
        lines.append("- Données trafic insuffisantes pour conclure.")

    if correlation_result and correlation_result.get("correlation") is not None:
        lines.append(f"- Corrélation trafic/pollution : **{correlation_result['correlation']:.2f}**.")
    else:
        lines.append("- Corrélation trafic/pollution non calculable avec les données disponibles.")

    lines.append("")
    lines.append("## Graphiques")
    if pollution_result:
        lines.append(f"![AQI moyen par zone](aqi_by_zone.png)")
    if traffic_result:
        lines.append(f"![Trafic moyen par heure](traffic_by_hour.png)")
    if correlation_result:
        lines.append(f"![Corrélation trafic / pollution](traffic_pollution_correlation.png)")

    lines.append("")
    lines.append("## Tableau pollution par zone")
    if pollution_result:
        pollution_pdf = pollution_result["summary_df"].toPandas()
        lines.append(format_markdown_table(pollution_pdf, ["zone", "avg_aqi", "avg_co2", "avg_pm25"]))
    else:
        lines.append("_Aucune donnée disponible._")

    lines.append("")
    lines.append("## Tableau trafic par heure")
    if traffic_result:
        traffic_pdf = traffic_result["summary_df"].toPandas()
        lines.append(format_markdown_table(traffic_pdf, ["heure", "avg_vehicles", "avg_speed"]))
    else:
        lines.append("_Aucune donnée disponible._")

    lines.append("")
    lines.append("## Recommandations opérationnelles")
    recommendations = []
    if pollution_result:
        recommendations.append(f"Renforcer la surveillance dans la zone **{pollution_result['worst_zone']['zone']}** et y prioriser les contrôles qualité de l'air.")
    if traffic_result:
        recommendations.append(f"Adapter la synchronisation des feux et les alertes trafic autour de **{int(traffic_result['peak_hour']['heure'])}h**.")
    if correlation_result and correlation_result.get("correlation") is not None:
        recommendations.append("Croiser les pics de trafic avec les mesures pollution pour déclencher des actions préventives sur les zones les plus corrélées.")

    if not recommendations:
        recommendations.append("Les données disponibles sont insuffisantes pour formuler une recommandation précise.")

    for recommendation in recommendations:
        lines.append(f"- {recommendation}")

    with open(REPORT_FILE, mode="w", encoding="utf-8") as report_file:
        report_file.write("\n".join(lines))

    print(f"Rapport généré dans {REPORT_FILE}")


# ============================================
# 3. Exécution Principale
# ============================================

if __name__ == "__main__":
    print("Démarrage de l'analyse des tendances historiques...")
    pollution_result = analyze_pollution_by_zone()
    traffic_result = analyze_traffic_by_hour()
    correlation_result = analyze_traffic_pollution_correlation()
    build_report(pollution_result, traffic_result, correlation_result)
    print("\nAnalyses terminées.")
    spark.stop()
