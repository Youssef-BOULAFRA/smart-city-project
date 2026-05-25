from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import sys, os

current_file  = os.path.abspath(__file__)
writers_dir   = os.path.dirname(current_file)
streaming_dir = os.path.dirname(writers_dir)
if streaming_dir not in sys.path:
    sys.path.insert(0, streaming_dir)

from configuration.config import INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET

_client    = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
_write_api = _client.write_api(write_options=SYNCHRONOUS)


def write_batch_to_influx(df, epoch_id):
    """
    Écrit un micro-batch Spark dans InfluxDB.
    Compatible avec les deux DataFrames :
      - alertes 5min  (alert_level = ORANGE / ROUGE)
      - tendances 15min (alert_level = TENDANCE)
    """
    rows = df.collect()
    if not rows:
        return

    points = []
    for row in rows:
        try:
            point = Point("alertes") \
                .tag("zone",         row.zone) \
                .tag("niveau",       row.alert_level) \
                .tag("type",         row.type_capteur) \
                .field("valeur",     float(row.valeur_detectee)) \
                .time(row.window.start)
            points.append(point)
        except Exception as e:
            print(f"⚠️ InfluxDB write_batch_to_influx — ligne ignorée : {e}")

    if points:
        _write_api.write(bucket=INFLUX_BUCKET, record=points)
        print(f"   📊 InfluxDB ← {len(points)} points écrits (epoch {epoch_id})")