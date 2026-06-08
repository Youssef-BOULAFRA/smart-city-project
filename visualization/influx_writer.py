import json
import os
import sys
from dotenv import load_dotenv
from datetime import datetime, timezone

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from kafka import KafkaConsumer

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'iot_simulation', '.env'))

# ── Configuration ─────────────────────────────────────────────────────────────
EVENT_HUB_FQDN                     = os.getenv("EVENT_HUB_FQDN", "")
EVENT_HUB_CONNECTION_STRING_LISTEN = os.getenv("EVENT_HUB_CONNECTION_STRING_LISTEN", "")
KAFKA_BOOTSTRAP = f"{EVENT_HUB_FQDN}:9093"
KAFKA_GROUP_ID  = os.getenv("KAFKA_GROUP_ID", "influx-writer-p5")

TOPIC_POLLUTION = os.getenv("TOPIC_POLLUTION", "smartcity-pollution")
TOPIC_TRAFFIC   = os.getenv("TOPIC_TRAFFIC",   "smartcity-traffic")
TOPIC_ECLAIRAGE = os.getenv("TOPIC_ECLAIRAGE", "smartcity-eclairage")
TOPIC_DECHETS   = os.getenv("TOPIC_DECHETS",   "smartcity-dechets")

INFLUX_URL    = os.getenv("INFLUX_URL",    "http://localhost:8086")
INFLUX_TOKEN  = os.getenv("INFLUX_TOKEN",  "smartcity-token")
INFLUX_ORG    = os.getenv("INFLUX_ORG",    "smartcity")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "smartcity")

TOPIC_TO_MEASUREMENT = {
    TOPIC_POLLUTION: "pollution",
    TOPIC_TRAFFIC:   "traffic",
    TOPIC_ECLAIRAGE: "eclairage",
    TOPIC_DECHETS:   "dechets",
}


# ── Helpers ───────────────────────────────────────────────────────────────────
def _parse_time(value) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    if isinstance(value, str):
        try:
            if value.endswith("Z"):
                value = value.replace("Z", "+00:00")
            return datetime.fromisoformat(value)
        except ValueError:
            return datetime.now(timezone.utc)
    return datetime.now(timezone.utc)


def _add_field(point: Point, name: str, value) -> None:
    if value is None:
        return
    try:
        point.field(name, float(value))
    except (TypeError, ValueError):
        return


def _build_point(measurement: str, payload: dict):
    zone      = payload.get("zone",      "unknown")
    device_id = payload.get("device_id", "unknown")

    point = Point(measurement).tag("zone", zone).tag("device_id", device_id)

    if measurement == "pollution":
        _add_field(point, "co2_ppm", payload.get("co2_ppm"))
        _add_field(point, "pm25",    payload.get("pm25"))
        _add_field(point, "aqi",     payload.get("air_quality_index"))

    elif measurement == "traffic":
        _add_field(point, "vehicles_per_min", payload.get("vehicles_per_min"))
        _add_field(point, "avg_speed_kmh",    payload.get("avg_speed_kmh"))

    elif measurement == "eclairage":
        _add_field(point, "luminosity_lux", payload.get("luminosity_lux"))
        _add_field(point, "power_kw",       payload.get("power_kw"))
        status      = str(payload.get("status", "")).lower()
        faulty_count = 1 if status == "faulty" else 0
        _add_field(point, "faulty_count", faulty_count)

    elif measurement == "dechets":
        _add_field(point, "fill_percent", payload.get("fill_percent"))
        _add_field(point, "weight_kg",    payload.get("weight_kg"))
        alert_flag = 1 if str(payload.get("alert", "false")).lower() == "true" else 0
        _add_field(point, "alert_flag", alert_flag)

    else:
        return None

    point.time(_parse_time(payload.get("timestamp")))
    return point


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> int:
    print("=" * 60)
    print("  Smart City — Influx Writer (P5)")
    print(f"  Kafka     : {KAFKA_BOOTSTRAP}")
    print(f"  InfluxDB  : {INFLUX_URL}")
    print("=" * 60)

    try:
        consumer = KafkaConsumer(
            TOPIC_POLLUTION,
            TOPIC_TRAFFIC,
            TOPIC_ECLAIRAGE,
            TOPIC_DECHETS,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            # ── Authentification Event Hubs ──
            security_protocol='SASL_SSL',
            sasl_mechanism='PLAIN',
            sasl_plain_username='$ConnectionString',
            sasl_plain_password=EVENT_HUB_CONNECTION_STRING_LISTEN,
            # ────────────────────────────────
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="latest",
            group_id=KAFKA_GROUP_ID,
            enable_auto_commit=True,
            api_version_auto_timeout_ms=10000,
        )
        print("✅ Connecté à Event Hubs — en attente de messages...\n")
    except Exception as exc:
        print(f"❌ Kafka connection failed: {exc}")
        return 1

    influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api     = influx_client.write_api(write_options=SYNCHRONOUS)

    try:
        for message in consumer:
            measurement = TOPIC_TO_MEASUREMENT.get(message.topic)
            if not measurement:
                continue
            payload = message.value
            if not isinstance(payload, dict):
                continue
            point = _build_point(measurement, payload)
            if point is None:
                continue
            try:
                write_api.write(bucket=INFLUX_BUCKET, record=point)
                print(f"✅ {measurement} — zone={payload.get('zone')} écrit dans InfluxDB")
            except Exception as exc:
                print(f"❌ Influx write failed: {exc}")

    except KeyboardInterrupt:
        print("\nStopping Influx writer")
    finally:
        consumer.close()
        influx_client.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
