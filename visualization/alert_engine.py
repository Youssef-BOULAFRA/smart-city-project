import json
import os
import sys
import smtplib
import threading
from datetime import datetime, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# ── Configuration ─────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP   = "localhost:9093"    # PLAINTEXT_HOST (hors Docker)
KAFKA_TOPIC       = "smartcity-alerts"
KAFKA_GROUP_ID    = "alert-engine-p5"

INFLUX_URL    = "http://localhost:8086"
INFLUX_TOKEN  = "smartcity-token"
INFLUX_ORG    = "smartcity"
INFLUX_BUCKET = "smartcity"

# Optionnel — Email (mettre les vraies valeurs si vous activez SMTP)
SMTP_ENABLED  = False
SMTP_HOST     = "smtp.gmail.com"
SMTP_PORT     = 587
SMTP_USER     = "votre_email@gmail.com"
SMTP_PASSWORD = "votre_app_password"
EMAIL_TO      = "equipe@smartcity.ma"

# Couleurs console ANSI
RED    = "\033[91m"
ORANGE = "\033[93m"
GREEN  = "\033[92m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

# ── Clients ───────────────────────────────────────────────────────────────────
_influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
_write_api     = _influx_client.write_api(write_options=SYNCHRONOUS)

# Compteur d'alertes par niveau (pour affichage statistiques)
_alert_counts = {"ROUGE": 0, "ORANGE": 0, "total": 0}
_lock = threading.Lock()


# ═══════════════════════════════════════════════════════════════════════════════
# 1. ÉCRITURE INFLUXDB
# ═══════════════════════════════════════════════════════════════════════════════

def write_to_influx(alert: dict):
    """
    Écrit l'alerte dans la mesure InfluxDB "alertes".
    Cette mesure est utilisée par Grafana pour afficher l'historique complet.
    """
    try:
        point = (
            Point("alertes")
            .tag("zone",    alert.get("zone", "unknown"))
            .tag("niveau",  alert.get("alert_level", "unknown"))
            .tag("type",    alert.get("type_capteur", "unknown"))
            .field("valeur",       float(alert.get("valeur_detectee", 0.0)))
            .time(datetime.now(timezone.utc))
        )
        _write_api.write(bucket=INFLUX_BUCKET, record=point)
    except Exception as e:
        print(f"  ❌ InfluxDB write error : {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# 2. EMAIL (OPTIONNEL)
# ═══════════════════════════════════════════════════════════════════════════════

def send_email(alert: dict):
    """
    Envoie un email de notification d'alerte.
    N'est appelé que si SMTP_ENABLED = True et l'alerte est ROUGE.
    """
    if not SMTP_ENABLED or alert.get("alert_level") != "ROUGE":
        return

    zone       = alert.get("zone", "?")
    type_c     = alert.get("type_capteur", "?")
    message    = alert.get("alert_message", "Alerte critique")
    valeur     = alert.get("valeur_detectee", "?")
    debut      = alert.get("debut_fenetre", "?")

    subject = f"🚨 [SmartCity] ALERTE ROUGE — {zone.upper()} ({type_c})"
    body = f"""
Alerte critique détectée dans le système Smart City.

Zone       : {zone}
Capteur    : {type_c}
Message    : {message}
Valeur     : {valeur}
Début      : {debut}

Action requise immédiatement.
    """.strip()

    try:
        msg = MIMEMultipart()
        msg["From"]    = SMTP_USER
        msg["To"]      = EMAIL_TO
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)

        print(f"  📧 Email envoyé à {EMAIL_TO}")
    except Exception as e:
        print(f"  ⚠️  Email non envoyé : {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# 3. AFFICHAGE CONSOLE
# ═══════════════════════════════════════════════════════════════════════════════

def display_alert(alert: dict):
    """Affiche l'alerte dans la console avec couleurs et formatage."""
    niveau    = alert.get("alert_level", "?")
    zone      = alert.get("zone", "?")
    type_c    = alert.get("type_capteur", "?")
    message   = alert.get("alert_message", "?")
    valeur    = alert.get("valeur_detectee", "?")
    heure     = datetime.now().strftime("%H:%M:%S")

    color = RED if niveau == "ROUGE" else ORANGE

    print(f"\n{color}{BOLD}{'━' * 55}{RESET}")
    print(f"{color}{BOLD}  🚨 ALERTE {niveau}  —  {heure}{RESET}")
    print(f"{color}{'━' * 55}{RESET}")
    print(f"  Zone     : {BOLD}{zone.upper()}{RESET}")
    print(f"  Capteur  : {type_c}")
    print(f"  Message  : {message}")
    print(f"  Valeur   : {valeur}")
    print(f"{color}{'━' * 55}{RESET}\n")

    with _lock:
        _alert_counts[niveau] = _alert_counts.get(niveau, 0) + 1
        _alert_counts["total"] += 1


def display_stats():
    """Affiche un résumé des alertes toutes les 60 secondes."""
    def _loop():
        while True:
            import time
            time.sleep(60)
            with _lock:
                r = _alert_counts.get("ROUGE", 0)
                o = _alert_counts.get("ORANGE", 0)
                t = _alert_counts.get("total", 0)
            print(f"\n📊 Stats alertes (dernière heure) — "
                  f"{RED}ROUGE: {r}{RESET} | "
                  f"{ORANGE}ORANGE: {o}{RESET} | "
                  f"Total: {t}\n")

    t = threading.Thread(target=_loop, daemon=True)
    t.start()


# ═══════════════════════════════════════════════════════════════════════════════
# 4. MAIN — LECTURE KAFKA EN BOUCLE
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 55)
    print("  Smart City — Alert Engine (P5)")
    print(f"  Topic Kafka : {KAFKA_TOPIC}")
    print(f"  Bootstrap   : {KAFKA_BOOTSTRAP}")
    print(f"  InfluxDB    : {INFLUX_URL}")
    print("=" * 55)
    print("\n⏳ Connexion à Kafka...")

    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="latest",
            group_id=KAFKA_GROUP_ID,
            enable_auto_commit=True,
        )
        print("✅ Connecté à Kafka — en attente d'alertes...\n")
    except Exception as e:
        print(f"❌ Impossible de se connecter à Kafka : {e}")
        sys.exit(1)

    display_stats()

    try:
        for message in consumer:
            alert = message.value

            # Filtrer : ne traiter que les alertes ORANGE et ROUGE
            niveau = alert.get("alert_level", "VERT")
            if niveau not in ("ORANGE", "ROUGE"):
                continue

            # Traitement en parallèle pour ne pas bloquer la lecture Kafka
            t = threading.Thread(
                target=lambda a=alert: [
                    display_alert(a),
                    write_to_influx(a),
                    send_email(a),
                ],
                daemon=True,
            )
            t.start()

    except KeyboardInterrupt:
        print("\n🛑 Alert Engine arrêté")
    finally:
        consumer.close()
        _influx_client.close()


if __name__ == "__main__":
    main()