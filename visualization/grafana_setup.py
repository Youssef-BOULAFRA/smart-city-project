import requests
import json
import time
import sys
import os
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'iot_simulation', '.env'))

# ── Configuration ─────────────────────────────────────────────────────────────
GRAFANA_URL      = os.getenv("GRAFANA_URL", "http://localhost:3000")
GRAFANA_USER     = os.getenv("GRAFANA_USER", "admin")
GRAFANA_PASSWORD = os.getenv("GRAFANA_PASSWORD", "smartcity-admin")
GRAFANA_AUTH     = (GRAFANA_USER, GRAFANA_PASSWORD)

# InfluxDB — accès depuis DANS le réseau Docker (nom du container)
INFLUX_URL_DOCKER = os.getenv("INFLUX_URL_INTERNAL", "http://influxdb:8086")
INFLUX_TOKEN      = os.getenv("INFLUX_TOKEN", "smartcity-token")
INFLUX_ORG        = os.getenv("INFLUX_ORG", "smartcity")
INFLUX_BUCKET     = os.getenv("INFLUX_BUCKET", "smartcity")


# ═══════════════════════════════════════════════════════════════════════════════
# 1. ATTENTE DE GRAFANA
# ═══════════════════════════════════════════════════════════════════════════════

def wait_for_grafana(retries: int = 30, delay: int = 3) -> bool:
    """Attend que Grafana réponde sur /api/health."""
    print("⏳ Attente de Grafana...")
    for i in range(retries):
        try:
            r = requests.get(f"{GRAFANA_URL}/api/health", timeout=3)
            if r.status_code == 200 and r.json().get("database") == "ok":
                print("✅ Grafana opérationnel !")
                return True
        except Exception:
            pass
        print(f"   Tentative {i + 1}/{retries}...")
        time.sleep(delay)
    print("❌ Grafana inaccessible. Vérifie que docker-compose est lancé.")
    return False


# ═══════════════════════════════════════════════════════════════════════════════
# 2. DATASOURCE INFLUXDB
# ═══════════════════════════════════════════════════════════════════════════════

def create_datasource() -> str | None:
    """
    Crée la datasource InfluxDB dans Grafana.
    Retourne l'UID de la datasource créée (ou existante).
    """
    # Vérifier si elle existe déjà
    r = requests.get(
        f"{GRAFANA_URL}/api/datasources/name/InfluxDB-SmartCity",
        auth=GRAFANA_AUTH
    )
    if r.status_code == 200:
        uid = r.json().get("uid")
        print(f"✅ Datasource déjà existante (uid: {uid})")
        return uid

    # Créer la datasource InfluxDB 2.x avec Flux
    payload = {
        "name":    "InfluxDB-SmartCity",
        "type":    "influxdb",
        "access":  "proxy",
        "url":     INFLUX_URL_DOCKER,
        "isDefault": True,
        "jsonData": {
            "version":      "Flux",           # InfluxDB 2.x utilise Flux, pas InfluxQL
            "organization": INFLUX_ORG,
            "defaultBucket": INFLUX_BUCKET,
            "tlsSkipVerify": True,
        },
        "secureJsonData": {
            "token": INFLUX_TOKEN,
        },
    }

    r = requests.post(
        f"{GRAFANA_URL}/api/datasources",
        auth=GRAFANA_AUTH,
        json=payload,
        headers={"Content-Type": "application/json"},
    )

    if r.status_code in (200, 201):
        uid = r.json().get("datasource", {}).get("uid") or r.json().get("uid")
        print(f"✅ Datasource créée (uid: {uid})")
        return uid
    else:
        print(f"❌ Erreur datasource : {r.status_code} — {r.text}")
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# 3. DÉFINITION DES PANELS
# ═══════════════════════════════════════════════════════════════════════════════

def _flux(query: str) -> str:
    """Nettoie l'indentation des requêtes Flux pour l'API Grafana."""
    return "\n".join(line for line in query.strip().splitlines())


def build_panels(ds_uid: str) -> list:
    """
    Retourne la liste des 6 panels du dashboard.
    Chaque panel utilise les mesures écrites par influxdb_writer.py (version améliorée).
    """

    def datasource():
        return {"type": "influxdb", "uid": ds_uid}

    def flux_target(query: str, ref_id: str = "A") -> dict:
        return {
            "datasource": datasource(),
            "query":      _flux(query),
            "refId":      ref_id,
        }

    # ─────────────────────────────────────────────────────────────────────────
    # PANEL 1 — AQI par zone (Gauge)
    # Mesure : pollution | Champ : aqi | Refresh : 10s
    # ─────────────────────────────────────────────────────────────────────────
    panel_aqi = {
        "id":       1,
        "title":    "🌫  Indice de Qualité de l'Air (AQI) par zone",
        "type":     "gauge",
        "gridPos":  {"x": 0, "y": 0, "w": 12, "h": 8},
        "options": {
            "reduceOptions": {"calcs": ["lastNotNull"], "values": False},
            "orientation":          "auto",
            "showThresholdLabels":  True,
            "showThresholdMarkers": True,
        },
        "fieldConfig": {
            "defaults": {
                "unit": "short",
                "min":  0,
                "max":  500,
                "displayName": "${__field.labels.zone}",
                "color": {"mode": "thresholds"},
                "thresholds": {
                    "mode": "absolute",
                    "steps": [
                        {"value": None, "color": "green"},
                        {"value": 100,  "color": "yellow"},
                        {"value": 200,  "color": "orange"},
                        {"value": 300,  "color": "red"},
                    ],
                },
            },
            "overrides": [],
        },
        "targets": [
            flux_target("""
from(bucket: "smartcity")
  |> range(start: -15m)
  |> filter(fn: (r) => r._measurement == "pollution" and r._field == "aqi")
  |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
  |> last()
""")
        ],
    }

    # ─────────────────────────────────────────────────────────────────────────
    # PANEL 2 — Trafic temps réel (Time series)
    # Mesure : traffic | Champs : vehicles_per_min | Refresh : 5s
    # ─────────────────────────────────────────────────────────────────────────
    panel_traffic = {
        "id":      2,
        "title":   "🚗  Trafic — Véhicules par minute par zone",
        "type":    "timeseries",
        "gridPos": {"x": 12, "y": 0, "w": 12, "h": 8},
        "options": {
            "tooltip": {"mode": "multi"},
            "legend":  {"displayMode": "list", "placement": "bottom"},
        },
        "fieldConfig": {
            "defaults": {
                "unit":       "short",
                "lineWidth":  2,
                "fillOpacity": 10,
                "color": {"mode": "palette-classic"},
                "custom": {
                    "lineInterpolation": "smooth",
                    "showPoints":        "never",
                },
                "thresholds": {
                    "mode": "absolute",
                    "steps": [
                        {"value": None, "color": "green"},
                        {"value": 41,   "color": "orange"},
                        {"value": 61,   "color": "red"},
                    ],
                },
            },
            "overrides": [],
        },
        "targets": [
            flux_target("""
from(bucket: "smartcity")
  |> range(start: -30m)
  |> filter(fn: (r) => r._measurement == "traffic" and r._field == "vehicles_per_min")
  |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
""")
        ],
    }

    # ─────────────────────────────────────────────────────────────────────────
    # PANEL 3 — CO2 + PM2.5 (Time series multi-champs)
    # Mesure : pollution | Champs : co2_ppm, pm25 | Refresh : 10s
    # ─────────────────────────────────────────────────────────────────────────
    panel_pollution_ts = {
        "id":      3,
        "title":   "📊  CO₂ (ppm) & PM2.5 — Évolution temps réel",
        "type":    "timeseries",
        "gridPos": {"x": 0, "y": 8, "w": 12, "h": 8},
        "options": {
            "tooltip": {"mode": "multi"},
            "legend":  {"displayMode": "table", "placement": "right"},
        },
        "fieldConfig": {
            "defaults": {
                "lineWidth":   2,
                "fillOpacity": 8,
                "color": {"mode": "palette-classic"},
                "custom": {
                    "lineInterpolation": "smooth",
                    "showPoints": "never",
                },
            },
            "overrides": [
                {
                    "matcher": {"id": "byName", "options": "co2_ppm"},
                    "properties": [
                        {"id": "unit",         "value": "ppm"},
                        {"id": "displayName",  "value": "CO₂ (ppm)"},
                        {"id": "custom.axisPlacement", "value": "left"},
                    ],
                },
                {
                    "matcher": {"id": "byName", "options": "pm25"},
                    "properties": [
                        {"id": "unit",         "value": "µg/m³"},
                        {"id": "displayName",  "value": "PM2.5 (µg/m³)"},
                        {"id": "custom.axisPlacement", "value": "right"},
                    ],
                },
            ],
        },
        "targets": [
            flux_target("""
from(bucket: "smartcity")
  |> range(start: -30m)
  |> filter(fn: (r) => r._measurement == "pollution")
  |> filter(fn: (r) => r._field == "co2_ppm" or r._field == "pm25")
  |> aggregateWindow(every: 1m, fn: mean, createEmpty: false)
""")
        ],
    }

    # ─────────────────────────────────────────────────────────────────────────
    # PANEL 4 — Niveau des poubelles par zone (Bar gauge)
    # Mesure : dechets | Champ : fill_percent | Refresh : 30s
    # ─────────────────────────────────────────────────────────────────────────
    panel_dechets = {
        "id":      4,
        "title":   "🗑️  Poubelles — Niveau de remplissage par zone (%)",
        "type":    "bargauge",
        "gridPos": {"x": 12, "y": 8, "w": 12, "h": 8},
        "options": {
            "reduceOptions": {"calcs": ["lastNotNull"], "values": False},
            "orientation":  "horizontal",
            "displayMode":  "gradient",
            "minVizWidth":  0,
            "minVizHeight": 10,
        },
        "fieldConfig": {
            "defaults": {
                "unit":        "percent",
                "min":         0,
                "max":         100,
                "displayName": "${__field.labels.zone}",
                "color": {"mode": "thresholds"},
                "thresholds": {
                    "mode": "absolute",
                    "steps": [
                        {"value": None, "color": "green"},
                        {"value": 70,   "color": "yellow"},
                        {"value": 90,   "color": "red"},
                    ],
                },
            },
            "overrides": [],
        },
        "targets": [
            flux_target("""
from(bucket: "smartcity")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "dechets" and r._field == "fill_percent")
  |> last()
""")
        ],
    }

    # ─────────────────────────────────────────────────────────────────────────
    # PANEL 5 — Éclairage : pannes + consommation (Stat)
    # Mesure : eclairage | Champs : faulty_count, power_kw | Refresh : 15s
    # ─────────────────────────────────────────────────────────────────────────
    panel_eclairage = {
        "id":      5,
        "title":   "💡  Éclairage — Pannes & Consommation",
        "type":    "stat",
        "gridPos": {"x": 0, "y": 16, "w": 8, "h": 6},
        "options": {
            "reduceOptions":  {"calcs": ["lastNotNull"], "values": False},
            "orientation":    "auto",
            "colorMode":      "background",
            "graphMode":      "area",
            "textMode":       "auto",
            "justifyMode":    "auto",
        },
        "fieldConfig": {
            "defaults": {
                "color": {"mode": "thresholds"},
                "thresholds": {
                    "mode": "absolute",
                    "steps": [
                        {"value": None, "color": "green"},
                        {"value": 1,    "color": "red"},
                    ],
                },
            },
            "overrides": [
                {
                    "matcher": {"id": "byName", "options": "power_kw"},
                    "properties": [
                        {"id": "unit",        "value": "kwatt"},
                        {"id": "displayName", "value": "Consommation (kW)"},
                        {"id": "thresholds",  "value": {
                            "mode": "absolute",
                            "steps": [
                                {"value": None, "color": "green"},
                                {"value": 0.40, "color": "orange"},
                                {"value": 0.55, "color": "red"},
                            ],
                        }},
                    ],
                },
                {
                    "matcher": {"id": "byName", "options": "faulty_count"},
                    "properties": [
                        {"id": "displayName", "value": "Pannes détectées"},
                        {"id": "unit",        "value": "short"},
                    ],
                },
            ],
        },
        "targets": [
            flux_target("""
from(bucket: "smartcity")
  |> range(start: -30m)
  |> filter(fn: (r) => r._measurement == "eclairage")
  |> filter(fn: (r) => r._field == "faulty_count" or r._field == "power_kw")
  |> aggregateWindow(every: 5m, fn: mean, createEmpty: false)
  |> last()
""")
        ],
    }

    # ─────────────────────────────────────────────────────────────────────────
    # PANEL 6 — Alertes actives (Table)
    # Mesure : alertes | Tags : zone, niveau, type | Refresh : 5s
    # ─────────────────────────────────────────────────────────────────────────
    panel_alertes = {
        "id":      6,
        "title":   "🚨  Alertes actives — ORANGE & ROUGE (dernières 2h)",
        "type":    "table",
        "gridPos": {"x": 8, "y": 16, "w": 16, "h": 6},
        "options": {
            "showHeader": True,
            "sortBy":     [{"displayName": "Time", "desc": True}],
            "footer":     {"show": False},
        },
        "fieldConfig": {
            "defaults": {"custom": {"align": "left"}},
            "overrides": [
                {
                    "matcher": {"id": "byName", "options": "niveau"},
                    "properties": [
                        {
                            "id": "custom.displayMode",
                            "value": "color-background",
                        },
                        {
                            "id": "mappings",
                            "value": [
                                {
                                    "type": "value",
                                    "options": {
                                        "ROUGE":  {"color": "red",    "index": 0},
                                        "ORANGE": {"color": "orange", "index": 1},
                                    },
                                }
                            ],
                        },
                    ],
                },
            ],
        },
        "targets": [
            flux_target("""
from(bucket: "smartcity")
  |> range(start: -2h)
  |> filter(fn: (r) => r._measurement == "alertes")
  |> filter(fn: (r) => r.niveau == "ROUGE" or r.niveau == "ORANGE")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["_time", "zone", "niveau", "type", "valeur"])
  |> sort(columns: ["_time"], desc: true)
  |> limit(n: 50)
""")
        ],
        "transformations": [
            {
                "id": "organize",
                "options": {
                    "renameByName": {
                        "_time":  "Heure",
                        "zone":   "Zone",
                        "niveau": "Niveau",
                        "type":   "Capteur",
                        "valeur": "Valeur mesurée",
                    },
                },
            },
        ],
    }

    return [
        panel_aqi,
        panel_traffic,
        panel_pollution_ts,
        panel_dechets,
        panel_eclairage,
        panel_alertes,
    ]


# ═══════════════════════════════════════════════════════════════════════════════
# 4. CRÉATION DU DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════════

def create_dashboard(ds_uid: str) -> str | None:
    """
    Crée le dashboard Smart City via l'API Grafana.
    Retourne l'URL du dashboard créé.
    """
    dashboard_json = {
        "title":         "Smart City — Surveillance en temps réel",
        "uid":           "smartcity-main",
        "tags":          ["smartcity", "iot", "realtime"],
        "timezone":      "browser",
        "refresh":       "5s",
        "time":          {"from": "now-30m", "to": "now"},
        "schemaVersion": 38,
        "version":       1,
        "panels":        build_panels(ds_uid),
        "annotations":   {"list": []},
        "templating":    {"list": []},
        "links":         [],
    }

    payload = {
        "dashboard": dashboard_json,
        "overwrite": True,
        "folderId":  0,
        "message":   "Création automatique via grafana_setup.py (P5)",
    }

    r = requests.post(
        f"{GRAFANA_URL}/api/dashboards/db",
        auth=GRAFANA_AUTH,
        json=payload,
        headers={"Content-Type": "application/json"},
    )

    if r.status_code in (200, 201):
        resp = r.json()
        slug = resp.get("slug", "smartcity-surveillance-en-temps-reel")
        uid  = resp.get("uid", "smartcity-main")
        url  = f"{GRAFANA_URL}/d/{uid}/{slug}"
        print(f"✅ Dashboard créé : {url}")
        return url
    else:
        print(f"❌ Erreur dashboard : {r.status_code} — {r.text}")
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# 5. CONFIGURATION DES ALERTES GRAFANA
# ═══════════════════════════════════════════════════════════════════════════════

def create_alert_contact_point():
    """
    Crée un contact point email pour les alertes Grafana.
    Nécessite que GF_SMTP_* soit configuré dans docker-compose.yml.
    """
    payload = {
        "name": "SmartCity-Email",
        "type": "email",
        "settings": {
            "addresses": "equipe@smartcity.ma",
            "subject":   "🚨 [SmartCity] Alerte {{ .CommonLabels.niveau }} — {{ .CommonLabels.zone }}",
        },
        "disableResolveMessage": False,
    }

    r = requests.post(
        f"{GRAFANA_URL}/api/v1/provisioning/contact-points",
        auth=GRAFANA_AUTH,
        json=payload,
        headers={"Content-Type": "application/json"},
    )

    if r.status_code in (200, 202):
        print("✅ Contact point email configuré")
    else:
        # Non bloquant — SMTP optionnel
        print(f"⚠️  Contact point email ignoré (SMTP non configuré) : {r.status_code}")


# ═══════════════════════════════════════════════════════════════════════════════
# 6. MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("  Smart City — Configuration Grafana (P5)")
    print("=" * 60)

    # Étape 1 : Attendre Grafana
    if not wait_for_grafana():
        sys.exit(1)

    # Étape 2 : Créer la datasource InfluxDB
    print("\n[1/3] Création de la datasource InfluxDB...")
    ds_uid = create_datasource()
    if not ds_uid:
        print("❌ Impossible de continuer sans datasource.")
        sys.exit(1)

    # Étape 3 : Créer le dashboard
    print("\n[2/3] Création du dashboard...")
    dashboard_url = create_dashboard(ds_uid)

    # Étape 4 : Contact point email (optionnel)
    print("\n[3/3] Configuration du contact point email...")
    create_alert_contact_point()

    print("\n" + "=" * 60)
    print("  ✅ Configuration terminée !")
    print(f"  🌐 Dashboard : {dashboard_url}")
    print(f"  👤 Login     : admin / smartcity-admin")
    print("=" * 60)


if __name__ == "__main__":
    main()
