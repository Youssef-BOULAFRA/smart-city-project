# Cahier des Règles d'Alertes Intelligentes
## Projet Smart City — P3 Spark Streaming
### Système de Surveillance d'une Ville Connectée

---

## 1. Principes généraux

### Niveaux d'alerte

| Niveau | Couleur | Signification | Action requise |
|--------|---------|---------------|----------------|
| VERT   | 🟢      | Fonctionnement normal — aucune anomalie | Aucune |
| ORANGE | 🟠      | Anomalie détectée — surveillance requise | Notification préventive |
| ROUGE  | 🔴      | Situation critique — intervention urgente | Notification immédiate + action |

### Condition de déclenchement ROUGE

> Conformément au cahier de charge : **une alerte ROUGE est déclenchée uniquement si la valeur dépasse le seuil critique en moyenne sur une fenêtre glissante de 5 minutes.** Cela garantit l'absence de faux positifs sur des pics instantanés.

### Fenêtres temporelles

| Fenêtre | Slide | Usage |
|---------|-------|-------|
| 5 minutes | 1 minute | Alertes opérationnelles (ORANGE/ROUGE) |
| 15 minutes | 5 minutes | Tendances analytiques → Grafana P5 |

---

## 2. Règles — Capteurs Pollution

### Normes de référence
- **OMS (2021)** : PM2.5 < 15 µg/m³ (moyenne annuelle), pic journalier < 37.5 µg/m³
- **Norme EU** : NO2 < 40 µg/m³ annuel
- **AQI EPA** : 0-50 Bon, 51-100 Modéré, 101-150 Mauvais pour groupes sensibles, 151-200 Mauvais, 201-300 Très mauvais, 301-500 Dangereux

### Tableau des seuils

| Paramètre | VERT (normal) | ORANGE (surveillance) | ROUGE (critique) | Source |
|-----------|--------------|----------------------|-----------------|--------|
| CO2 (ppm) | ≤ 600 | 601 – 800 | ≥ 801 | Valeur de référence intérieure |
| PM2.5 (µg/m³) | ≤ 35 | 36 – 75 | ≥ 76 | OMS + EPA AQI |
| AQI | ≤ 150 | 151 – 300 | ≥ 301 | EPA Air Quality Index |

### Règles de déclenchement

| # | Condition | Durée minimale | Niveau | Message |
|---|-----------|---------------|--------|---------|
| P1 | avg_co2 ≥ 801 ppm | 5 minutes | 🔴 ROUGE | "CRITIQUE pollution - CO2 élevé 5min - Service Environnement" |
| P2 | avg_pm25 ≥ 76 µg/m³ | 5 minutes | 🔴 ROUGE | "CRITIQUE pollution - PM2.5 dangereux - Service Environnement" |
| P3 | avg_aqi ≥ 301 | 5 minutes | 🔴 ROUGE | "AQI dangereux - Population à risque - Alerte publique" |
| P4 | avg_co2 ≥ 601 ppm | 5 minutes | 🟠 ORANGE | "Élévation CO2 - Surveillance renforcée" |
| P5 | avg_pm25 ≥ 36 µg/m³ | 5 minutes | 🟠 ORANGE | "Élévation PM2.5 - Surveiller l'évolution" |
| P6 | avg_aqi ≥ 151 | 5 minutes | 🟠 ORANGE | "Qualité d'air dégradée - Surveillance active" |

### Condition de résolution
Spark Streaming émet un nouvel état à chaque slide de fenêtre (1 minute). La résolution est **implicite** : dès que la moyenne 5 min repasse sous le seuil ORANGE, plus aucun message n'est publié dans `smartcity-alerts` pour cette zone. Côté tableau de bord Grafana (P5), le panel d'alerte se vide automatiquement après 2 fenêtres consécutives sans nouveau message (10 minutes), confirmant le retour à la normale.

---

## 3. Règles — Capteurs Trafic

### Normes de référence
- **Code de la route** : vitesse minimale autoroute 80 km/h, circulation critique < 10 km/h
- **Définition congestion** : > 60 véhicules/min sur voie urbaine = saturation

### Tableau des seuils

| Paramètre | VERT (normal) | ORANGE (surveillance) | ROUGE (critique) |
|-----------|--------------|----------------------|-----------------|
| Véhicules/min | ≤ 40 | 41 – 60 | ≥ 61 |
| Vitesse moy (km/h) | > 30 | 11 – 30 | ≤ 10 |

### Règles de déclenchement

| # | Condition | Durée minimale | Niveau | Message |
|---|-----------|---------------|--------|---------|
| T1 | avg_vehicules ≥ 61/min OU avg_vitesse ≤ 10 km/h | 5 minutes | 🔴 ROUGE | "Chaos routier soutenu - Dérivation obligatoire - Police municipale" |
| T2 | avg_vehicules ≥ 41/min OU avg_vitesse ≤ 30 km/h | 5 minutes | 🟠 ORANGE | "Embouteillage détecté - Ralentissement en cours" |

---

## 4. Règles — Capteurs Déchets

### Normes de référence
- **Standard municipal** : collecte déclenchée à 90% de remplissage
- **Prévention odeurs** : intervention préventive à 75%

### Tableau des seuils

| Paramètre | VERT (normal) | ORANGE (surveillance) | ROUGE (critique) |
|-----------|--------------|----------------------|-----------------|
| Remplissage (%) | ≤ 75 | 76 – 90 | ≥ 91 |
| Poids (kg) | ≤ 60 | 61 – 80 | ≥ 81 |

### Règles de déclenchement

| # | Condition | Durée minimale | Niveau | Message |
|---|-----------|---------------|--------|---------|
| D1 | avg_remplissage ≥ 91% OU avg_poids ≥ 81 kg | 5 minutes | 🔴 ROUGE | "Poubelle saturée - Collecte immédiate - Notification Voirie" |
| D2 | avg_remplissage ≥ 76% OU avg_poids ≥ 61 kg | 5 minutes | 🟠 ORANGE | "Poubelle presque pleine - Planifier collecte sous 2h" |

---

## 5. Règles — Capteurs Éclairage

### Normes de référence
- **Norme EN 13201** : éclairage voie publique min 10 lux
- **Consommation LED urbaine** : 0.3 – 0.6 kW nominal par point lumineux. Une consommation ≥ 0.55 kW indique un court-circuit ou une défaillance probable.
- **Statut embarqué** : le capteur remonte directement un champ `status = "faulty"` lorsqu'il détecte sa propre panne matérielle. Cette information est plus fiable qu'une dérive indirecte.

> Note : les seuils consommation ont été recalibrés sur la plage réelle du simulateur (0.05 – 0.60 kW). Les valeurs initiales (5 / 8 kW) étaient hors plage et n'auraient jamais déclenché d'alerte.

### Tableau des seuils

| Paramètre | VERT (normal) | ORANGE (surveillance) | ROUGE (critique) |
|-----------|--------------|----------------------|-----------------|
| Luminosité (lux) | > 10 | 6 – 10 | ≤ 5 |
| Consommation (kW) | < 0.40 | 0.40 – 0.54 | ≥ 0.55 |
| Statut capteur | working | — | nb_faulty ≥ 1 |

### Règles de déclenchement

| # | Condition | Durée minimale | Niveau | Message |
|---|-----------|---------------|--------|---------|
| E0 | nb_faulty ≥ 1 sur la fenêtre | 5 minutes | 🔴 ROUGE | "Panne éclairage détectée (status=faulty) - Intervention technique urgente" |
| E1 | avg_luminosite ≤ 5 lux OU avg_conso ≥ 0.55 kW | 5 minutes | 🔴 ROUGE | "Panne éclairage soutenue - Intervention technique urgente" |
| E2 | avg_luminosite ≤ 10 lux OU avg_conso ≥ 0.40 kW | 5 minutes | 🟠 ORANGE | "Anomalie éclairage - Surveillance nécessaire" |

---

## 6. Matrice de routage des alertes

> Qui reçoit quoi selon le type et le niveau d'alerte.

| Type capteur | Niveau | Destinataire | Canal | Délai max |
|-------------|--------|-------------|-------|-----------|
| Pollution | 🔴 ROUGE | Service Environnement + Mairie | Email + SMS | < 2 min |
| Pollution | 🟠 ORANGE | Service Environnement | Dashboard Grafana | < 5 min |
| Trafic | 🔴 ROUGE | Police municipale + Service Voirie | SMS + Radio | < 1 min |
| Trafic | 🟠 ORANGE | Centre de régulation trafic | Dashboard Grafana | < 5 min |
| Déchets | 🔴 ROUGE | Service de Voirie (collecte) | Email + App mobile | < 10 min |
| Déchets | 🟠 ORANGE | Planification Voirie | Dashboard Grafana | < 30 min |
| Éclairage | 🔴 ROUGE | Service technique municipal | Email + Astreinte | < 15 min |
| Éclairage | 🟠 ORANGE | Service technique municipal | Dashboard Grafana | < 1h |

### Flux technique de notification

```
Capteur IoT
    ↓ (MQTT / 5s)
Azure IoT Hub
    ↓
Apache Kafka (topics sources)
    ↓
Spark Streaming (fenêtre 5min)
    ↓
topic smartcity-alerts  ──────────────→  Grafana (panel alertes actives)
    ↓                                         ↓
InfluxDB (métriques)               Webhook → Email / SMS
```

---

## 7. Scénarios de test pour la soutenance

| Scénario | Action dans simulateur | Résultat attendu |
|----------|----------------------|-----------------|
| Pic pollution zone industrielle | `co2_ppm = 950` pendant 6 min | Alerte ORANGE à T+1min, ROUGE à T+5min dans Kafka |
| Embouteillage centre-ville | `vehicles_per_min = 80` | Alerte ROUGE traffic en < 6 min |
| Panne éclairage résidentiel | `luminosity_lux = 2` | Alerte ROUGE éclairage en < 6 min |
| Poubelle pleine zone industrielle | `fill_percent = 95` | Alerte ROUGE déchets en < 6 min |
| Retour à la normale | Valeurs normales | Disparition des alertes après 2 fenêtres (10 min) |
