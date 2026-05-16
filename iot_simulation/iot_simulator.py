# iot_simulator.py
import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# Configuration Kafka
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# Topics Kafka par type de capteur
TOPICS = {
    'traffic': 'smartcity-traffic',
    'pollution': 'smartcity-pollution',
    'lighting': 'smartcity-lighting',
    'waste': 'smartcity-waste'
}

# Zones de la ville
ZONES = ['centre_ville', 'zone_industrielle', 'residentiel', 'peripherie']

class SensorSimulator:
    def __init__(self):
        """Initialise le producteur Kafka"""
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("✅ Connecté à Kafka")
    
    def get_traffic_data(self, zone, hour):
        """Génère des données de trafic réalistes"""
        # Base de trafic par zone
        base_traffic = {
            'centre_ville': 60,
            'zone_industrielle': 45,
            'residentiel': 25,
            'peripherie': 15
        }
        
        # Coefficient selon l'heure (heures de pointe)
        if hour in [8, 17]:  # Pic maximum
            coeff = 3.0
        elif hour in [7, 9, 16, 18]:  # Heures de demi-pointe
            coeff = 2.0
        elif 10 <= hour <= 15:  # Heures creuses
            coeff = 0.8
        elif 20 <= hour <= 23:  # Soirée
            coeff = 0.5
        else:  # Nuit
            coeff = 0.2
            
        vehicles = int(base_traffic[zone] * coeff * random.uniform(0.8, 1.2))
        
        # Vitesse moyenne inversement proportionnelle au trafic
        speed = max(10, min(80, 80 - (vehicles / 2)))
        
        # Niveau de congestion
        if vehicles > 100:
            congestion = 'critical'
        elif vehicles > 70:
            congestion = 'high'
        elif vehicles > 40:
            congestion = 'medium'
        else:
            congestion = 'low'
            
        return {
            'vehicles_per_min': vehicles,
            'avg_speed_kmh': round(speed, 1),
            'congestion': congestion
        }
    
    def get_pollution_data(self, zone, hour):
        """Génère des données de pollution réalistes"""
        # Base de pollution par zone
        base_co2 = {
            'centre_ville': 500,
            'zone_industrielle': 700,
            'residentiel': 400,
            'peripherie': 350
        }
        
        # Coefficient selon l'heure (pollution plus élevée aux heures de pointe)
        if hour in [8, 9, 17, 18]:
            coeff = 1.4
        elif 10 <= hour <= 16:
            coeff = 1.1
        else:
            coeff = 0.9
            
        co2 = base_co2[zone] * coeff * random.uniform(0.85, 1.15)
        
        # PM2.5 corrélé au CO2
        pm25 = (co2 / 15) * random.uniform(0.8, 1.2)
        
        # AQI (Air Quality Index)
        if co2 > 800:
            aqi = random.randint(300, 500)
        elif co2 > 600:
            aqi = random.randint(150, 300)
        else:
            aqi = random.randint(50, 150)
            
        return {
            'co2_ppm': round(co2, 1),
            'pm25': round(pm25, 1),
            'air_quality_index': aqi
        }
    
    def get_lighting_data(self, zone, hour):
        """Génère des données d'éclairage public"""
        # Nuit = éclairage allumé
        is_night = hour < 6 or hour > 20
        
        if is_night:
            luminosity = random.randint(30, 80)  # lux
            power = round(random.uniform(0.3, 0.6), 2)  # kW
            status = 'working' if random.random() > 0.03 else 'faulty'  # 3% de panne
        else:
            luminosity = random.randint(300, 800)  # lumière naturelle
            power = round(random.uniform(0.05, 0.15), 2)
            status = 'working'
            
        return {
            'luminosity_lux': luminosity,
            'status': status,
            'power_kw': power
        }
    
    def get_waste_data(self, zone, hour):
        """Génère des données de remplissage des poubelles"""
        # Remplissage progresse sur la journée
        base_fill = (hour * 100 / 24) * random.uniform(0.9, 1.1)
        fill_percent = min(100, max(0, base_fill))
        
        # Poids proportionnel au remplissage
        weight = fill_percent * 1.2
        
        # Alerte si trop plein
        alert = fill_percent > 90
        
        return {
            'fill_percent': round(fill_percent, 1),
            'weight_kg': round(weight, 1),
            'alert': alert
        }
    
    def send_data(self):
        """Envoie les données pour tous les capteurs et zones"""
        print("🚀 Démarrage de la simulation Smart City...")
        print(f"📡 Envoi vers Kafka : {KAFKA_BOOTSTRAP_SERVERS}")
        print("-" * 50)
        
        message_count = 0
        
        try:
            while True:
                now = datetime.utcnow()
                timestamp = now.isoformat()
                hour = now.hour
                
                for zone in ZONES:
                    # Trafic (toutes les 5 secondes)
                    traffic_data = self.get_traffic_data(zone, hour)
                    traffic_msg = {
                        'timestamp': timestamp,
                        'device_id': f'traffic_{zone}',
                        'zone': zone,
                        'type': 'traffic',
                        **traffic_data
                    }
                    self.producer.send(TOPICS['traffic'], traffic_msg)
                    
                    # Pollution (toutes les 5 secondes)
                    pollution_data = self.get_pollution_data(zone, hour)
                    pollution_msg = {
                        'timestamp': timestamp,
                        'device_id': f'pollution_{zone}',
                        'zone': zone,
                        'type': 'pollution',
                        **pollution_data
                    }
                    self.producer.send(TOPICS['pollution'], pollution_msg)
                    
                    # Éclairage (toutes les 10 secondes)
                    if message_count % 2 == 0:
                        lighting_data = self.get_lighting_data(zone, hour)
                        lighting_msg = {
                            'timestamp': timestamp,
                            'device_id': f'lighting_{zone}',
                            'zone': zone,
                            'type': 'lighting',
                            **lighting_data
                        }
                        self.producer.send(TOPICS['lighting'], lighting_msg)
                    
                    # Déchets (toutes les 30 secondes)
                    if message_count % 6 == 0:
                        waste_data = self.get_waste_data(zone, hour)
                        waste_msg = {
                            'timestamp': timestamp,
                            'device_id': f'waste_{zone}',
                            'zone': zone,
                            'type': 'waste',
                            **waste_data
                        }
                        self.producer.send(TOPICS['waste'], waste_msg)
                
                self.producer.flush()
                message_count += 1
                print(f"📤 [{message_count}] Messages envoyés pour {timestamp}")
                time.sleep(5)  # Boucle toutes les 5 secondes
                
        except KeyboardInterrupt:
            print("\n🛑 Simulation arrêtée par l'utilisateur")
        finally:
            self.producer.close()
            print("🔒 Producteur Kafka fermé")

if __name__ == "__main__":
    simulator = SensorSimulator()
    simulator.send_data()