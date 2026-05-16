# iot_to_kafka.py
import json
from kafka import KafkaProducer
from azure.iot.hub import IoTHubRegistryManager
from azure.iot.hub.models import CloudToDeviceMethod
import time
import os

# Configuration Kafka
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# Topics Kafka
TOPICS = {
    'traffic': 'smartcity-traffic',
    'pollution': 'smartcity-pollution',
    'lighting': 'smartcity-lighting',
    'waste': 'smartcity-waste'
}

# Configuration Azure IoT Hub (à remplacer avec vos vraies valeurs)
# À obtenir depuis le Portail Azure → IoT Hub → Shared access policies → iothubowner
AZURE_IOT_HUB_CONNECTION_STRING = os.environ.get('IOTHUB_CONNECTION_STRING', 'HostName=your-hub.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=your-key')

class IoTToKafkaBridge:
    def __init__(self):
        """Initialise le producteur Kafka et la connexion Azure"""
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("✅ Connecté à Kafka")
        
        # Connexion à Azure IoT Hub
        try:
            self.registry_manager = IoTHubRegistryManager(AZURE_IOT_HUB_CONNECTION_STRING)
            print("✅ Connecté à Azure IoT Hub")
        except Exception as e:
            print(f"⚠️ Erreur connexion Azure: {e}")
            print("Mode démo: simulation locale uniquement")
            self.registry_manager = None
    
    def detect_sensor_type(self, device_id):
        """Détecte le type de capteur à partir de l'ID du device"""
        device_id_lower = device_id.lower()
        if 'traffic' in device_id_lower:
            return 'traffic'
        elif 'pollution' in device_id_lower or 'air' in device_id_lower:
            return 'pollution'
        elif 'lighting' in device_id_lower or 'light' in device_id_lower:
            return 'lighting'
        elif 'waste' in device_id_lower or 'bin' in device_id_lower:
            return 'waste'
        else:
            return None
    
    def process_message(self, message, device_id, timestamp):
        """Traite un message reçu et l'envoie vers Kafka"""
        try:
            # Convertir le message en dict si nécessaire
            if isinstance(message, str):
                data = json.loads(message)
            else:
                data = message
            
            # Détecter le type de capteur
            sensor_type = self.detect_sensor_type(device_id)
            
            # Si type inconnu, essayer de le déduire du contenu
            if not sensor_type:
                if 'co2_ppm' in data or 'air_quality_index' in data:
                    sensor_type = 'pollution'
                elif 'vehicles_per_min' in data or 'congestion' in data:
                    sensor_type = 'traffic'
                elif 'fill_percent' in data or 'weight_kg' in data:
                    sensor_type = 'waste'
                elif 'luminosity_lux' in data or 'power_kw' in data:
                    sensor_type = 'lighting'
                else:
                    print(f"⚠️ Type inconnu pour device {device_id}")
                    return
            
            # Enrichir le message
            enriched_message = {
                'timestamp': timestamp,
                'device_id': device_id,
                'type': sensor_type,
                **data
            }
            
            # Envoyer vers le topic Kafka correspondant
            topic = TOPICS.get(sensor_type, f'smartcity-{sensor_type}')
            self.producer.send(topic, enriched_message)
            self.producer.flush()
            
            print(f"✅ [{timestamp}] {device_id} → {topic}")
            
        except Exception as e:
            print(f"❌ Erreur traitement message: {e}")
    
    def simulate_messages_for_test(self):
        """Génère des messages de test sans Azure (pour développement)"""
        print("📡 Mode test: génération de messages simulés...")
        
        ZONES = ['centre_ville', 'zone_industrielle', 'residentiel', 'peripherie']
        count = 0
        
        try:
            while True:
                timestamp = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
                
                for zone in ZONES:
                    # Simuler trafic
                    self.process_message(
                        {'vehicles_per_min': 45, 'avg_speed_kmh': 35.5, 'congestion': 'medium'},
                        f'traffic_{zone}',
                        timestamp
                    )
                    time.sleep(0.5)
                    
                    # Simuler pollution
                    self.process_message(
                        {'co2_ppm': 550.0, 'pm25': 42.3, 'air_quality_index': 180},
                        f'pollution_{zone}',
                        timestamp
                    )
                    time.sleep(0.5)
                    
                count += 1
                print(f"\n--- Cycle {count} terminé ---\n")
                
        except KeyboardInterrupt:
            print("\n🛑 Bridge arrêté")
        finally:
            self.producer.close()
    
    def run(self):
        """Lance le bridge (version réelle avec Azure)"""
        print("🚀 Démarrage du bridge IoT Hub → Kafka")
        
        if self.registry_manager:
            print("Mode: Connexion réelle à Azure IoT Hub")
            # Ici, implémenter la lecture des messages depuis IoT Hub
            # Via Event Hub compatible endpoint
            print("⚠️ Configuration supplémentaire nécessaire pour Event Hub")
        else:
            print("Mode: Test local (sans Azure)")
            self.simulate_messages_for_test()

if __name__ == "__main__":
    bridge = IoTToKafkaBridge()
    bridge.run()