# iot_to_kafka_bridge.py - Lit depuis Azure IoT Hub et envoie vers Kafka
from azure.eventhub import EventHubConsumerClient
from kafka import KafkaProducer
import json
import os

# Configuration Kafka
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# Configuration Azure Event Hub (compatible IoT Hub)
# À récupérer depuis Azure Portal → IoT Hub → Built-in endpoints
# Cliquez sur "Built-in endpoints" → Copiez "Event Hub-compatible endpoint"
EVENT_HUB_CONNECTION_STRING = "Endpoint=sb://ihsuprodparres010dednamespace.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=cnPlPBT3Rq95QeK0dK1slDXWk6jtWfjbkAIoTDpnM6E=;EntityPath=iothub-ehub-smartcityh-55757095-4fc13d48b9"

# Topics Kafka par type
TOPICS = {
    'pollution': 'smartcity-pollution',
    'traffic': 'smartcity-traffic',
    'lighting': 'smartcity-lighting',
    'waste': 'smartcity-waste'
}

class IoTToKafkaBridge:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("✅ Connecté à Kafka")
        
    def get_topic_from_device(self, device_id):
        """Détermine le topic Kafka à partir du device_id"""
        if 'pollution' in device_id:
            return TOPICS['pollution']
        elif 'traffic' in device_id:
            return TOPICS['traffic']
        elif 'lighting' in device_id:
            return TOPICS['lighting']
        elif 'waste' in device_id:
            return TOPICS['waste']
        return None
    
    def on_event(self, partition_context, event):
        """Callback appelé pour chaque message reçu d'IoT Hub"""
        try:
            # Décoder le message
            message_body = event.body_as_str()
            data = json.loads(message_body)
            
            device_id = data.get('device_id', 'unknown')
            topic = self.get_topic_from_device(device_id)
            
            if topic:
                # Envoyer vers Kafka
                self.producer.send(topic, data)
                self.producer.flush()
                print(f"✅ Bridge: {device_id} → {topic}")
            else:
                print(f"⚠️ Topic inconnu pour device: {device_id}")
                
        except Exception as e:
            print(f"❌ Erreur: {e}")
    
    def run(self):
        """Lance le bridge"""
        print("🚀 Démarrage du bridge IoT Hub → Kafka")
        print("📡 En attente des messages depuis Azure IoT Hub...")
        
        client = EventHubConsumerClient.from_connection_string(
            EVENT_HUB_CONNECTION_STRING,
            consumer_group="$Default"
        )
        
        try:
            client.receive(
                on_event=self.on_event,
                starting_position="-1"  # Lire depuis le début
            )
        except KeyboardInterrupt:
            print("\n🛑 Bridge arrêté")
        finally:
            client.close()
            self.producer.close()

if __name__ == "__main__":
    bridge = IoTToKafkaBridge()
    bridge.run()