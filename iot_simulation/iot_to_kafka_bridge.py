# iot_to_kafka_bridge.py - Lit depuis Azure IoT Hub et envoie vers Kafka
import os
import json
from dotenv import load_dotenv
from azure.eventhub import EventHubConsumerClient
from kafka import KafkaProducer

# Charger .env depuis le même dossier
env_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(env_path)

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9093'

EVENT_HUB_CONNECTION_STRING = os.environ.get('EVENT_HUB_CONNECTION_STRING', '')

TOPICS = {
    'pollution': 'smartcity-pollution',
    'traffic': 'smartcity-traffic',
    'eclairage': 'smartcity-eclairage',
    'dechets': 'smartcity-dechets'
}

class IoTToKafkaBridge:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("✅ Connecté à Kafka")

    def get_topic_from_device(self, device_id):
        did = device_id.lower()
        if 'pollution' in did:
            return TOPICS['pollution']
        elif 'traffic' in did:
            return TOPICS['traffic']
        elif 'eclairage' in did or 'lighting' in did:
            return TOPICS['eclairage']
        elif 'dechets' in did or 'waste' in did:
            return TOPICS['dechets']
        return None

    def on_event(self, partition_context, event):
        try:
            message_body = event.body_as_str()
            data = json.loads(message_body)

            # device_id vient des propriétés système IoT Hub (fiable)
            device_id = event.system_properties.get(
                b"iothub-connection-device-id", b"unknown"
            ).decode()

            if device_id == "unknown" and "device_id" in data:
                device_id = data["device_id"]

            topic = self.get_topic_from_device(device_id)

            if topic:
                self.producer.send(topic, data)
                self.producer.flush()
                print(f"✅ Bridge: {device_id} → {topic}")
            else:
                print(f"⚠️ Topic inconnu pour device: {device_id}")

        except Exception as e:
            print(f"❌ Erreur: {e}")

    def run(self):
        print("🚀 Démarrage du bridge IoT Hub → Kafka")
        print("📡 En attente des messages depuis Azure IoT Hub...")

        if not EVENT_HUB_CONNECTION_STRING:
            print("❌ EVENT_HUB_CONNECTION_STRING manquante dans .env")
            return

        client = EventHubConsumerClient.from_connection_string(
            EVENT_HUB_CONNECTION_STRING,
            consumer_group="$Default"
        )

        try:
            client.receive(
                on_event=self.on_event,
                starting_position="@latest"
            )
        except KeyboardInterrupt:
            print("\n🛑 Bridge arrêté")
        finally:
            client.close()
            self.producer.close()

if __name__ == "__main__":
    bridge = IoTToKafkaBridge()
    bridge.run()