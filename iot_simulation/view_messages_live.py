# view_messages_live.py
from kafka import KafkaConsumer
import json

# Configuration
TOPICS = ['smartcity-pollution', 'smartcity-traffic', 'smartcity-lighting', 'smartcity-waste']

print("🔍 Connexion à Kafka...")
print("📡 Topics :", TOPICS)
print("🟢 En attente de messages en temps réel... (Ctrl+C pour arrêter)")
print("-" * 50)

# Créer un consommateur (sans from-beginning)
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers='localhost:9092',
    enable_auto_commit=False,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("✅ Connecté ! Les messages vont s'afficher au fur et à mesure :\n")

try:
    for message in consumer:
        print(f"\n📌 [{message.topic}] {message.value.get('zone', 'unknown')}")
        print(f"   {json.dumps(message.value, indent=2)}")
        print("-" * 50)
except KeyboardInterrupt:
    print("\n🛑 Arrêté par l'utilisateur")
finally:
    consumer.close()