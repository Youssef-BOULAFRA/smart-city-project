# kafka_api.py
from flask import Flask, jsonify, send_file
from flask_cors import CORS
from kafka import KafkaConsumer
import json
import threading
import os

app = Flask(__name__)
CORS(app)

latest_messages = {
    'pollution': [],
    'traffic': [],
    'lighting': [],
    'waste': []
}

def consume_kafka():
    try:
        consumer = KafkaConsumer(
            'smartcity-pollution', 'smartcity-traffic', 'smartcity-lighting', 'smartcity-waste',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        print("✅ Connecté à Kafka, en attente des messages...")
        
        for msg in consumer:
            topic = msg.topic.replace('smartcity-', '')
            data = msg.value
            latest_messages[topic].insert(0, data)
            if len(latest_messages[topic]) > 50:
                latest_messages[topic].pop()
            print(f"📥 Message reçu: {topic} - {data.get('zone', 'unknown')}")
            
    except Exception as e:
        print(f"❌ Erreur Kafka: {e}")

@app.route('/api/messages')
def get_messages():
    return jsonify(latest_messages)

@app.route('/')
def index():
    return send_file('kafka_viewer.html')

if __name__ == '__main__':
    print("🚀 Démarrage du serveur API Kafka...")
    thread = threading.Thread(target=consume_kafka, daemon=True)
    thread.start()
    app.run(host='0.0.0.0', port=5000, debug=False)