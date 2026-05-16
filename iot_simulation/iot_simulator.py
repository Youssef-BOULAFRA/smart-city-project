# iot_simulator.py - Version avec fichier .env
import asyncio
import json
import random
from datetime import datetime
from azure.iot.device.aio import IoTHubDeviceClient
from dotenv import load_dotenv
import os

# Charger les variables d'environnement depuis .env
load_dotenv()

# ============================================
# Les chaînes de connexion sont chargées depuis le fichier .env
# ============================================

DEVICE_CONNECTION_STRINGS = {
    # Pollution
    "pollution_centre_ville": os.getenv("POLLUTION_CENTRE_VILLE"),
    "pollution_zone_industrielle": os.getenv("POLLUTION_ZONE_INDUSTRIELLE"),
    "pollution_residentiel": os.getenv("POLLUTION_RESIDENTIEL"),
    "pollution_peripherie": os.getenv("POLLUTION_PERIPHERIE"),
    
    # Trafic
    "traffic_centre_ville": os.getenv("TRAFFIC_CENTRE_VILLE"),
    "traffic_zone_industrielle": os.getenv("TRAFFIC_ZONE_INDUSTRIELLE"),
    "traffic_residentiel": os.getenv("TRAFFIC_RESIDENTIEL"),
    "traffic_peripherie": os.getenv("TRAFFIC_PERIPHERIE"),
    
    # Éclairage
    "lighting_centre_ville": os.getenv("LIGHTING_CENTRE_VILLE"),
    "lighting_zone_industrielle": os.getenv("LIGHTING_ZONE_INDUSTRIELLE"),
    "lighting_residentiel": os.getenv("LIGHTING_RESIDENTIEL"),
    "lighting_peripherie": os.getenv("LIGHTING_PERIPHERIE"),
    
    # Déchets
    "waste_centre_ville": os.getenv("WASTE_CENTRE_VILLE"),
    "waste_zone_industrielle": os.getenv("WASTE_ZONE_INDUSTRIELLE"),
    "waste_residentiel": os.getenv("WASTE_RESIDENTIEL"),
    "waste_peripherie": os.getenv("WASTE_PERIPHERIE"),
}

# Vérifier qu'aucune clé n'est manquante
for device_id, conn_str in DEVICE_CONNECTION_STRINGS.items():
    if not conn_str or conn_str == "xxx" or "xxx" in conn_str:
        print(f"⚠️ Attention: La chaîne de connexion pour {device_id} n'est pas valide (contient 'xxx')")

# Zones
ZONES = ['centre_ville', 'zone_industrielle', 'residentiel', 'peripherie']

class SmartCitySimulator:
    def __init__(self):
        self.clients = {}
        
    async def connect_devices(self):
        """Connecte tous les capteurs à Azure IoT Hub"""
        print("🔌 Connexion des capteurs à Azure IoT Hub...")
        for device_id, conn_str in DEVICE_CONNECTION_STRINGS.items():
            if not conn_str or "xxx" in conn_str:
                print(f"⚠️ Ignoré {device_id}: chaîne invalide")
                continue
            try:
                client = IoTHubDeviceClient.create_from_connection_string(conn_str)
                await client.connect()
                self.clients[device_id] = client
                print(f"✅ Connecté: {device_id}")
            except Exception as e:
                print(f"❌ Erreur connexion {device_id}: {e}")
        print(f"📡 {len(self.clients)} capteurs connectés\n")
    
    def get_pollution_data(self, zone, hour):
        """Génère des données de pollution"""
        base_pollution = {
            'centre_ville': 500,
            'zone_industrielle': 700,
            'residentiel': 400,
            'peripherie': 350
        }
        
        if hour in [8, 9, 17, 18]:
            coeff = 1.5
        else:
            coeff = 1.0
            
        co2 = base_pollution[zone] * coeff * random.uniform(0.8, 1.2)
        pm25 = (co2 / 15) * random.uniform(0.8, 1.2)
        
        if co2 > 800:
            aqi = random.randint(300, 500)
        elif co2 > 600:
            aqi = random.randint(150, 300)
        else:
            aqi = random.randint(50, 150)
            
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "device_id": f"pollution_{zone}",
            "zone": zone,
            "type": "pollution",
            "co2_ppm": round(co2, 1),
            "pm25": round(pm25, 1),
            "air_quality_index": aqi
        }
    
    def get_traffic_data(self, zone, hour):
        """Génère des données de trafic"""
        base_traffic = {
            'centre_ville': 60,
            'zone_industrielle': 45,
            'residentiel': 25,
            'peripherie': 15
        }
        
        if hour in [8, 17]:
            coeff = 3.0
        elif hour in [7, 9, 16, 18]:
            coeff = 2.0
        else:
            coeff = 0.8
            
        vehicles = int(base_traffic[zone] * coeff * random.uniform(0.8, 1.2))
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "device_id": f"traffic_{zone}",
            "zone": zone,
            "type": "traffic",
            "vehicles_per_min": vehicles,
            "avg_speed_kmh": round(max(10, min(80, 80 - vehicles/2)), 1),
            "congestion": "critical" if vehicles > 100 else "high" if vehicles > 70 else "medium" if vehicles > 40 else "low"
        }
    
    def get_lighting_data(self, zone, hour):
        """Génère des données d'éclairage"""
        is_night = hour < 6 or hour > 20
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "device_id": f"lighting_{zone}",
            "zone": zone,
            "type": "lighting",
            "luminosity_lux": random.randint(30, 80) if is_night else random.randint(300, 800),
            "status": "working" if random.random() > 0.05 else "faulty",
            "power_kw": round(random.uniform(0.3, 0.6) if is_night else random.uniform(0.05, 0.15), 2)
        }
    
    def get_waste_data(self, zone, hour):
        """Génère des données de déchets"""
        fill_percent = min(100, (hour * 100 / 24) * random.uniform(0.9, 1.1))
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "device_id": f"waste_{zone}",
            "zone": zone,
            "type": "waste",
            "fill_percent": round(fill_percent, 1),
            "weight_kg": round(fill_percent * 1.2, 1),
            "alert": fill_percent > 90
        }
    
    async def send_data(self):
        """Envoie les données vers Azure IoT Hub"""
        await self.connect_devices()
        
        if not self.clients:
            print("❌ Aucun capteur connecté. Vérifiez vos chaînes de connexion.")
            return
        
        message_count = 0
        try:
            while True:
                now = datetime.utcnow()
                hour = now.hour
                
                for zone in ZONES:
                    # Pollution
                    pollution_msg = self.get_pollution_data(zone, hour)
                    device_id = f"pollution_{zone}"
                    if device_id in self.clients:
                        await self.clients[device_id].send_message(json.dumps(pollution_msg))
                        print(f"📤 Pollution envoyée - {zone} (CO₂: {pollution_msg['co2_ppm']}ppm)")
                    
                    # Traffic
                    traffic_msg = self.get_traffic_data(zone, hour)
                    device_id = f"traffic_{zone}"
                    if device_id in self.clients:
                        await self.clients[device_id].send_message(json.dumps(traffic_msg))
                        print(f"📤 Trafic envoyé - {zone} (Véhicules: {traffic_msg['vehicles_per_min']}/min)")
                    
                    # Éclairage - toutes les 10 secondes (modulo 2)
                    if message_count % 2 == 0:
                        lighting_msg = self.get_lighting_data(zone, hour)
                        device_id = f"lighting_{zone}"
                        if device_id in self.clients:
                            await self.clients[device_id].send_message(json.dumps(lighting_msg))
                            print(f"📤 Éclairage envoyé - {zone}")
                    
                    # Déchets - toutes les 30 secondes (modulo 6)
                    if message_count % 6 == 0:
                        waste_msg = self.get_waste_data(zone, hour)
                        device_id = f"waste_{zone}"
                        if device_id in self.clients:
                            await self.clients[device_id].send_message(json.dumps(waste_msg))
                            print(f"📤 Déchets envoyés - {zone} (Remplissage: {waste_msg['fill_percent']}%)")
                
                message_count += 1
                print(f"--- Cycle {message_count} terminé ---\n")
                await asyncio.sleep(5)
                
        except KeyboardInterrupt:
            print("\n🛑 Simulation arrêtée")
        finally:
            for client in self.clients.values():
                await client.disconnect()
            print("🔒 Tous les capteurs déconnectés")

if __name__ == "__main__":
    simulator = SmartCitySimulator()
    asyncio.run(simulator.send_data())