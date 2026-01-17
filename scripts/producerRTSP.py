import time
import json
import csv
import requests
import sys
from kafka import KafkaProducer
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# ================= CONFIGURATION =================
API_URL = "http://viomobilenet_api:8000"
KAFKA_BROKER = "kafka:9092" 
KAFKA_TOPIC = "urban-safety-alerts"
METADATA_FILE = "./data/metadata/camera_registry.csv"

HEARTBEAT_INTERVAL = 5.0
ALERT_INTERVAL = 0.5

def json_serializer(data):
    return json.dumps(data).encode("utf-8")

producer = None 
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=json_serializer,
        linger_ms=10, 
        acks=1
    )
    print(f"Connected to Kafka successfully at {KAFKA_BROKER}")
except Exception as e:
    print(f"Fatal Kafka Error: {e}")
    sys.exit(1)

# ================= PROCESSING FUNCTIONS =================

def load_camera_registry(csv_path):
    registry = {}
    try:
        with open(csv_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    row['latitude'] = float(row['latitude'])
                    row['longitude'] = float(row['longitude'])
                except: pass
                registry[row['camera_id']] = row
        print(f"Loaded {len(registry)} cameras from CSV.")
        return registry
    except Exception as e:
        print(f"CSV Read Error: {e}")
        return {}

last_sent_time = {}

def process_camera(cam_id, cam_metadata):
    global last_sent_time
    
    try:
        # 1. Call API to get status
        try:
            resp = requests.get(f"{API_URL}/camera/status/{cam_id}", timeout=2)
        except requests.exceptions.RequestException as e:
            # print(f"[{cam_id}] API CONNECTION ERROR: {e}")
            return

        if resp.status_code != 200:
            return

        ai_data = resp.json()
        
        # --- DEBUG LOGGING ---
        debug_data = ai_data.copy()
        debug_data['image_preview'] = '[REDACTED]' if debug_data.get('image_preview') else None
        print(f"[{cam_id}] RECEIVED AI DATA: {json.dumps(debug_data, indent=2)}")
        
        # 2. Check offline status
        if ai_data.get("status") == "offline" or ai_data.get("status") == "error":
            return

        # 3. Get core data
        is_violent = ai_data.get("is_violent", False)
        score = ai_data.get("score")
        if score is None:
             score = ai_data.get("fight_prob", 0.0)
        score = float(score)

        # 4. Check send frequency
        now = time.time()
        last_time = last_sent_time.get(cam_id, 0)
        interval = ALERT_INTERVAL if is_violent else HEARTBEAT_INTERVAL
        
        if now - last_time < interval:
            return 

        # 5. ENRICH DATA
        enriched_data = cam_metadata.copy()
        enriched_data.update({
            "timestamp": datetime.now().isoformat(),
            "ai_timestamp": ai_data.get("timestamp"),
            "is_violent": is_violent,
            "score": score,
            "fps": ai_data.get("fps", 0),
            "latency_ms": ai_data.get("latency_ms", 0),
            "image_preview": ai_data.get("image_preview", ""),
            "evidence_url": ai_data.get("evidence_url", None)
        })

        if producer:
            producer.send(KAFKA_TOPIC, value=enriched_data)
            last_sent_time[cam_id] = now
            status_icon = "VIOLENCE" if is_violent else "Normal"
            print(f"Sent Kafka [{cam_id}] {status_icon} | Score: {score:.4f} | FPS: {enriched_data['fps']}")

    except Exception as e:
        print(f"Error processing {cam_id}: {e}")


def main():
    registry = load_camera_registry(METADATA_FILE)
    if not registry: return
    
    print(f"\nProducer is running... Monitoring topic: {KAFKA_TOPIC}")
    print(f"Fetching data from: {API_URL}\n")

    # Limit threads to match number of cameras
    executor = ThreadPoolExecutor(max_workers=10)

    try:
        while True:
            for cam_id, meta in registry.items():
                executor.submit(process_camera, cam_id, meta)
            time.sleep(0.2) 
            
    except KeyboardInterrupt:
        print("\nStopping Producer...")
        executor.shutdown(wait=False)
        if producer:
            producer.close()
        print("Done.")

if __name__ == "__main__":
    main()