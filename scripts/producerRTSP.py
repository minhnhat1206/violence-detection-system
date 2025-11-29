import time
import json
import csv
import requests
import sys
from kafka import KafkaProducer
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# ================= CẤU HÌNH =================
API_URL = "http://103.78.3.30:8000"
KAFKA_BROKER = "http://kafka:9092"
KAFKA_TOPIC = "urban-safety-alerts"
METADATA_FILE = "./data/metadata/camera_registry.csv"

# Cấu hình tần suất gửi tin
HEARTBEAT_INTERVAL = 5.0  # Bình thường: 5s/tin
ALERT_INTERVAL = 0.5      # Bạo lực: 0.5s/tin

# ================= KAFKA SETUP =================
def json_serializer(data):
    return json.dumps(data).encode("utf-8")

try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=json_serializer,
        linger_ms=10, 
        acks=1
    )
    print(f"Kết nối Kafka thành công tới {KAFKA_BROKER}")
except Exception as e:
    print(f"Lỗi Fatal Kafka: {e}")
    sys.exit(1)

# ================= HÀM XỬ LÝ =================

def load_camera_registry(csv_path):
    registry = {}
    try:
        with open(csv_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Ép kiểu float để tránh lỗi JSON sau này
                try:
                    row['latitude'] = float(row['latitude'])
                    row['longitude'] = float(row['longitude'])
                except: pass # Nếu lỗi tọa độ thì giữ nguyên string
                registry[row['camera_id']] = row
        print(f"Đã load {len(registry)} camera từ CSV.")
        return registry
    except Exception as e:
        print(f"Lỗi đọc CSV: {e}")
        return {}

def start_ai_processing(camera_list):
    print("Đang kích hoạt AI cho các Camera...")
    for cam_id, info in camera_list.items():
        rtsp_url = info['rtsp_url']
        try:
            # Gọi API start
            response = requests.post(
                f"{API_URL}/camera/start",
                params={"camera_id": cam_id, "rtsp_url": rtsp_url},
                timeout=3
            )
            if response.status_code == 200:
                print(f"   + {cam_id}: Started")
            elif "already running" in response.text:
                print(f"   = {cam_id}: Running")
            else:
                print(f"   - {cam_id}: Failed ({response.text})")
        except Exception as e:
            print(f"   ! {cam_id}: Error API ({e})")

# Biến lưu thời gian gửi cuối cùng
last_sent_time = {}

def process_camera(cam_id, cam_metadata):
    global last_sent_time
    
    try:
        # 1. Gọi API lấy trạng thái
        try:
            resp = requests.get(f"{API_URL}/camera/status/{cam_id}", timeout=2)
        except requests.exceptions.RequestException:
            # Nếu API chết hoặc timeout, bỏ qua vòng này
            return

        if resp.status_code != 200:
            print(f"[{cam_id}] API Error: {resp.status_code}")
            return

        ai_data = resp.json()
        
        # 2. Kiểm tra trạng thái Camera
        if ai_data.get("status") == "offline":
            # In ra để biết là nó đang chờ, chứ không phải bị lỗi
            # (Chỉ in 5s một lần cho đỡ spam terminal)
            now = time.time()
            if now - last_sent_time.get(cam_id, 0) > 5.0:
                 print(f"[{cam_id}] Đang khởi động/Mất tín hiệu...")
                 last_sent_time[cam_id] = now
            return

        # 3. Lấy dữ liệu cốt lõi
        is_violent = ai_data.get("is_violent", False)
        
        # --- QUAN TRỌNG: LẤY SCORE CHÍNH XÁC ---
        # Ưu tiên lấy 'score' (đã làm mịn), nếu không có thì lấy 'fight_prob' hoặc 'raw_prob'
        score = ai_data.get("score")
        if score is None:
             score = ai_data.get("fight_prob", 0.0)
        
        # Đảm bảo score là float
        score = float(score)
        # ---------------------------------------

        # 4. Kiểm tra tần suất gửi (Smart Frequency)
        now = time.time()
        last_time = last_sent_time.get(cam_id, 0)
        interval = ALERT_INTERVAL if is_violent else HEARTBEAT_INTERVAL
        
        if now - last_time < interval:
            return 

        # 5. ENRICH DATA (Merge CSV + AI)
        enriched_data = cam_metadata.copy()
        enriched_data.update({
            "timestamp": datetime.now().isoformat(),
            "ai_timestamp": ai_data.get("timestamp"),
            "is_violent": is_violent,
            "score": score, # Giá trị thực, không bao giờ là 0 (trừ khi model trả về 0)
            "fps": ai_data.get("fps", 0),
            "latency_ms": ai_data.get("latency_ms", 0),
            "image_preview": ai_data.get("image_preview", ""),
            "evidence_url": ai_data.get("evidence_url", None)
        })

        # 6. Gửi Kafka
        producer.send(KAFKA_TOPIC, value=enriched_data)
        
        last_sent_time[cam_id] = now
        
        # Log ra màn hình để biết đang chạy
        status_icon = "VIOLENCE" if is_violent else "Normal"
        print(f"Sent Kafka [{cam_id}] {status_icon} | Score: {score:.4f} | FPS: {enriched_data['fps']}")

    except Exception as e:
        # IN LỖI RA THAY VÌ PASS
        print(f"Error processing {cam_id}: {e}")

# ================= MAIN LOOP =================

def main():
    registry = load_camera_registry(METADATA_FILE)
    if not registry: return

    start_ai_processing(registry)
    
    print("\n🎧 Producer đang chạy... (Chờ khoảng 10s để Camera khởi động)")
    print(f"Topic: {KAFKA_TOPIC}\n")

    executor = ThreadPoolExecutor(max_workers=8)

    try:
        while True:
            for cam_id, meta in registry.items():
                executor.submit(process_camera, cam_id, meta)
            
            # Giảm tải CPU
            time.sleep(0.2) 
            
    except KeyboardInterrupt:
        print("\nStopping Producer...")
        executor.shutdown(wait=False)
        producer.close()
        print("Done.")

if __name__ == "__main__":
    main()