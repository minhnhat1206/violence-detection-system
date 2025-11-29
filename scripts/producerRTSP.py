import time
import json
import csv
import requests
import threading
from kafka import KafkaProducer
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# ================= CẤU HÌNH =================
API_URL = "http://103.78.3.29:8000"
KAFKA_BROKER = "localhost:9092" # IP Kafka Broker của bạn
KAFKA_TOPIC = "urban-safety-alerts"
METADATA_FILE = "../data/metadata/camera_registry.csv" # Đường dẫn tới file CSV bạn tạo

# Cấu hình chiến lược gửi tin
HEARTBEAT_INTERVAL = 5.0  # Giây (Gửi tin định kỳ khi bình thường)
ALERT_INTERVAL = 0.5      # Giây (Gửi tin dồn dập khi có bạo lực)

# ================= KAFKA PRODUCER SETUP =================
def json_serializer(data):
    return json.dumps(data).encode("utf-8")

try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=json_serializer,
        # Tối ưu hóa việc gửi
        linger_ms=10, 
        acks=1
    )
    print(f"Kết nối Kafka thành công tới {KAFKA_BROKER}")
except Exception as e:
    print(f"Lỗi kết nối Kafka: {e}")
    exit(1)

# ================= HÀM XỬ LÝ =================

def load_camera_registry(csv_path):
    """Đọc CSV và lưu vào Dictionary để tra cứu nhanh theo camera_id"""
    registry = {}
    try:
        with open(csv_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Chuyển đổi kiểu dữ liệu cho đúng chuẩn JSON
                row['latitude'] = float(row['latitude'])
                row['longitude'] = float(row['longitude'])
                registry[row['camera_id']] = row
        print(f"Đã load thông tin {len(registry)} camera từ CSV.")
        return registry
    except Exception as e:
        print(f"Lỗi đọc CSV: {e}")
        return {}

def start_ai_processing(camera_list):
    """Gọi API để kích hoạt AI Worker cho từng Camera"""
    print("Đang kích hoạt AI cho các Camera...")
    for cam_id, info in camera_list.items():
        rtsp_url = info['rtsp_url']
        try:
            # Gọi API /camera/start
            # Lưu ý: rtsp_url trong CSV của bạn là "rtsp://.../cam01"
            # Nhưng API cần param riêng lẻ, ta gửi nguyên chuỗi vào
            response = requests.post(
                f"{API_URL}/camera/start",
                params={"camera_id": cam_id, "rtsp_url": rtsp_url},
                timeout=2
            )
            if response.status_code == 200:
                print(f"   + {cam_id}: Started OK")
            elif "already running" in response.text:
                print(f"   = {cam_id}: Already running")
            else:
                print(f"   - {cam_id}: Failed ({response.text})")
        except Exception as e:
            print(f"   ! {cam_id}: Error calling API ({e})")

# Biến lưu thời gian gửi cuối cùng của mỗi cam
last_sent_time = {}

def process_camera(cam_id, cam_metadata):
    """Hàm worker: Lấy data AI -> Merge -> Gửi Kafka"""
    global last_sent_time
    
    try:
        # 1. Gọi API lấy trạng thái Realtime
        resp = requests.get(f"{API_URL}/camera/status/{cam_id}", timeout=1)
        if resp.status_code != 200: return

        ai_data = resp.json()
        
        # Bỏ qua nếu camera offline
        if ai_data.get("status") == "offline": return

        # 2. Kiểm tra chiến lược gửi tin (Smart Frequency)
        is_violent = ai_data.get("is_violent", False)
        now = time.time()
        last_time = last_sent_time.get(cam_id, 0)
        
        # Logic: 
        # - Nếu có bạo lực: Gửi liên tục (0.5s/lần)
        # - Nếu bình thường: Chỉ gửi Heartbeat (5s/lần)
        interval = ALERT_INTERVAL if is_violent else HEARTBEAT_INTERVAL
        
        if now - last_time < interval:
            return # Chưa đến lúc gửi, bỏ qua để giảm tải Kafka

        # 3. ENRICH DATA (Kết hợp AI + CSV)
        # Copy metadata gốc để không bị dính dữ liệu cũ
        enriched_data = cam_metadata.copy() 
        
        # Merge dữ liệu từ AI vào
        enriched_data.update({
            "timestamp": datetime.now().isoformat(), # Thời gian hiện tại của hệ thống gửi
            "ai_timestamp": ai_data.get("timestamp"), # Thời gian frame của AI
            "is_violent": is_violent,
            "score": ai_data.get("score", 0),
            "fps": ai_data.get("fps", 0),
            "latency_ms": ai_data.get("latency_ms", 0),
            "image_preview": ai_data.get("image_preview", ""), # Ảnh nhỏ Base64
            "evidence_url": ai_data.get("evidence_url", None)  # Link ảnh Full HD (nếu có)
        })

        # 4. Gửi vào Kafka
        future = producer.send(KAFKA_TOPIC, value=enriched_data)
        # future.get(timeout=10) # Bỏ comment nếu muốn đảm bảo gửi thành công (chậm hơn)
        
        # Cập nhật thời gian gửi
        last_sent_time[cam_id] = now
        
        status_icon = "Violence detected" if is_violent else "Non Violence Detected"
        print(f"Sent {status_icon} [{cam_id}] Score: {enriched_data['score']} | FPS: {enriched_data['fps']}")

    except Exception as e:
        # print(f"Error processing {cam_id}: {e}")
        pass

# ================= MAIN LOOP =================

def main():
    # 1. Load Metadata
    registry = load_camera_registry(METADATA_FILE)
    if not registry: return

    # 2. Khởi động AI trên Server (Chỉ chạy 1 lần lúc đầu)
    start_ai_processing(registry)
    
    print("\nĐang lắng nghe và đẩy dữ liệu vào Kafka...")
    print(f"Topic: {KAFKA_TOPIC}")
    print("Nhấn Ctrl+C để dừng.\n")

    # Sử dụng ThreadPool để xử lý song song nhiều camera cùng lúc
    # Giúp không bị lag khi số lượng camera tăng lên 16, 32...
    executor = ThreadPoolExecutor(max_workers=8)

    try:
        while True:
            # Tạo task cho từng camera
            futures = []
            for cam_id, meta in registry.items():
                futures.append(executor.submit(process_camera, cam_id, meta))
            
            # Đợi một chút để không spam CPU (Polling loop)
            # Tần suất poll tổng thể. Các cam sẽ tự filter bằng logic Smart Frequency ở trên
            time.sleep(0.2) 
            
    except KeyboardInterrupt:
        print("\nStopping Producer...")
        executor.shutdown(wait=False)
        producer.close()
        print("Done.")

if __name__ == "__main__":
    main()