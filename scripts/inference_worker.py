import json
import time
import uuid
import random
import os
import csv
from pathlib import Path
from kafka import KafkaConsumer, KafkaProducer
# from minio import Minio # Tạm thời không dùng MinIO để đơn giản hóa skeleton
from datetime import datetime
import threading

# --- CẤU HÌNH KAFKA ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", 'kafka:9092')
INGEST_TOPIC = 'ingest.media.events'
RESULT_TOPIC = 'model.inference.results'

# Đường dẫn metadata file (giả định chạy từ thư mục gốc /app)
METADATA_FILE = Path("/app/data/metadata/camera_registry.csv")

# --- KHỞI TẠO CLIENTS & METADATA ---

def load_camera_registry():
    """Đọc file CSV camera_registry và trả về dictionary."""
    registry = {}
    try:
        # Đường dẫn đã được mount trong Docker Compose
        with open(METADATA_FILE, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                registry[row['camera_id']] = row
        return registry
    except FileNotFoundError:
        print(f"[LỖI] Không tìm thấy file metadata tại {METADATA_FILE.resolve()}")
        # KHÔNG THOÁT: Tiếp tục với registry rỗng để worker vẫn chạy (nhưng không có metadata)
        return {} 

CAMERA_REGISTRY = load_camera_registry()

try:
    # Initialize Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Kafka Producer đã khởi tạo thành công.")
except Exception as e:
    print(f"[LỖI KHỞI TẠO CLIENTS]: {e}")
    exit(1)

# --- CHỨC NĂNG INFERENCE SKELETON ---

# LƯU Ý: Đây là Skeleton. Khi có model, bạn sẽ thay thế logic này.
def run_model(frame_s3_path: str):
    """
    Hàm mô phỏng việc chạy model AI.
    - Trong thực tế, bạn sẽ dùng MinIO client để tải ảnh về:
      frame = minio_client.get_object(bucket, frame_s3_path)
      # Xử lý ảnh...
    """
    
    # Mô phỏng độ trễ xử lý (giá trị thực tế cho AI)
    latency = random.randint(50, 200) 
    time.sleep(latency / 1000)

    # Mô phỏng kết quả AI
    score = random.uniform(0.1, 0.99)
    # 20% xác suất có bạo lực (violence)
    label = "violence" if score > 0.8 else "normal" 
    
    # Bounding Box (chỉ có khi phát hiện bạo lực)
    bbox = [[100, 100, 300, 300]] if label == "violence" else []

    return {
        "label": label,
        "score": round(score, 4),
        "bbox": bbox,
        "model_name": "fight-detector-v1",
        "model_version": "2025-11-10-rc1",
        "latency_ms": latency
    }

def process_ingest_event(message_value: dict):
    """
    Xử lý sự kiện từ ingest.media.events, chạy model,
    và publish kết quả lên model.inference.results.
    """
    
    start_time = time.time()
    camera_id = message_value.get("camera_id")
    
    # 1. Chạy mô hình (hoặc skeleton)
    inference_result = run_model(message_value.get("frame_s3_path"))

    # 2. Tạo message kết quả theo schema model.inference.results
    result_message = {
        # Dữ liệu copy từ ingest event
        "event_id": message_value.get("event_id"), 
        "camera_id": camera_id,
        "timestamp_utc": message_value.get("timestamp_utc"),
        "frame_s3_path": message_value.get("frame_s3_path"),
        
        # Kết quả từ inference_result
        "label": inference_result["label"],
        "score": inference_result["score"],
        "bbox": inference_result["bbox"],
        "model_name": inference_result["model_name"],
        "model_version": inference_result["model_version"],
        "latency_ms": inference_result["latency_ms"],
        
        "extra": {"notes": f"Processed by {threading.current_thread().name}"}
    }
    
    # 3. Publish kết quả lên topic model.inference.results
    # Sử dụng camera_id làm key để đảm bảo thứ tự xử lý (tùy chọn)
    producer.send(
        RESULT_TOPIC, 
        value=result_message,
        key=camera_id.encode('utf-8') if camera_id else None
    )
    # producer.flush() # Có thể bỏ qua flush để tăng throughput, nhưng cần cho debugging

    end_time = time.time()
    print(f"[SUCCESS] Worker {threading.current_thread().name} processed {camera_id} -> {result_message['label']}. Total time: {(end_time - start_time):.3f}s")


if __name__ == '__main__':
    # Initialize Kafka Consumer
    consumer = KafkaConsumer(
        INGEST_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='latest', # Bắt đầu từ message mới nhất
        enable_auto_commit=True,
        group_id='inference-workers-group-v1',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print(f"Inference Worker started, listening to {INGEST_TOPIC} on {KAFKA_BROKER}...")
    
    # Xử lý từng message
    for message in consumer:
        try:
            process_ingest_event(message.value)
        except Exception as e:
            # Xử lý lỗi nếu message bị hỏng hoặc logic xử lý thất bại
            print(f"[FATAL ERROR] Failed to process message at offset {message.offset}: {e}")