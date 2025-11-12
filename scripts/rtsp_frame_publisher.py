# scripts/rtsp_frame_publisher.py

import cv2
import time
import json
import os
import csv
import threading
import uuid
from minio import Minio
from kafka import KafkaProducer
from datetime import datetime
from io import BytesIO
from pathlib import Path

# --- Cấu hình ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = "rtsp-frames"
MINIO_REGION = "ap-southeast-1" 

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = "ingest.media.events"

# Tần suất trích xuất frame (vd: 2 frame/giây)
FRAME_RATE = 1

METADATA_FILE = Path("data/metadata/camera_registry.csv")

# --- Hàm đọc Metadata ---
def load_camera_registry():
    """Đọc file CSV camera_registry và trả về dictionary."""
    registry = {}
    try:
        with open(METADATA_FILE, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                registry[row['camera_id']] = row
        return registry
    except FileNotFoundError:
        print(f"LỖI: Không tìm thấy file metadata tại {METADATA_FILE.resolve()}")
        return {}

# --- Khởi tạo Clients & Metadata ---
CAMERA_REGISTRY = load_camera_registry()
if not CAMERA_REGISTRY:
    exit(1)

# Lấy danh sách RTSP streams từ registry
RTSP_STREAMS = {
    cam_id: data['rtsp_url'] 
    for cam_id, data in CAMERA_REGISTRY.items()
}

try:
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False, 
        region=MINIO_REGION 
    )
    if not minio_client.bucket_exists(MINIO_BUCKET):
        minio_client.make_bucket(MINIO_BUCKET)
        print(f"Bucket {MINIO_BUCKET} created successfully.")
    
    kafka_producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("MinIO và Kafka Producer đã khởi tạo thành công.")
except Exception as e:
    print(f"LỖI KHỞI TẠO CLIENT: {e}")
    exit(1)


def process_stream(camera_id, rtsp_url):
    """Xử lý từng luồng RTSP."""
    cam_metadata = CAMERA_REGISTRY.get(camera_id, {})
    if not cam_metadata:
        print(f"[LỖI] Không tìm thấy metadata cho {camera_id}. Bỏ qua.")
        return

    print(f"Đang bắt đầu xử lý luồng: {camera_id} ({cam_metadata.get('street')})")
    
    cap = cv2.VideoCapture(rtsp_url, cv2.CAP_FFMPEG)
    if not cap.isOpened():
        print(f"[LỖI] Không thể mở luồng RTSP: {camera_id}")
        return

    frame_interval = 1.0 / FRAME_RATE
    frame_seq = 0 # Thêm bộ đếm frame sequence

    while True:
        start_time = time.time()
        
        ret, frame = cap.read()
        
        if not ret:
            # Logic kết nối lại
            cap.release()
            time.sleep(5)
            cap = cv2.VideoCapture(rtsp_url, cv2.CAP_FFMPEG)
            if not cap.isOpened():
                break 
            continue
        
        frame_seq += 1 #
        
        # Nén frame sang JPEG
        _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
        frame_bytes = BytesIO(buffer)
        frame_width, frame_height = frame.shape[1], frame.shape[0]

        # Tạo metadata
        now = datetime.utcnow()
        timestamp_str = now.isoformat(timespec='milliseconds') + "Z" # Định dạng chuẩn ISO 8601
        
        # Định dạng path lưu MinIO: rtsp-frames/cam_01/YYYY/MM/DD/HHMMSS_ms.jpg
        minio_object_name = f"{MINIO_BUCKET}/{camera_id}/{now.strftime('%Y/%m/%d')}/{now.strftime('%H%M%S')}{now.microsecond//1000}.jpg"

        try:
            # 1. Upload frame lên MinIO
            minio_client.put_object(
                MINIO_BUCKET,
                minio_object_name,
                frame_bytes,
                length=frame_bytes.getbuffer().nbytes,
                content_type='image/jpeg'
            )

            # 2. Tạo và Publish metadata lên Kafka (CHUẨN HÓA SCHEMA)
            message = {
                "event_id": str(uuid.uuid4()), 
                "camera_id": camera_id,
                "timestamp_utc": timestamp_str,
                "frame_s3_path": minio_object_name.replace(f"{MINIO_BUCKET}/", ""), 
                "frame_width": frame_width,
                "frame_height": frame_height,
                "frame_seq": frame_seq,
                "source_rtsp": rtsp_url,
                "metadata": {
                    "city": cam_metadata.get("city"),
                    "district": cam_metadata.get("district"),
                    "ward": cam_metadata.get("ward"),
                    "street": cam_metadata.get("street"),
                    "latitude": float(cam_metadata.get("latitude", 0)),  
                    "longitude": float(cam_metadata.get("longitude", 0)) 
                }
            }
            
            kafka_producer.send(KAFKA_TOPIC, value=message, key=camera_id.encode('utf-8'))

        except Exception as e:
            print(f"[LỖI XỬ LÝ {camera_id} - Frame {frame_seq}]: {e}")
            
        # Điều chỉnh thời gian chờ để duy trì FRAME_RATE
        elapsed_time = time.time() - start_time
        sleep_time = frame_interval - elapsed_time
        if sleep_time > 0:
            time.sleep(sleep_time)

    cap.release()
    print(f"Luồng {camera_id} đã dừng.")

def main():
    """Khởi chạy tất cả các luồng RTSP."""
    if not RTSP_STREAMS:
        print("Không có luồng RTSP nào được định nghĩa. Vui lòng chạy generate_metadata.py trước.")
        return
    
    threads = []
    for cam_id, rtsp_url in RTSP_STREAMS.items():
        t = threading.Thread(target=process_stream, args=(cam_id, rtsp_url), daemon=True)
        threads.append(t)
        t.start()
        time.sleep(0.1) # Dãn cách khởi động

    for t in threads:
        t.join() 

if __name__ == "__main__":
    main()
