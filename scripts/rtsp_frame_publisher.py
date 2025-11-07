# scripts/rtsp_frame_publisher.py

import cv2
import time
import json
import os
from minio import Minio
from kafka import KafkaProducer
from datetime import datetime
from io import BytesIO
import threading # Giữ lại import này cho hàm main()

# --- Cấu hình ---
# Đảm bảo các biến môi trường được thiết lập trong Docker Compose
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = "rtsp-frames"
# ĐÃ SỬA: Thêm region cần thiết để MinIO SDK ký request đúng
MINIO_REGION = "ap-southeast-1" 

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = "ingest.media.events"

# Tần suất trích xuất frame (vd: 2 frame/giây)
FRAME_RATE = 2 

# Địa chỉ RTSP (sẽ được thay thế bằng logic đọc từ camera_registry.csv)
# Tạm thời dùng camera giả lập (chạy trên MediaMTX ở mediamtx:8554)
RTSP_STREAMS = {
    "cam_01": "rtsp://mediamtx:8554/cam_02", 
    # Thêm các camera khác nếu cần
}

# --- Khởi tạo Clients ---
try:
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False, # Dùng HTTP nội bộ
        # ĐÃ SỬA LỖI: Truyền tham số region vào constructor
        region=MINIO_REGION 
    )
    # Kiểm tra và tạo bucket nếu chưa có
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
    print(f"Đang bắt đầu xử lý luồng: {camera_id} từ {rtsp_url}")
    
    cap = cv2.VideoCapture(rtsp_url, cv2.CAP_FFMPEG)
    if not cap.isOpened():
        print(f"[LỖI] Không thể mở luồng RTSP: {camera_id}")
        return

    frame_interval = 1.0 / FRAME_RATE

    while True:
        start_time = time.time()
        
        # Đọc frame
        ret, frame = cap.read()
        
        if not ret:
            print(f"[CẢNH BÁO] Không đọc được frame từ {camera_id}. Thử kết nối lại sau 5s...")
            cap.release()
            time.sleep(5)
            cap = cv2.VideoCapture(rtsp_url, cv2.CAP_FFMPEG)
            if not cap.isOpened():
                break # Dừng nếu không thể kết nối lại
            continue

        # Nén frame sang JPEG
        _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
        frame_bytes = BytesIO(buffer)
        
        # Tạo metadata
        now = datetime.utcnow()
        timestamp_str = now.isoformat() + "Z"
        # Định dạng path lưu MinIO: rtsp-frames/cam_01/YYYY/MM/DD/HHMMSS_ms.jpg
        minio_object_name = f"{camera_id}/{now.strftime('%Y/%m/%d')}/{now.strftime('%H%M%S')}_{now.microsecond//1000}.jpg"

        try:
            # 1. Upload frame lên MinIO
            minio_client.put_object(
                MINIO_BUCKET,
                minio_object_name,
                frame_bytes,
                length=frame_bytes.getbuffer().nbytes,
                content_type='image/jpeg'
            )

            # 2. Tạo và Publish metadata lên Kafka
            message = {
                "camera_id": camera_id,
                "timestamp": timestamp_str,
                "frame_path": minio_object_name,
                "minio_endpoint": MINIO_ENDPOINT.split(':')[0], # Chỉ lấy hostname cho Worker
                "minio_bucket": MINIO_BUCKET
            }
            
            kafka_producer.send(KAFKA_TOPIC, value=message)
            # print(f"[{camera_id}] Uploaded & Published: {minio_object_name}")

        except Exception as e:
            # In ra lỗi chi tiết nếu quá trình xử lý (upload/publish) thất bại
            print(f"[LỖI XỬ LÝ {camera_id}]: {e}")
            
        # Điều chỉnh thời gian chờ để duy trì FRAME_RATE
        elapsed_time = time.time() - start_time
        sleep_time = frame_interval - elapsed_time
        if sleep_time > 0:
            time.sleep(sleep_time)

    cap.release()
    print(f"Luồng {camera_id} đã dừng.")

def main():
    """Khởi chạy tất cả các luồng RTSP."""
    import threading
    
    threads = []
    for cam_id, rtsp_url in RTSP_STREAMS.items():
        # Khởi chạy mỗi luồng trong một Thread riêng biệt
        t = threading.Thread(target=process_stream, args=(cam_id, rtsp_url))
        threads.append(t)
        t.start()
        time.sleep(0.5) # Dãn cách khởi động

    for t in threads:
        t.join() # Chờ tất cả các thread kết thúc

if __name__ == "__main__":
    main()

