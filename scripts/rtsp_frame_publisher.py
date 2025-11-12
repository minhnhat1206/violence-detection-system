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
import math

# --- Cấu hình ---
# Cập nhật giá trị mặc định để đồng bộ với cấu hình Docker Compose mới
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio") 
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "mypassword")
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
        # Đường dẫn file metadata cần được điều chỉnh cho môi trường container
        # Nếu producer chạy trong container, đường dẫn cần phải tuyệt đối hoặc tương đối với thư mục làm việc.
        # Giả sử thư mục gốc là /app và data/metadata được mount vào đó.
        # METADATA_FILE = Path("/app/data/metadata/camera_registry.csv") 
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
    print("Vui lòng đảm bảo file camera_registry.csv tồn tại và không rỗng.")
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
    # MinIO client đã được thêm vào minio_client service để tự động tạo bucket.
    # Tuy nhiên, kiểm tra và tạo thủ công ở đây là một lớp phòng vệ tốt.
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

    # Lấy tốc độ khung hình gốc của luồng RTSP
    fps_original = cap.get(cv2.CAP_PROP_FPS)
    if fps_original <= 0:
        # Nếu không lấy được FPS, đặt tốc độ đọc mặc định cao (ví dụ: 30)
        fps_original = 30
    
    # Tính toán số frame cần bỏ qua
    frame_skip_count = max(1, math.floor(fps_original / FRAME_RATE))
    
    frame_seq = 0 
    current_frame_count = 0

    while True:
        start_time = time.time()
        
        # Đọc frame
        ret, frame = cap.read()
        
        if not ret:
            print(f"[CẢNH BÁO {camera_id}] Mất kết nối, thử kết nối lại sau 5 giây...")
            cap.release()
            time.sleep(5)
            cap = cv2.VideoCapture(rtsp_url, cv2.CAP_FFMPEG)
            if not cap.isOpened():
                print(f"[LỖI {camera_id}] Không thể kết nối lại.")
                break 
            continue
        
        current_frame_count += 1
        
        # Logic BỎ QUA FRAME để duy trì FRAME_RATE
        if current_frame_count % frame_skip_count != 0:
            continue
            
        frame_seq += 1 
        
        # Nén frame sang JPEG
        _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 80])
        frame_bytes = BytesIO(buffer)
        frame_width, frame_height = frame.shape[1], frame.shape[0]

        # Tạo metadata
        now = datetime.utcnow()
        timestamp_str = now.isoformat(timespec='milliseconds') + "Z" # Định dạng chuẩn ISO 8601
        
        # Định dạng path lưu MinIO: cam_01/YYYY/MM/DD/HHMMSS_ms.jpg
        # CHỈnh sửa logic: Xóa MINIO_BUCKET ở đầu chuỗi path để tránh path bị lặp lại
        # khi sử dụng put_object(bucket_name, object_name, ...)
        minio_object_path = f"{camera_id}/{now.strftime('%Y/%m/%d')}/{now.strftime('%H%M%S')}{now.microsecond//1000}.jpg"

        try:
            # 1. Upload frame lên MinIO
            minio_client.put_object(
                MINIO_BUCKET,
                minio_object_path, # Sửa: chỉ là path trong bucket
                frame_bytes,
                length=frame_bytes.getbuffer().nbytes,
                content_type='image/jpeg'
            )

            # 2. Tạo và Publish metadata lên Kafka
            message = {
                "event_id": str(uuid.uuid4()), 
                "camera_id": camera_id,
                "timestamp_utc": timestamp_str,
                # Sửa: frame_s3_path là object_path đầy đủ trong bucket
                "frame_s3_path": minio_object_path, 
                "frame_width": frame_width,
                "frame_height": frame_height,
                "frame_seq": frame_seq,
                "source_rtsp": rtsp_url,
                "metadata": {
                    "city": cam_metadata.get("city"),
                    "district": cam_metadata.get("district"),
                    "ward": cam_metadata.get("ward"),
                    "street": cam_metadata.get("street"),
                    # Chuyển đổi sang float trước khi gửi (đảm bảo schema)
                    "latitude": float(cam_metadata.get("latitude", 0.0)), 
                    "longitude": float(cam_metadata.get("longitude", 0.0)) 
                }
            }
            
            kafka_producer.send(KAFKA_TOPIC, value=message, key=camera_id.encode('utf-8'))

            # Chỉ in ra log khi thành công
            elapsed_time = time.time() - start_time
            print(f"[INFO {camera_id} - Frame {frame_seq}] Đã xử lý (time: {elapsed_time:.3f}s)")

        except Exception as e:
            print(f"[LỖI XỬ LÝ {camera_id} - Frame {frame_seq}]: {e}")
            
        # KHÔNG cần logic sleep/thời gian chờ vì việc bỏ qua frame đã điều chỉnh tốc độ
        # Logic này chỉ cần để đảm bảo không bị quá tải CPU/RAM, nhưng không cần thiết
        # nếu tốc độ được kiểm soát bằng frame_skip_count. 
        # Chúng ta đã bỏ qua logic sleep cũ để tối ưu hóa hiệu suất đọc/ghi I/O.


    cap.release()
    print(f"Luồng {camera_id} đã dừng.")

def main():
    """Khởi chạy tất cả các luồng RTSP."""
    # Flush Kafka producer buffer khi chương trình kết thúc
    import atexit
    atexit.register(lambda: kafka_producer.flush() if 'kafka_producer' in globals() else None)
    
    if not RTSP_STREAMS:
        print("Không có luồng RTSP nào được định nghĩa. Vui lòng kiểm tra camera_registry.csv.")
        return
    
    threads = []
    for cam_id, rtsp_url in RTSP_STREAMS.items():
        t = threading.Thread(target=process_stream, args=(cam_id, rtsp_url), daemon=True)
        threads.append(t)
        t.start()
        time.sleep(0.1) # Dãn cách khởi động để tránh xung đột I/O ban đầu

    # Đợi tất cả các luồng kết thúc (thường là không bao giờ kết thúc trong streaming)
    # Tuy nhiên, nếu một luồng bị lỗi và dừng, thread.join() sẽ giữ cho luồng main hoạt động.
    for t in threads:
        t.join() 

if __name__ == "__main__":
    main()