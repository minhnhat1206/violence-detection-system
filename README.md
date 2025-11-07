
## 📝 Hướng Dẫn Thiết Lập và Chạy Dự Án

### 1\. Chuẩn bị Dữ liệu

Đảm bảo cấu trúc thư mục dự án của bạn (nơi chứa `docker-compose.yml`) có các thư mục sau:

```
realtime-violence-detection/
├── data/
│   ├── metadata/
│   │   └── camera_registry.csv  <-- CHỨA DANH SÁCH CAMERA VÀ PLAYLIST
│   └── processed/
│       └── clips_for_streaming/ <-- CHỨA CÁC FILE VIDEO (.avi, .mp4)
├── docker/
│   └── docker-compose.yml
└── scripts/
    └── simulate_rtsp_streams.py
    └── rtsp_frame_publisher.py (Producer)
```


### 2\. Thiết lập Biến môi trường và Cổng

Dự án sử dụng cổng sau trên máy Host của bạn (đã được map trong `docker-compose.yml`):

| Dịch vụ | Cổng Host | Mục đích |
| :--- | :--- | :--- |
| **MediaMTX (RTSP)** | `8554` | Xem luồng trực tiếp bằng VLC. |
| **MediaMTX (HTTP)** | `8888` | Xem luồng trên Web Dashboard (HLS/DASH). |
| **MinIO** | `9001` | Truy cập Dashboard MinIO (Web). |
| **Kafka** | `9092` | (Chỉ nội bộ) |

### 3\. Build và Khởi động Tất cả Dịch vụ

Chuyển đến thư mục chứa `docker-compose.yml` (ví dụ: `realtime-violence-detection\docker`) và chạy lệnh:

```bash
docker compose up -d --build
```

Lệnh này sẽ:

1.  Tải và build tất cả các images cần thiết (`kafka`, `rtsp_pusher`, `producer`).
2.  Khởi tạo các dịch vụ.
3.  **Tự động** khởi động Kafka server và chạy script Python trong `rtsp_pusher` và `producer`.

### 4\. Tạo Kafka Topics (Thủ công)

Nếu bạn đã làm theo hướng dẫn sửa lỗi và **tách việc tạo topics**, bạn cần chạy lệnh này để khởi tạo 2 topics cần thiết:

```bash
docker exec kafka /usr/local/bin/create-topics.sh
```

### 5\. Kiểm tra Luồng Dữ liệu (Dự án đã chạy)

Sau khi tất cả container chạy ổn định:

-----

#### 5.1. Kiểm tra RTSP Stream (MediaMTX)

Kiểm tra xem các luồng video đã được đẩy lên MediaMTX chưa:

  * **Sử dụng VLC:** Mở luồng mạng với địa chỉ:
    `rtsp://localhost:8554/cam_01` (thay `cam_01` bằng ID camera của bạn).

-----

#### 5.2. Kiểm tra MinIO (S3 Storage)

Kiểm tra xem Kafka Producer có lưu khung hình vào MinIO không:

  * **Truy cập Dashboard MinIO:** Mở trình duyệt và truy cập `http://localhost:9001`
  * **Đăng nhập:** Sử dụng thông tin đăng nhập đã cấu hình trong `docker-compose.yml` (ví dụ: `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`).
  * **Kiểm tra Bucket:** Tìm kiếm bucket được cấu hình trong dịch vụ `producer` (ví dụ: `violence-frames`). Sau một thời gian, bạn sẽ thấy các file ảnh (`.jpg`) của từng khung hình được lưu trữ tại đây.

-----

#### 5.3. Kiểm tra Kafka (Dữ liệu Luồng)

Kiểm tra xem Producer có đang gửi message lên Kafka không:

```bash
docker exec kafka /opt/bitnami/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic ingest.media.events \
    --from-beginning \
    --max-messages 5
```

Nếu lệnh này hiển thị các JSON message chứa metadata về frame (frame number, timestamp, MinIO path), điều đó có nghĩa là dữ liệu đã được **nạp vào MinIO và Kafka** thành công.

## Dừng Dự án

Để dừng và gỡ bỏ tất cả các services, chạy lệnh:

```bash
docker compose down
```