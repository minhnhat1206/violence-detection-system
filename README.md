
##  Hướng Dẫn Thiết Lập và Chạy Dự Án (v2.0 - Có Giám Sát)

Dự án này thiết lập một pipeline xử lý video streaming theo thời gian thực sử dụng Kafka, Spark Structured Streaming và MinIO (S3) cho Data Lakehouse.

### 1\.  Chuẩn bị Cấu trúc Dữ liệu và Mã nguồn

Đảm bảo cấu trúc thư mục dự án của bạn (nơi chứa `docker-compose.yml`) có các thư mục và files sau:

```
realtime-violence-detection/
├── data/
│   ├── metadata/
│   │   └── camera_registry.csv  <-- CHỨA DANH SÁCH CAMERA VÀ PLAYLIST
│   └── processed/
│       └── clips_for_streaming/ <-- CHỨA CÁC FILE VIDEO (.avi, .mp4)
├── docker/
│   ├── grafana/ ...                 <-- Cấu hình Grafana Provisioning
│   ├── prometheus/ ...              <-- Cấu hình Prometheus
│   ├── spark-conf/ ...              <-- Cấu hình Metrics Spark
│   └── docker-compose.yml
└── scripts/
    └── simulate_rtsp_streams.py
    └── rtsp_frame_publisher.py (Producer)
    └── kafka_parquet_sink.py (Consumer/Spark Job)
```

-----

### 2\.  Thiết lập Cổng (Ports) và Truy cập Dịch vụ

Dự án sử dụng các cổng sau trên máy Host của bạn:

| Dịch vụ | Cổng Host | Mục đích |
| :--- | :--- | :--- |
| **MediaMTX (RTSP)** | `8554` | Xem luồng video trực tiếp bằng VLC. |
| **MediaMTX (HTTP)** | `8888` | Xem luồng trên Web Dashboard (HLS/DASH). |
| **Spark UI** | `8080` | Giám sát Spark Master và các ứng dụng. |
| **MinIO** | `9001` | Truy cập Dashboard MinIO (Web). |
| **Prometheus** | `9090` | Truy cập Prometheus UI. |
| **Grafana** | `3001` | **Giám sát Dashboard** (Login mặc định: `admin`/`admin`). |
| **Kafka** | `9092` | (Chỉ nội bộ Docker) |

-----

### 3\.  Build và Khởi động Tất cả Dịch vụ

Chuyển đến thư mục chứa `docker-compose.yml` (ví dụ: `realtime-violence-detection\docker`) và chạy lệnh:

```bash
docker compose up -d --build --force-recreate
```

Lệnh này sẽ:

1.  Tải và build tất cả các images cần thiết (bao gồm cả **Spark** và **Monitoring Stack**).
2.  Khởi tạo tất cả các dịch vụ (Kafka, Spark, MinIO, Prometheus, Grafana).

-----

### 4\.  Khởi tạo Topic Kafka (Chỉ chạy lần đầu)

Nếu bạn đã tách việc tạo topics ra khỏi quá trình khởi động, bạn cần chạy lệnh này để khởi tạo 2 topics cần thiết:

```bash
docker exec kafka /usr/local/bin/create-topics.sh
```

-----

### 5\.  Xử lý và Lưu trữ Dữ liệu (Spark Streaming Job)

Đây là bước chạy ứng dụng Spark Structured Streaming để tiêu thụ kết quả từ Kafka và ghi vào MinIO (Iceberg/Parquet).

#### 5.1. Chạy Spark Streaming Job

Thực thi lệnh sau đây để chạy ứng dụng `kafka_parquet_sink.py` trên Cluster Spark. **Đây là bước bắt buộc để sinh ra các metrics Driver và Streaming cho Grafana.**

```bash
spark-submit \
    --master spark://spark-master:7077 \
    /opt/bitnami/spark/scripts/kafka_parquet_sink.py
```


#### 5.2. Xác nhận Dữ liệu Đã Ghi vào MinIO

Sau khi Job chạy và xử lý dữ liệu (khoảng 1-2 phút), bạn có thể kiểm tra dữ liệu:

1.  **Khởi động Spark Shell:**

    ```bash
    docker exec -it spark-master bash -lc "spark-shell --conf spark.driver.host=spark-master"
    ```

2.  **Đọc và Hiển thị Dữ liệu (trong Spark Shell):**

    ```scala
    val df = spark.read.parquet("s3a://inference-results/data/")
    df.show(5)
    ```

-----

### 6\.  Giám sát và Kiểm tra Luồng Dữ liệu

#### 6.0 Kiểm tra dữ liệu các metric của prometheus

```
curl.exe "http://localhost:9090/api/v1/query?query=spark_driver_streaming_processed_records_total"

curl.exe "http://localhost:9090/api/v1/query?query=spark_driver_streaming_end_to_end_latency_seconds"

```


#### 6.1. Giám sát Hiệu suất (Grafana)

1.  **Truy cập Grafana:** Mở trình duyệt và truy cập `http://localhost:3001`
2.  **Đăng nhập:** `admin`/`admin`.
3.  Dashboard **"Spark Structured Streaming Pipeline Monitoring"** sẽ tự động được tải và hiển thị hiệu suất xử lý (Throughput, Latency) và trạng thái Cluster (Alive Workers, CPU Usage).

#### 6.2. Kiểm tra Luồng Cơ bản

  * **RTSP Stream (MediaMTX):** Mở VLC với địa chỉ `rtsp://localhost:8554/cam_01`.
  * **MinIO (S3 Storage):** Truy cập `http://localhost:9001` và kiểm tra bucket `violence-frames` (frame ảnh) và `inference-results/data` (file Parquet).
  * **Kafka (Dữ liệu Luồng):** Kiểm tra xem Producer có đang gửi message không:
    ```bash
    docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ingest.media.events --from-beginning
    ```

-----

## Dừng Dự án

Để dừng và gỡ bỏ tất cả các services và networks, chạy lệnh:

```bash
docker compose down
```