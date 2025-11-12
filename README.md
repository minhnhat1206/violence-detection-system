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
    └── kafka_parquet_sink.py (Consumer/Spark Job)
```

-----

### 2\. Thiết lập Biến môi trường và Cổng

Dự án sử dụng cổng sau trên máy Host của bạn (đã được map trong `docker-compose.yml`):

| Dịch vụ | Cổng Host | Mục đích |
| :--- | :--- | :--- |
| **MediaMTX (RTSP)** | `8554` | Xem luồng trực tiếp bằng VLC. |
| **MediaMTX (HTTP)** | `8888` | Xem luồng trên Web Dashboard (HLS/DASH). |
| **Spark UI** | `8080` | Giám sát Spark Master. |
| **MinIO** | `9001` | Truy cập Dashboard MinIO (Web). |
| **Kafka** | `9092` | (Chỉ nội bộ) |

-----

### 3\. Build và Khởi động Tất cả Dịch vụ

Chuyển đến thư mục chứa `docker-compose.yml` (ví dụ: `realtime-violence-detection\docker`) và chạy lệnh:

```bash
docker compose up -d --build
```

Lệnh này sẽ:

1.  Tải và build tất cả các images cần thiết (bao gồm cả **Spark** đã được cấu hình Kafka/S3A JARs).
2.  Khởi tạo các dịch vụ.
3.  **Tự động** khởi động Kafka server và chạy script Python trong `rtsp_pusher` và `producer`.

-----

### 4\. Tạo Kafka Topics (Thủ công)

Nếu bạn đã làm theo hướng dẫn sửa lỗi và **tách việc tạo topics**, bạn cần chạy lệnh này để khởi tạo 2 topics cần thiết:

```bash
docker exec kafka /usr/local/bin/create-topics.sh
```

-----

### 5\. Kiểm tra Luồng Dữ liệu (Dự án đã chạy)

Sau khi tất cả container chạy ổn định:

#### 5.1. Kiểm tra RTSP Stream (MediaMTX)

Kiểm tra xem các luồng video đã được đẩy lên MediaMTX chưa:

  * **Sử dụng VLC:** Mở luồng mạng với địa chỉ:
    `rtsp://localhost:8554/cam_01` (thay `cam_01` bằng ID camera của bạn).

#### 5.2. Kiểm tra MinIO (S3 Storage)

  * **Truy cập Dashboard MinIO:** Mở trình duyệt và truy cập `http://localhost:9001`
  * **Đăng nhập:** Sử dụng thông tin đăng nhập đã cấu hình trong `docker-compose.yml`.
  * **Kiểm tra Bucket:** Kiểm tra bucket `violence-frames` để xem các file ảnh (`.jpg`) của từng khung hình.

#### 5.3. Kiểm tra Kafka (Dữ liệu Luồng)

Kiểm tra xem Producer có đang gửi message lên Kafka không:

```bash
docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ingest.media.events --from-beginning
```

-----

### 6\. 💾 Xử lý và Lưu trữ Dữ liệu (Spark)

Đây là bước chạy ứng dụng Spark Structured Streaming để tiêu thụ kết quả từ Kafka và ghi thành Parquet vào MinIO.

#### 6.1. Chạy Spark Streaming Job

Thực thi lệnh sau đây trong terminal trên máy host để chạy ứng dụng `kafka_parquet_sink.py` trên cluster Spark:

```bash
docker exec -it spark-master bash -lc "/opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.pyspark.python=/opt/bitnami/python/bin/python3 \
    --conf spark.pyspark.driver.python=/opt/bitnami/python/bin/python3 \
    --conf spark.driver.host=spark-master \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
    --conf spark.hadoop.fs.s3a.access.key=minio \
    --conf spark.hadoop.fs.s3a.secret.key=mypassword \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    /opt/bitnami/spark/scripts/kafka_parquet_sink.py"
```
hoặc
```
docker exec -it spark-master bash -lc "/opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.pyspark.python=/opt/bitnami/python/bin/python3 --conf spark.pyspark.driver.python=/opt/bitnami/python/bin/python3 --conf spark.driver.host=spark-master --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider --conf spark.hadoop.fs.s3a.access.key=minio --conf spark.hadoop.fs.s3a.secret.key=mypassword --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 --conf spark.hadoop.fs.s3a.path.style.access=true --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false /opt/bitnami/spark/scripts/kafka_parquet_sink.py"

```

#### 6.2. Xác nhận Dữ liệu Đã Ghi vào MinIO

Sau khi job chạy được một lúc, dữ liệu sẽ được ghi vào MinIO. Sử dụng Spark Shell để kiểm tra:

1.  **Khởi động Spark Shell:**

    ```bash
    docker exec -it spark-master bash -lc "/opt/bitnami/spark/bin/spark-shell"
    ```

2.  **Đọc và Hiển thị Dữ liệu (trong Spark Shell):**

    ```scala
    val df = spark.read.parquet("s3a://inference-results/data/")
    df.show(5)
    ```

    *Nếu dữ liệu hiển thị thành công, luồng xử lý của bạn đã hoạt động hoàn chỉnh.*

-----

## Dừng Dự án

Để dừng và gỡ bỏ tất cả các services, chạy lệnh:

```bash
docker compose down
```
