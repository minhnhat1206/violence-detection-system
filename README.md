
##  Hướng Dẫn Thiết Lập và Chạy Dự Án 

Dự án này thiết lập một pipeline xử lý video streaming theo thời gian thực sử dụng Kafka, Spark Structured Streaming và MinIO (S3) cho Data Lakehouse.

### 1\.  Chuẩn bị Cấu trúc Dữ liệu và Mã nguồn

Tải bộ dataset RWF2000 và lưu trong folder raw của data
```
realtime-violence-detection/
├── data/
│   ├── raw/
│   │   └── RWF-2000
├── docker/
│   ├── grafana/ ...                 <-- Cấu hình Grafana Provisioning
│   ├── prometheus/ ...              <-- Cấu hình Prometheus
│   ├── spark-conf/ ...              <-- Cấu hình Metrics Spark
│   └── docker-compose.yml
└── scripts/
    └── prepare_cameras_dataset.py (chia các clip được dùng để mô phỏng streaming cũng như metadata của từng cam)
    └── simulate_rtsp_streams.py (giả lập luồng dữ liệu camera, dùng giao thức RTSP và chạy bằng rtsp_pusher để gửi dữ liệu cho Mediamtx)
    └── rtsp_frame_publisher.py (Producer: gửi dữ liệu metadata đến kafka)
    └── kafka_parquet_sink.py (tiêu thụ kết quả từ Kafka và ghi vào MinIO (Iceberg/Parquet))
    └── inference_worker.py (giả lập service Model để lưu kết quả vào model.inference.results)

```
sau đó chạy script `prepare_cameras_dataset.py` để chia các clip được dùng để mô phỏng streaming cũng như metadata của từng cam

Sau khi chạy xong thì folder data sẽ có cấu trúc như sau:
```
realtime-violence-detection/
├── data/
│   ├── metadata/
│   │   └── camera_registry.csv  <-- CHỨA DANH SÁCH CAMERA VÀ PLAYLIST
│   └── processed/
│   │  └── clips_for_streaming/ <-- CHỨA CÁC FILE VIDEO (.avi, .mp4)
│   ├── raw/
│   │   └── RWF-2000
```
-----

### 2\.  Thiết lập Cổng (Ports) và Truy cập Dịch vụ

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


```bash
docker compose up -d --build --force-recreate
```

Lệnh này sẽ:

1.  Tải và build tất cả các images cần thiết (bao gồm cả **Spark** và **Monitoring Stack**).
2.  Khởi tạo tất cả các dịch vụ (Kafka, Spark, MinIO, Prometheus, Grafana).

-----

### 4\.  Khởi tạo Topic Kafka (Chỉ chạy lần đầu)

Tạo topic

```bash
docker exec kafka /usr/local/bin/create-topics.sh
```

Kiểm tra dữ liệu được nạp vào topic

```
docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ingest.media.events --from-beginning
```
Đây là metadata được lưu ở kafka, sau khi thay model thì kiểm tra kết quả trong model.inference.results

```
docker exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic model.inference.results --from-beginning
```

-----
# 5\. Spark Streaming Job → Ghi dữ liệu vào Iceberg (MinIO)

### Chạy ứng dụng:

```
docker exec -it spark-master bash -lc "
spark-submit \
    --master spark://spark-master:7077 \
    /opt/bitnami/spark/scripts/kafka_iceberg_sink.py
"
```

Sau 30–60s dữ liệu sẽ xuất hiện trong:

```
s3a://inference-results/iceberg_warehouse/inference_results/
```

---

# 6\. Kiểm tra dữ liệu bằng Spark

```
docker exec -it spark-master bash -lc "spark-shell --conf spark.driver.host=spark-master"
```

Trong Spark Shell:

```scala
val df = spark.read.format("iceberg").load("iceberg.default.inference_results")
df.show(5)
```

---

# 7\. Giám sát bằng Prometheus + Grafana

### Kiểm tra metric Prometheus:

```
curl "http://localhost:9090/api/v1/query?query=spark_driver_streaming_processed_records_total"
curl "http://localhost:9090/api/v1/query?query=spark_driver_streaming_end_to_end_latency_seconds"
```

### Grafana Dashboard:

* Truy cập: [http://localhost:3001](http://localhost:3001)
* Login: `admin` / `admin`

Dashboard hiển thị:

* Throughput (records per batch)
* Avg latency
* Worker health
* JVM metrics

---

# 8\. **TRUY VẤN DỮ LIỆU ICEBERG BẰNG TRINO** 

Sau khi Spark đã ghi dữ liệu vào MinIO dưới dạng Iceberg table, bạn dùng Trino để truy vấn.

---

## 8.1. Truy cập Trino CLI

**Không dùng bash để chạy SQL**, hãy dùng CLI:

```
docker exec -it trino-coordinator trino
```

Bạn sẽ thấy prompt:

```
trino>
```

---

## 8.2. Kiểm tra catalog

```
SHOW CATALOGS;
```

Bạn sẽ thấy:

```
iceberg
system
tpch
```

---

## 8.3. Kiểm tra namespace Iceberg

```
SHOW SCHEMAS FROM iceberg;
```

Mặc định:

```
default
```

---

## 8.4. Xem danh sách bảng

```
SHOW TABLES FROM iceberg.default;
```

Nếu đúng, bạn sẽ thấy:

```
inference_results
```

---

## 8.5. Truy vấn dữ liệu Iceberg

### Xem 10 dòng mới nhất:

```
SELECT *
FROM iceberg.default.inference_results
ORDER BY timestamp_utc DESC
LIMIT 10;
```

### Lấy số lượng record:

```
SELECT count(*) FROM iceberg.default.inference_results;
```

### Thống kê label:

```
SELECT label, count(*) 
FROM iceberg.default.inference_results
GROUP BY label;
```

### Thống kê latency:

```
SELECT avg(latency_ms) AS avg_latency_ms,
       max(latency_ms) AS max_latency_ms
FROM iceberg.default.inference_results;
```

---

# 9\. Kiểm tra luồng trực tiếp

* VLC RTSP:

```
rtsp://localhost:8554/cam_01
```

* MinIO Dashboard:

```
http://localhost:9001
```

Bucket:

* `violence-frames` → ảnh frame
* `inference-results` → Iceberg warehouse

---

# 10\. Dừng toàn bộ hệ thống

```
docker compose down
```
<<<<<<< HEAD

---
=======
>>>>>>> b122156f9c7ba461a1c418b467605e6f59eb7f97
