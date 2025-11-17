from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, date_format, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, ArrayType
)

# Prometheus
from prometheus_client import start_http_server, Counter, Gauge
import threading
import time
import sys
import traceback

# --- CẤU HÌNH ---
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "model.inference.results"
MINIO_SINK_PATH = "s3a://inference-results/data/"
CHECKPOINT_PATH = "s3a://inference-results/checkpoint/"
PROMETHEUS_PORT = 8000

# --- PROMETHEUS METRICS ---
# Counter tổng số bản ghi đã xử lý (tăng dần)
PROCESSED_COUNTER = Counter(
    'spark_driver_streaming_processed_records_total',
    'Total processed records by streaming job'
)

# Gauge end-to-end latency (giá trị trung bình theo batch, đơn vị: giây)
E2E_LATENCY_GAUGE = Gauge(
    'spark_driver_streaming_end_to_end_latency_seconds',
    'End-to-End latency (avg) in seconds for streaming job'
)

def start_metrics_server(port: int = PROMETHEUS_PORT):
    """
    Khởi chạy HTTP server cho prometheus_client (mặc định path /metrics).
    Chạy trong một thread tách rời để không block Spark driver.
    """
    try:
        start_http_server(port)
        print(f"[metrics] Prometheus metrics server started on port {port}")
    except Exception as e:
        print("[metrics] Failed to start Prometheus server:", e)
        traceback.print_exc()

# start server non-blocking
t = threading.Thread(target=start_metrics_server, args=(PROMETHEUS_PORT,), daemon=True)
t.start()

# --- SCHEMA ---
METADATA_SCHEMA = StructType([StructField("notes", StringType(), True)])
BBOX_SCHEMA = ArrayType(ArrayType(DoubleType()))
SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("camera_id", StringType(), False),
    StructField("timestamp_utc", StringType(), False),
    StructField("frame_s3_path", StringType(), True),
    StructField("label", StringType(), True),
    StructField("score", DoubleType(), True),
    StructField("bbox", BBOX_SCHEMA, True),
    StructField("model_name", StringType(), True),
    StructField("model_version", StringType(), True),
    StructField("latency_ms", IntegerType(), True),
    StructField("extra", METADATA_SCHEMA, True),
])

# --- SPARK SESSION (S3A + Hive Metastore) ---
spark = SparkSession.builder \
    .appName("KafkaParquetSink") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "mypassword") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- READ KAFKA STREAM ---
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# --- TRANSFORM ---
df_processed = df_stream \
    .selectExpr("CAST(value AS STRING) as json_payload", "CAST(key AS STRING) as camera_key") \
    .select(from_json(col("json_payload"), SCHEMA).alias("data"), col("camera_key")) \
    .select("data.*") \
    .withColumn("processed_timestamp",
                to_timestamp(col("timestamp_utc"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")) \
    .withColumn("date", date_format(col("processed_timestamp"), "yyyy-MM-dd")) \
    .select(
        col("event_id"),
        col("camera_id"),
        col("processed_timestamp").alias("timestamp_utc"),
        col("frame_s3_path"),
        col("label"),
        col("score"),
        col("bbox"),
        col("model_name"),
        col("model_version"),
        col("latency_ms"),
        col("extra"),
        col("date")
    )

# --- PROCESS BATCH FUNCTION (foreachBatch) ---
def process_batch(batch_df, batch_id):
    """
    batch_df: DataFrame chứa dữ liệu micro-batch hiện tại.
    Cập nhật Prometheus metrics và ghi batch vào MinIO (S3A).
    """
    try:
        if batch_df is None:
            print(f"[batch {batch_id}] Empty batch (None)")
            return

        # 1) Đếm số bản ghi của batch (action)
        #    Nếu throughput quá lớn, cân nhắc dùng phương pháp nhẹ hơn để tính count.
        count = batch_df.count()
        PROCESSED_COUNTER.inc(count)
        print(f"[batch {batch_id}] processed count = {count}")

        # 2) Tính average latency_ms nếu cột latency_ms có mặt
        if 'latency_ms' in batch_df.columns:
            avg_latency_row = batch_df.agg({'latency_ms': 'avg'}).collect()
            if avg_latency_row and len(avg_latency_row) > 0:
                avg_latency_ms = avg_latency_row[0][0]
                if avg_latency_ms is not None:
                    E2E_LATENCY_GAUGE.set(float(avg_latency_ms) / 1000.0)  # chuyển ms -> s
                    print(f"[batch {batch_id}] avg latency (ms) = {avg_latency_ms}, set gauge (s) = {float(avg_latency_ms) / 1000.0}")

        # 3) Ghi batch xuống MinIO dưới dạng Parquet (giữ partition giống trước)
        (batch_df
         .write
         .mode("append")
         .partitionBy("date", "camera_id")
         .option("parquet.compression", "snappy")
         .format("parquet")
         .save(MINIO_SINK_PATH))
        print(f"[batch {batch_id}] saved to {MINIO_SINK_PATH}")

    except Exception as exc:
        print(f"[batch {batch_id}] Exception during process_batch: {exc}", file=sys.stderr)
        traceback.print_exc()

# --- START STREAM (dùng foreachBatch để cập nhật metrics) ---
print(f"Bắt đầu ghi dữ liệu từ Kafka topic '{KAFKA_TOPIC}' vào MinIO path: {MINIO_SINK_PATH}")

query = (df_processed.writeStream
         .outputMode("append")
         .foreachBatch(process_batch)
         .trigger(processingTime="30 seconds")
         .start())

# Đợi kết thúc
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Stopping streaming query (KeyboardInterrupt)...")
    query.stop()
except Exception as e:
    print("Streaming query terminated with exception:", e)
    traceback.print_exc()
finally:
    spark.stop()
