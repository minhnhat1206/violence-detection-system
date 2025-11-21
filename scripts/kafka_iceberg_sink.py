# kafka_iceberg_sink.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, date_format, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, ArrayType
)

# Prometheus
from prometheus_client import start_http_server, Counter, Gauge
import threading
import sys
import traceback

# --- CONFIG ---
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "model.inference.results"

# Note: use catalog.namespace.table format (catalog = iceberg, namespace = default)
ICEBERG_TABLE = "iceberg.default.inference_results"
ICEBERG_NAMESPACE = "iceberg.default"   # catalog.namespace
CHECKPOINT_PATH = "s3a://inference-results/checkpoint/"
PROMETHEUS_PORT = 8000

# --- PROMETHEUS METRICS ---
PROCESSED_COUNTER = Counter(
    'spark_driver_streaming_processed_records_total',
    'Total processed records by streaming job'
)

E2E_LATENCY_GAUGE = Gauge(
    'spark_driver_streaming_end_to_end_latency_seconds',
    'End-to-End latency (avg) in seconds for streaming job'
)

def start_metrics_server(port=PROMETHEUS_PORT):
    try:
        start_http_server(port)
        print(f"[metrics] Prometheus metrics server started on port {port}")
    except Exception as e:
        print("[metrics] Failed to start Prometheus server:", e)
        traceback.print_exc()

threading.Thread(target=start_metrics_server, daemon=True).start()

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

# --- SPARK SESSION (Iceberg + MinIO + Hive Metastore) ---
spark = SparkSession.builder \
    .appName("KafkaIcebergSink") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.0") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hive") \
    .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083") \
    .config("spark.sql.catalog.iceberg.warehouse", "s3a://inference-results/iceberg_warehouse/") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "mypassword") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- TỰ ĐỘNG ĐẢM BẢO NAMESPACE/TABLE ICEBERG TỒN TẠI ---
# NOTE: 'iceberg' là catalog; namespace phải có thêm phần (ví dụ 'default' hoặc 'dbname').
try:
    # create namespace under catalog (eg. iceberg.default)
    # This will create namespace 'default' in catalog 'iceberg'
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_NAMESPACE}")
    print(f"[iceberg] Ensured namespace {ICEBERG_NAMESPACE} exists.")
except Exception as e:
    # Log warning but continue; maybe namespace already exists or creation not allowed
    print("[iceberg] Warning: failed to ensure/create namespace:", e)
    traceback.print_exc()

try:
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {ICEBERG_TABLE} (
      event_id string,
      camera_id string,
      timestamp_utc timestamp,
      frame_s3_path string,
      label string,
      score double,
      bbox array<array<double>>,
      model_name string,
      model_version string,
      latency_ms int,
      extra struct<notes:string>,
      date string
    )
    USING iceberg
    LOCATION 's3a://inference-results/iceberg_warehouse/inference_results'
    """
    spark.sql(create_table_sql)
    print(f"[iceberg] Ensured table {ICEBERG_TABLE} exists (created if missing).")
except Exception as e:
    print("[iceberg] Warning: failed to ensure/create iceberg table:", e)
    traceback.print_exc()

# --- READ KAFKA STREAM ---
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# --- TRANSFORM ---
df_processed = df_stream \
    .selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), SCHEMA).alias("data")) \
    .select("data.*") \
    .withColumn("processed_timestamp",
                to_timestamp(col("timestamp_utc"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")) \
    .withColumn("date", date_format(col("processed_timestamp"), "yyyy-MM-dd")) \
    .select(
        "event_id", "camera_id", col("processed_timestamp").alias("timestamp_utc"),
        "frame_s3_path", "label", "score", "bbox",
        "model_name", "model_version", "latency_ms", "extra", "date"
    )

# --- FOREACHBATCH ---
def process_batch(batch_df, batch_id):
    try:
        if batch_df is None:
            print(f"[batch {batch_id}] Empty batch (None)")
            return

        count = batch_df.count()
        PROCESSED_COUNTER.inc(count)
        print(f"[batch {batch_id}] processed count = {count}")

        # calc avg latency safely
        avg_latency_row = batch_df.agg({'latency_ms': 'avg'}).collect()
        if avg_latency_row and len(avg_latency_row) > 0:
            avg_latency = avg_latency_row[0][0]
            if avg_latency is not None:
                E2E_LATENCY_GAUGE.set(float(avg_latency) / 1000.0)
                print(f"[batch {batch_id}] avg latency {avg_latency}ms")

        # Repartition để kiểm soát số file (tùy nhu cầu)
        out_df = batch_df.repartition(4)

        # Ghi vào Iceberg table đã đảm bảo tồn tại ở trên
        try:
            out_df.writeTo(ICEBERG_TABLE).append()
            print(f"[batch {batch_id}] appended to Iceberg table {ICEBERG_TABLE}")
        except Exception as write_exc:
            print(f"[batch {batch_id}] ERROR writing to Iceberg: {write_exc}")
            traceback.print_exc()

    except Exception as exc:
        print(f"[batch {batch_id}] ERROR in process_batch: {exc}", file=sys.stderr)
        traceback.print_exc()

# --- START STREAM ---
print(f"Streaming Kafka → Iceberg table: {ICEBERG_TABLE}")

query = (df_processed.writeStream
         .outputMode("append")
         .foreachBatch(process_batch)
         .option("checkpointLocation", CHECKPOINT_PATH)
         .trigger(processingTime="30 seconds")
         .start())

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
