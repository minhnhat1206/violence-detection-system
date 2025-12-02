import sys
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, current_timestamp, date_format
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    BooleanType, IntegerType, FloatType
)

# ================= CẤU HÌNH (CONFIG) =================
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "urban-safety-alerts"

# Lakehouse Config
CATALOG_NAME = "iceberg"
NAMESPACE = "default"
TABLE_NAME = "bronzeViolence"  # Tên bảng Bronze
FULL_TABLE_NAME = f"{CATALOG_NAME}.{NAMESPACE}.{TABLE_NAME}"

# Đường dẫn (MinIO/S3)
WAREHOUSE_PATH = "s3a://warehouse/"
CHECKPOINT_PATH = "s3a://checkpoint/bronzeViolence/"

# ================= SCHEMA DEFINITION =================
# Dựa trên code Producer: enriched_data
SCHEMA = StructType([
    # -- Fields từ CSV (Camera Registry) --
    StructField("camera_id", StringType(), True),
    StructField("city", StringType(), True),   
    StructField("district", StringType(), True),     
    StructField("ward", StringType(), True),     
    StructField("street", StringType(), True),     
    StructField("latitude", DoubleType(), True),   # Producer đã ép kiểu float
    StructField("longitude", DoubleType(), True),  # Producer đã ép kiểu float
    StructField("rtsp_url", StringType(), True),     
    StructField("risk_level", StringType(), True),     

    # -- Fields từ Code Logic & API --
    StructField("timestamp", StringType(), True),       # ISO format string từ datetime.now()
    StructField("ai_timestamp", StringType(), True),    # Timestamp gốc từ AI
    StructField("is_violent", BooleanType(), True),     # Quan trọng
    StructField("score", DoubleType(), True),           # Quan trọng
    StructField("fps", DoubleType(), True),
    StructField("latency_ms", DoubleType(), True),      # Có thể là int hoặc float
    StructField("evidence_url", StringType(), True)     # URL video/ảnh
])

# ================= SPARK SESSION SETUP =================
spark = SparkSession.builder \
    .appName("BronzeLayer_Ingestion") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.0") \
    .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "hive") \
    .config(f"spark.sql.catalog.{CATALOG_NAME}.uri", "thrift://hive-metastore:9083") \
    .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", WAREHOUSE_PATH) \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "mypassword") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ================= KHỞI TẠO BẢNG ICEBERG (DDL) =================
# Bronze Layer thường Partition theo ngày nhận dữ liệu (Processing Time) hoặc Event Time
def ensure_iceberg_table():
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.{NAMESPACE}")
        
        # Tạo bảng nếu chưa tồn tại
        # Partitioned by days(event_time) để tối ưu truy vấn theo thời gian
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {FULL_TABLE_NAME} (
            camera_id string,
            city string,
            district string,
            ward string,
            street string,
            latitude double,
            longitude double,
            rtsp_url string,
            risk_level string,
            event_time timestamp, 
            ai_timestamp string,
            is_violent boolean,
            score double,
            fps double,
            latency_ms double,
            evidence_url string,
            ingest_time timestamp
        )
        USING iceberg
        PARTITIONED BY (days(event_time))
        """
        spark.sql(create_sql)
        print(f"[Iceberg] Table {FULL_TABLE_NAME} is ready.")
    except Exception as e:
        print(f"[Iceberg] Init failed: {e}")
        traceback.print_exc()

ensure_iceberg_table()

# ================= READ STREAM (KAFKA) =================
print(f"Reading stream from {KAFKA_BROKER} topic: {KAFKA_TOPIC}")

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# ================= TRANSFORM (BRONZE LOGIC) =================
# Parse JSON -> Cast Timestamp -> Add Ingestion Time
df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), SCHEMA).alias("data")) \
    .select("data.*")

# Chuẩn hóa dữ liệu cho khớp với bảng Iceberg
df_transformed = df_parsed \
    .withColumn("event_time", to_timestamp(col("timestamp"))) \
    .withColumn("ingest_time", current_timestamp()) \
    .drop("timestamp") # Bỏ cột string gốc, dùng cột timestamp đã cast

# ================= WRITE STREAM (FOREACH BATCH) =================
def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    
    print(f"Processing Batch ID: {batch_id} | Records: {batch_df.count()}")
    
    try:
        # Repartition để tránh lỗi Small Files (quan trọng với Iceberg)
        # Sắp xếp theo partition key để ghi hiệu quả hơn
        batch_df.repartition(2) \
                .sortWithinPartitions("event_time") \
                .writeTo(FULL_TABLE_NAME) \
                .append()
                
        print(f"   -> Written to {FULL_TABLE_NAME}")
    except Exception as e:
        print(f"   -> ERROR writing batch {batch_id}: {e}")
        traceback.print_exc()

# Chạy Streaming Query
query = df_transformed.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="10 seconds") \
    .start()

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Stopping query...")
    query.stop()