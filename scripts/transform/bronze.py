import sys
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, current_timestamp, to_date
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
TABLE_NAME = "bronzeViolence"  # Đổi tên bảng để tránh conflict schema cũ
FULL_TABLE_NAME = f"{CATALOG_NAME}.{NAMESPACE}.{TABLE_NAME}"

# Đường dẫn (MinIO/S3)
WAREHOUSE_PATH = "s3a://warehouse/"
CHECKPOINT_PATH = "s3a://checkpoint/bronzeViolence/"

# ================= SCHEMA DEFINITION =================
SCHEMA = StructType([
    StructField("camera_id", StringType(), True),
    StructField("city", StringType(), True),   
    StructField("district", StringType(), True),     
    StructField("ward", StringType(), True),     
    StructField("street", StringType(), True),     
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("rtsp_url", StringType(), True),     
    StructField("risk_level", StringType(), True),     
    StructField("timestamp", StringType(), True),
    StructField("ai_timestamp", StringType(), True),
    StructField("is_violent", BooleanType(), True),
    StructField("score", DoubleType(), True),
    StructField("fps", DoubleType(), True),
    StructField("latency_ms", DoubleType(), True),
    StructField("evidence_url", StringType(), True)
])

# ================= SPARK SESSION SETUP =================
spark = SparkSession.builder \
    .appName("BronzeLayer_Ingestion") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
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
def ensure_iceberg_table():
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.{NAMESPACE}")
        
        # SỬA ĐỔI: Thêm cột event_date và Partition theo cột này
        # Loại bỏ days(event_time) gây lỗi
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
            event_date date, 
            ai_timestamp string,
            is_violent boolean,
            score double,
            fps double,
            latency_ms double,
            evidence_url string,
            ingest_time timestamp
        )
        USING iceberg
        PARTITIONED BY (event_date)
        """
        spark.sql(create_sql)
        print(f"[Iceberg] Table {FULL_TABLE_NAME} is ready.")
    except Exception as e:
        print(f"[Iceberg] Init failed: {e}")
        traceback.print_exc()
        sys.exit(1) # Dừng chương trình nếu không tạo được bảng

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
df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), SCHEMA).alias("data")) \
    .select("data.*")

# SỬA ĐỔI: Tạo cột event_date từ event_time để phục vụ Partition
df_transformed = df_parsed \
    .withColumn("event_time", to_timestamp(col("timestamp"))) \
    .withColumn("event_date", to_date(col("timestamp"))) \
    .withColumn("ingest_time", current_timestamp()) \
    .drop("timestamp")

# ================= WRITE STREAM (FOREACH BATCH) =================
def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    
    print(f"Processing Batch ID: {batch_id} | Records: {batch_df.count()}")
    
    try:
        # Sort theo partition key để tối ưu ghi file
        batch_df.repartition(2) \
                .sortWithinPartitions("event_date") \
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