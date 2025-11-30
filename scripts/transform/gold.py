import sys
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, current_timestamp, 
    sum, count, avg, max, min, first,
    hour, minute, floor, ceil,
    sha2, concat_ws, when, lit, struct
)
from pyspark.sql.types import TimestampType

# ================= 1. CẤU HÌNH (ĐỒNG BỘ VỚI BRONZE) =================
# MinIO / S3 Config
WAREHOUSE_PATH = "s3a://warehouse/"
CHECKPOINT_PATH = "s3a://checkpoint/gold_monitoring/"

# Iceberg Catalog
CATALOG = "iceberg"
NS = "default"
BRONZE_TABLE = f"{CATALOG}.{NS}.bronzeViolence"

# Output Tables (Gold Layer)
DIM_TIME_TBL = f"{CATALOG}.{NS}.dim_time"
DIM_LOC_TBL = f"{CATALOG}.{NS}.dim_location"
DIM_CAM_TBL = f"{CATALOG}.{NS}.dim_camera"
FACT_TBL = f"{CATALOG}.{NS}.fact_camera_monitoring"

# Setup Spark Session
spark = SparkSession.builder \
    .appName("GoldLayer_StarSchema_ETL") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.0") \
    .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{CATALOG}.type", "hive") \
    .config(f"spark.sql.catalog.{CATALOG}.uri", "thrift://hive-metastore:9083") \
    .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE_PATH) \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "mypassword") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ================= 2. DDL: CHUẨN BỊ BẢNG STAR SCHEMA =================
def init_gold_layer():
    print("🛠 Initializing Gold Layer Schema...")
    
    try:
        # 2.1 DIM_TIME (Static)
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {DIM_TIME_TBL} (
                time_key INT, hour INT, minute INT, time_of_day STRING
            ) USING iceberg
        """)
        
        # Populate DIM_TIME
        if spark.table(DIM_TIME_TBL).count() == 0:
            print("   -> Populating DIM_TIME...")
            spark.range(0, 1440).select(
                (floor(col("id") / 60) * 100 + (col("id") % 60)).alias("time_key"),
                floor(col("id") / 60).alias("hour"),
                (col("id") % 60).alias("minute"),
                when(col("id") < 360, "Night")
                .when(col("id") < 720, "Morning")
                .when(col("id") < 1080, "Afternoon")
                .otherwise("Evening").alias("time_of_day")
            ).writeTo(DIM_TIME_TBL).append()
        
        # 2.2 DIM_LOCATION
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {DIM_LOC_TBL} (
                location_key STRING, city STRING, district STRING, ward STRING, street STRING
            ) USING iceberg
        """)

        # 2.3 DIM_CAMERA
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {DIM_CAM_TBL} (
                camera_key STRING, camera_id STRING, rtsp_url STRING, 
                risk_level STRING, last_updated TIMESTAMP
            ) USING iceberg
        """)

        # 2.4 FACT_MONITORING
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {FACT_TBL} (
                fact_id STRING,
                time_key INT,
                location_key STRING,
                camera_key STRING,
                window_start TIMESTAMP,
                total_duration_sec DOUBLE,
                uptime_percent DOUBLE,
                avg_latency_ms DOUBLE,
                avg_fps DOUBLE,
                is_violent_window BOOLEAN,
                violent_duration_sec DOUBLE,
                max_risk_score DOUBLE,
                alert_count LONG,
                evidence_url STRING,
                etl_ts TIMESTAMP
            ) USING iceberg
            PARTITIONED BY (days(window_start))
        """)
        print("Schema Ready.")
    except Exception as e:
        print(f"Init failed: {e}")
        traceback.print_exc()

init_gold_layer()

# ================= 3. CORE LOGIC =================
print(f"Reading stream from {BRONZE_TABLE}")
raw_stream = spark.readStream \
    .format("iceberg") \
    .option("streaming-skip-delete-snapshots", "true") \
    .option("streaming-skip-overwrite-snapshots", "true") \
    .load(BRONZE_TABLE)

# 3.1 CLEANING & WATERMARK
df_clean = raw_stream \
    .drop("image_preview") \
    .withWatermark("event_time", "10 minutes")

# 3.2 AGGREGATION
df_agg = df_clean.groupBy(
    window(col("event_time"), "5 minutes"),
    col("camera_id"),
    col("city"), col("district"), col("ward"), col("street"),
    col("rtsp_url"), col("risk_level")
).agg(
    count("*").alias("total_msgs"),
    avg("latency_ms").alias("avg_latency_ms"),
    avg("fps").alias("avg_fps"),
    sum(when(col("is_violent") == True, 0.5).otherwise(5.0)).alias("total_duration_sec"),
    sum(when(col("is_violent") == True, 0.5).otherwise(0.0)).alias("violent_duration_sec"),
    sum(when(col("is_violent") == True, 1).otherwise(0)).alias("alert_count"),
    max(struct(col("score"), col("evidence_url"))).alias("max_risk_struct")
).select(
    col("window.start").alias("window_start"),
    col("camera_id"),
    col("city"), col("district"), col("ward"), col("street"),
    col("rtsp_url"), col("risk_level"),
    col("total_duration_sec"),
    (col("total_duration_sec") / 300.0 * 100).cast("double").alias("uptime_percent"),
    col("avg_latency_ms"),
    col("avg_fps"),
    col("violent_duration_sec"),
    col("alert_count"),
    (col("alert_count") > 0).alias("is_violent_window"),
    col("max_risk_struct.score").alias("max_risk_score"),
    col("max_risk_struct.evidence_url").alias("evidence_url")
)

# 3.3 Key Prep
df_final = df_agg \
    .withColumn("time_key", (hour("window_start") * 100 + minute("window_start")).cast("int")) \
    .withColumn("location_key", sha2(concat_ws("|", "city", "district", "ward", "street"), 256)) \
    .withColumn("camera_key", col("camera_id")) \
    .withColumn("fact_id", sha2(concat_ws("-", "camera_id", "window_start"), 256)) \
    .withColumn("etl_ts", current_timestamp())

# ================= 4. WRITER (FIXED GlobalTempView) =================
def merge_data_gold(batch_df, batch_id):
    if batch_df.isEmpty(): return
    print(f"⚡ Batch {batch_id}: Processing {batch_df.count()} windows...")
    
    _spark = batch_df.sparkSession
    batch_df.cache()
    
    try:
        # Upsert DIM_LOCATION (Global View)
        loc_df = batch_df.select("location_key", "city", "district", "ward", "street").distinct()
        loc_df.createOrReplaceGlobalTempView("src_loc_global")
        
        _spark.sql(f"""
            MERGE INTO {DIM_LOC_TBL} t 
            USING global_temp.src_loc_global s ON t.location_key = s.location_key
            WHEN NOT MATCHED THEN INSERT *
        """)
        
        # Upsert DIM_CAMERA (Global View)
        cam_df = batch_df.select("camera_key", "camera_id", "rtsp_url", "risk_level") \
            .withColumn("last_updated", current_timestamp()) \
            .distinct()
        cam_df.createOrReplaceGlobalTempView("src_cam_global")
            
        _spark.sql(f"""
            MERGE INTO {DIM_CAM_TBL} t 
            USING global_temp.src_cam_global s ON t.camera_key = s.camera_key
            WHEN MATCHED THEN 
                UPDATE SET t.rtsp_url = s.rtsp_url, t.risk_level = s.risk_level, t.last_updated = s.last_updated
            WHEN NOT MATCHED THEN INSERT *
        """)
        
        # Append FACT
        batch_df.select(
            "fact_id", "time_key", "location_key", "camera_key", "window_start",
            "total_duration_sec", "uptime_percent", "avg_latency_ms", "avg_fps",
            "is_violent_window", "violent_duration_sec", "max_risk_score", "alert_count", "evidence_url",
            "etl_ts"
        ).writeTo(FACT_TBL).append()
        
        print(f"   -> [Gold] Data merged successfully.")
        
    except Exception as e:
        print(f"Error in batch {batch_id}: {e}")
        traceback.print_exc()
    finally:
        batch_df.unpersist()

# ================= 5. RUN QUERY =================
print("Starting Gold Layer Stream...")

query = df_final.writeStream \
    .outputMode("append") \
    .foreachBatch(merge_data_gold) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="1 minute") \
    .start()

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Stopping Gold Query...")
    query.stop()