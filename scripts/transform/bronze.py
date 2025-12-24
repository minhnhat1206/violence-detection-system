import time, traceback, threading
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, current_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType
from prometheus_client import start_http_server, Counter, Gauge

# ----- Metrics (Prometheus) -----
PROM_PORT = 8005
PROCESSED = Counter("bronze_records_total", "Total bronze records")
BATCH_DUR = Gauge("bronze_batch_duration_seconds", "Batch duration (s)")

# ----- Cau hinh he thong -----
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "urban-safety-alerts"
CATALOG = "iceberg"
NS = "default"
BRONZE_TBL = f"{CATALOG}.{NS}.bronzeViolence"
WAREHOUSE = "s3a://warehouse/"
CHECKPOINT = "s3a://checkpoint/bronzeViolence/"

# Giu lai nhieu snapshot hon de Trino truy van on dinh
RETAIN_SNAPSHOTS = 100 

# ----- Dinh nghia Schema du lieu -----
SCHEMA = StructType([
    StructField("camera_id", StringType()),
    StructField("city", StringType()),
    StructField("district", StringType()),
    StructField("ward", StringType()),
    StructField("street", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("rtsp_url", StringType()),
    StructField("risk_level", StringType()),
    StructField("timestamp", StringType()),
    StructField("ai_timestamp", StringType()),
    StructField("is_violent", BooleanType()),
    StructField("score", DoubleType()),
    StructField("fps", DoubleType()),
    StructField("latency_ms", DoubleType()),
    StructField("evidence_url", StringType())
])

# ----- Khoi tao Spark Session -----
spark = (
    SparkSession.builder
    .appName("BronzeLayer_Streaming_CleanLog")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CATALOG}.type", "hive")
    .config(f"spark.sql.catalog.{CATALOG}.uri", "thrift://hive-metastore:9083")
    .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)
    .getOrCreate()
)

# An cac log thong tin he thong khong can thiet
spark.sparkContext.setLogLevel("ERROR")

# Dam bao bang Iceberg ton tai
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_TBL} (
    camera_id string, city string, district string, ward string, street string,
    latitude double, longitude double, rtsp_url string, risk_level string,
    event_time timestamp, event_date date, ai_timestamp string,
    is_violent boolean, score double, fps double, latency_ms double,
    evidence_url string, ingest_time timestamp
) USING iceberg PARTITIONED BY (event_date)
""")

def process_batch(batch_df, batch_id):
    """Ham xu ly cho tung dot du lieu (Micro-batch)"""
    start_time = time.time()
    try:
        record_count = batch_df.count()
        
        separator = "-" * 50
        print(separator)
        print(f"BATCH {batch_id} | Du lieu nhan: {record_count} ban ghi")
        
        if record_count > 0:
            # Ghi du lieu vao tang Bronze
            batch_df.withColumn("event_time", to_timestamp(col("timestamp"))) \
                    .withColumn("event_date", to_date(col("timestamp"))) \
                    .withColumn("ingest_time", current_timestamp()) \
                    .drop("timestamp") \
                    .writeTo(BRONZE_TBL).append()

            # Bao tri Metadata: Giu lai lich su snapshots
            if batch_id % 5 == 0:
                print(f"Bao tri Metadata: Expire Snapshots (retain={RETAIN_SNAPSHOTS})")
                spark.sql(f"CALL {CATALOG}.system.expire_snapshots(table => '{BRONZE_TBL}', retain_last => {RETAIN_SNAPSHOTS})")

            PROCESSED.inc(record_count)
            duration = time.time() - start_time
            BATCH_DUR.set(duration)
            print(f"Hoan tat Batch {batch_id} trong {duration:.2f}s")
        else:
            print("Dang doi du lieu moi tu Kafka...")
            
        print(separator)

    except Exception as e:
        print(f"LOI TAI BATCH {batch_id}: {e}")
        traceback.print_exc()

# Chay Prometheus Metrics Server
def _start_metrics():
    start_http_server(PROM_PORT)
threading.Thread(target=_start_metrics, daemon=True).start()

# Cau hinh doc Kafka Stream
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed = (kafka_df
    .selectExpr("CAST(value AS STRING) as json")
    .select(from_json(col("json"), SCHEMA).alias("d"))
    .select("d.*")
)

# Khoi chay luong Streaming
print(f"Bronze Stream dang khoi dong. Topic: {KAFKA_TOPIC}")
query = (parsed.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="60 seconds")
    .start()
)

query.awaitTermination()