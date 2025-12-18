# scripts/transform/bronze.py
import time, traceback, threading
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, current_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

from prometheus_client import start_http_server, Counter, Gauge

# ----- Metrics -----
PROM_PORT = 8005
PROCESSED = Counter("bronze_records_total","Total bronze records")
BATCH_DUR = Gauge("bronze_batch_duration_seconds","Batch duration (s)")

# ----- Config -----
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "urban-safety-alerts"

CATALOG = "iceberg"
NS = "default"
BRONZE_TBL = f"{CATALOG}.{NS}.bronzeViolence"

WAREHOUSE = "s3a://warehouse/"
CHECKPOINT = "s3a://checkpoint/bronzeViolence/"

# Tunables (thử tăng/decrease tuỳ host)
MAX_OFFSETS_PER_TRIGGER = 2000   # GIỚI HẠN ingest; giảm nếu crash
MICRO_BATCH_SECONDS = 120       # tăng để giảm metadata churn
COALESCE_FILES = 2              # files per batch, không để 1 nếu metadata heavy
EXPIRE_EVERY_N_BATCH = 5        # clean snapshots mỗi N batch
TARGET_FILE_SIZE = 128 * 1024 * 1024  # 128MB

# ----- Schema -----
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

# ----- Spark session -----
spark = (
    SparkSession.builder
    .appName("BronzeLayer_Realtime_Resilient")
    .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config(f"spark.sql.catalog.{CATALOG}","org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CATALOG}.type","hive")
    .config(f"spark.sql.catalog.{CATALOG}.uri","thrift://hive-metastore:9083")
    .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)

    # IO / files
    .config("spark.sql.files.maxRecordsPerFile","200000")
    .config("spark.sql.iceberg.write.target-file-size-bytes", str(TARGET_FILE_SIZE))

    # conservative shuffle/partitions for small worker
    .config("spark.sql.shuffle.partitions","2")
    .config("spark.default.parallelism","2")

    # memory tuning (tweak if you change worker mem)
    .config("spark.memory.fraction","0.7")
    .config("spark.memory.storageFraction","0.3")

    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ----- ensure table -----
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {BRONZE_TBL} (
    camera_id string, city string, district string, ward string, street string,
    latitude double, longitude double, rtsp_url string, risk_level string,
    event_time timestamp, event_date date, ai_timestamp string,
    is_violent boolean, score double, fps double, latency_ms double,
    evidence_url string, ingest_time timestamp
) USING iceberg PARTITIONED BY (event_date)
""")

# ----- metrics server -----
def _start_metrics(): start_http_server(PROM_PORT)
threading.Thread(target=_start_metrics, daemon=True).start()

# ----- batch processor -----
def process_batch(batch_df, batch_id):
    start = time.time()
    try:
        if batch_df.isEmpty():
            return

        n = batch_df.count()
        print(f"[BRONZE] Batch {batch_id} records={n}")

        # safe write: coalesce small (not 1), append to Iceberg
        batch_df.coalesce(COALESCE_FILES) \
                .withColumn("event_time", to_timestamp(col("timestamp"))) \
                .withColumn("event_date", to_date(col("timestamp"))) \
                .withColumn("ingest_time", current_timestamp()) \
                .drop("timestamp") \
                .writeTo(BRONZE_TBL).append()

        # expire snapshots & remove orphan files occasionally
        if batch_id % EXPIRE_EVERY_N_BATCH == 0:
            try:
                spark.sql(f"CALL {CATALOG}.system.expire_snapshots(table => '{BRONZE_TBL}', retain_last => 2)")
            except Exception as e:
                print("[BRONZE] expire_snapshots error:", e)
            try:
                spark.sql(f"CALL {CATALOG}.system.remove_orphan_files(table => '{BRONZE_TBL}', older_than => TIMESTAMP '1970-01-01 00:00:00')")
            except Exception as e:
                print("[BRONZE] remove_orphan_files error:", e)

        # metrics
        PROCESSED.inc(n)
        BATCH_DUR.set(time.time() - start)

    except Exception as e:
        print("[BRONZE] ERROR writing batch:", e)
        traceback.print_exc()

# ----- stream read with maxOffsetsPerTrigger -----
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", str(MAX_OFFSETS_PER_TRIGGER))
    .load()
)

parsed = (kafka_df
    .selectExpr("CAST(value AS STRING) as json")
    .select(from_json(col("json"), SCHEMA).alias("d"))
    .select("d.*")
)

query = (parsed.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime=f"{MICRO_BATCH_SECONDS} seconds")
    .start()
)

query.awaitTermination()
