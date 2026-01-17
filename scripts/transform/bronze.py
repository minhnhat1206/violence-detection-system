import time, traceback, threading
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, current_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType
from prometheus_client import start_http_server, Counter, Gauge

# ----- Metrics (Prometheus) -----
PROM_PORT = 8005
PROCESSED = Counter("bronze_records_total", "Total bronze records")
BATCH_DUR = Gauge("bronze_batch_duration_seconds", "Batch duration (s)")

# ----- System Config -----
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "urban-safety-alerts"
CATALOG = "iceberg"
NS = "default"
BRONZE_TBL = f"{CATALOG}.{NS}.bronzeViolence"
WAREHOUSE = "s3a://warehouse/"
CHECKPOINT = "s3a://checkpoint/bronzeViolence/"

# Retain more snapshots for stable Trino verification
RETAIN_SNAPSHOTS = 100 

# ----- Data Schema Definition -----
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

# ----- Initialize Spark Session -----
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

# Hide unnecessary system log info
spark.sparkContext.setLogLevel("ERROR")

# Ensure Iceberg table exists
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
    """Processing function for each data batch (Micro-batch)"""
    start_time = time.time()
    try:
        record_count = batch_df.count()
        
        separator = "-" * 50
        print(separator)
        print(f"BATCH {batch_id} | Data received: {record_count} records")
        
        if record_count > 0:
            # Write data to Bronze layer
            batch_df.withColumn("event_time", to_timestamp(col("timestamp"))) \
                    .withColumn("event_date", to_date(col("timestamp"))) \
                    .withColumn("ingest_time", current_timestamp()) \
                    .drop("timestamp") \
                    .writeTo(BRONZE_TBL).append()

            # Metadata Maintenance: Retain snapshot history
            if batch_id % 5 == 0:
                print(f"Metadata Maintenance: Expire Snapshots (retain={RETAIN_SNAPSHOTS})")
                spark.sql(f"CALL {CATALOG}.system.expire_snapshots(table => '{BRONZE_TBL}', retain_last => {RETAIN_SNAPSHOTS})")

            PROCESSED.inc(record_count)
            duration = time.time() - start_time
            BATCH_DUR.set(duration)
            print(f"Finished Batch {batch_id} in {duration:.2f}s")
        else:
            print("Waiting for new data from Kafka...")
            
        print(separator)

    except Exception as e:
        print(f"ERROR AT BATCH {batch_id}: {e}")
        traceback.print_exc()

# Run Prometheus Metrics Server
def _start_metrics():
    start_http_server(PROM_PORT)
threading.Thread(target=_start_metrics, daemon=True).start()

# Configure Kafka Stream Reading
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

# Start Streaming thread
print(f"Bronze Stream is starting. Topic: {KAFKA_TOPIC}")
query = (parsed.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="60 seconds")
    .start()
)

query.awaitTermination()