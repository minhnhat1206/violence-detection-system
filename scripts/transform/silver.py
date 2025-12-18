import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, sha2, concat_ws

# ================= CONFIG =================
CATALOG = "iceberg"
NS = "default"

BRONZE_TBL = f"{CATALOG}.{NS}.bronzeViolence"
SILVER_TBL = f"{CATALOG}.{NS}.silverViolence"

WAREHOUSE = "s3a://warehouse/"
LOCAL_TMP = "/opt/spark-temp"

# ================= LOG =================
def log(msg):
    print(f"[SILVER] {msg}", flush=True)

# ================= SPARK =================
spark = (
    SparkSession.builder
    .appName("SilverLayer_Batch")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CATALOG}.type", "hive")
    .config(f"spark.sql.catalog.{CATALOG}.uri", "thrift://hive-metastore:9083")
    .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio")
    .config("spark.hadoop.fs.s3a.secret.key", "mypassword")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.local.dir", LOCAL_TMP)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
log("Spark session created")

# ================= TABLE INIT =================
log("Ensuring Silver table exists")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_TBL} (
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
    ingest_time timestamp,
    location_key string,
    camera_key string,
    etl_ts timestamp
)
USING iceberg
PARTITIONED BY (event_date)
""")

# ================= MAIN =================
def run():
    try:
        log("Starting Silver batch job")

        log("Reading Bronze table")
        df_all = spark.read.format("iceberg").load(BRONZE_TBL)
        log(f"Total Bronze records = {df_all.count()}")

        df = (
            df_all
            .where("event_date >= current_date() - 1")
            .where("event_time IS NOT NULL")
        )

        cnt = df.count()
        log(f"Records after filter = {cnt}")

        if cnt == 0:
            log("No data to process, exiting")
            return

        log("Transforming to Silver")
        silver = (
            df.repartition(4)
              .withColumn(
                  "location_key",
                  sha2(concat_ws("|", "city", "district", "ward", "street"), 256)
              )
              .withColumn("camera_key", col("camera_id"))
              .withColumn("etl_ts", current_timestamp())
        )

        log("Writing to Silver table")
        silver.writeTo(SILVER_TBL).append()

        log("Expiring old snapshots")
        spark.sql(f"""
            CALL {CATALOG}.system.expire_snapshots(
                table => '{SILVER_TBL}',
                retain_last => 1
            )
        """)

        log("Silver batch completed successfully")

    except Exception:
        log("ERROR in Silver job")
        traceback.print_exc()
    finally:
        log("Stopping Spark")
        spark.stop()

if __name__ == "__main__":
    run()
