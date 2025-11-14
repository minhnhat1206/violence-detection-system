# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, from_json, date_format, to_timestamp
# from pyspark.sql.types import (
#     StructType, StructField, StringType, DoubleType,
#     IntegerType, TimestampType, ArrayType
# )

# # --- CẤU HÌNH ---
# KAFKA_BROKER = "kafka:9092"
# KAFKA_TOPIC = "model.inference.results"
# MINIO_SINK_PATH = "s3a://inference-results/data/"
# CHECKPOINT_PATH = "s3a://inference-results/checkpoint/"


# # --- SCHEMA CỦA MODEL.INFERENCE.RESULTS ---
# METADATA_SCHEMA = StructType([
#     StructField("notes", StringType(), True)
# ])

# BBOX_SCHEMA = ArrayType(ArrayType(DoubleType()))

# SCHEMA = StructType([
#     StructField("event_id", StringType(), False),
#     StructField("camera_id", StringType(), False),
#     StructField("timestamp_utc", StringType(), False),  # sẽ chuyển đổi sau
#     StructField("frame_s3_path", StringType(), True),
#     StructField("label", StringType(), True),
#     StructField("score", DoubleType(), True),
#     StructField("bbox", BBOX_SCHEMA, True),
#     StructField("model_name", StringType(), True),
#     StructField("model_version", StringType(), True),
#     StructField("latency_ms", IntegerType(), True),
#     StructField("extra", METADATA_SCHEMA, True),
# ])

# # --- KHỞI TẠO SPARK SESSION ---
# spark = SparkSession.builder \
#     .appName("KafkaParquetSink") \
#     .config(
#         "spark.jars.packages",
#         "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2,org.apache.hadoop:hadoop-aws:3.3.4"
#     ) \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# # --- ĐỌC STREAM TỪ KAFKA ---
# df_stream = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", KAFKA_BROKER) \
#     .option("subscribe", KAFKA_TOPIC) \
#     .option("startingOffsets", "earliest") \
#     .load()

# # --- CHUYỂN ĐỔI DỮ LIỆU ---
# df_processed = df_stream \
#     .selectExpr("CAST(value AS STRING) as json_payload", "CAST(key AS STRING) as camera_key") \
#     .select(from_json(col("json_payload"), SCHEMA).alias("data"), col("camera_key")) \
#     .select("data.*") \
#     .withColumn(
#         "processed_timestamp",
#         to_timestamp(col("timestamp_utc"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
#     ) \
#     .withColumn(
#         "date",
#         date_format(col("processed_timestamp"), "yyyy-MM-dd")
#     ) \
#     .select(
#         col("event_id"),
#         col("camera_id"),
#         col("processed_timestamp").alias("timestamp_utc"),
#         col("frame_s3_path"),
#         col("label"),
#         col("score"),
#         col("bbox"),
#         col("model_name"),
#         col("model_version"),
#         col("latency_ms"),
#         col("extra"),
#         col("date")
#     )

# # --- GHI STREAM VÀO MINIO (S3A) ---
# print(f"Bắt đầu ghi dữ liệu từ Kafka topic '{KAFKA_TOPIC}' vào MinIO path: {MINIO_SINK_PATH}")

# query = df_processed.writeStream \
#     .outputMode("append") \
#     .format("parquet") \
#     .option("path", MINIO_SINK_PATH) \
#     .option("checkpointLocation", CHECKPOINT_PATH) \
#     .partitionBy("date", "camera_id") \
#     .trigger(processingTime="30 seconds") \
#     .start()

# query.awaitTermination()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, date_format, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, ArrayType
)

# --- CẤU HÌNH ---
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "model.inference.results"
MINIO_SINK_PATH = "s3a://inference-results/data/"
CHECKPOINT_PATH = "s3a://inference-results/checkpoint/"

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

# --- WRITE STREAM TO MINIO (PARQUET) ---
print(f"Bắt đầu ghi dữ liệu từ Kafka topic '{KAFKA_TOPIC}' vào MinIO path: {MINIO_SINK_PATH}")

query = df_processed.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", MINIO_SINK_PATH) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .option("parquet.compression", "snappy") \
    .partitionBy("date", "camera_id") \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()
