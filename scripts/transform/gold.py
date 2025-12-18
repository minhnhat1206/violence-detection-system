import sys
import time
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, current_timestamp,
    sum, avg, max, struct, first,
    hour, minute, sha2, concat_ws, when
)

# ================= 1. CẤU HÌNH =================
CATALOG = "iceberg"
NS = "default"
BRONZE_TABLE = f"{CATALOG}.{NS}.bronzeViolence"
DIM_LOC_TBL = f"{CATALOG}.{NS}.dim_location"
DIM_CAM_TBL = f"{CATALOG}.{NS}.dim_camera"
FACT_TBL = f"{CATALOG}.{NS}.fact_camera_monitoring"


def create_spark():
    spark = (
        SparkSession.builder
        .appName("GoldLayer_MemoryOptimized_Batch")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.memory.storageFraction", "0.1")
        .config("spark.memory.fraction", "0.8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def run_gold_batch():
    spark = None
    try:
        spark = create_spark()

        print(f"BẮT ĐẦU GOLD BATCH: Đọc dữ liệu từ {BRONZE_TABLE}")

        # 2.1 ĐỌC BRONZE
        raw_df = spark.read.format("iceberg").load(BRONZE_TABLE)

        if raw_df.isEmpty():
            print("THÔNG TIN: Bronze không có dữ liệu, bỏ qua chu kỳ.")
            return

        # 2.2 AGGREGATE + CACHE
        df_final_computed = (
            raw_df
            .groupBy(
                window(col("event_time"), "5 minutes"),
                col("camera_id"),
                col("city"), col("district"), col("ward"), col("street"),
                col("rtsp_url"), col("risk_level")
            )
            .agg(
                first("latitude").alias("latitude"),
                first("longitude").alias("longitude"),
                avg("latency_ms").alias("avg_latency_ms"),
                avg("fps").alias("avg_fps"),
                sum(when(col("is_violent") == True, 0.5).otherwise(5.0)).alias("total_duration_sec"),
                sum(when(col("is_violent") == True, 0.5).otherwise(0.0)).alias("violent_duration_sec"),
                sum(when(col("is_violent") == True, 1).otherwise(0)).alias("alert_count"),
                max(struct(col("score"), col("evidence_url"))).alias("max_risk_struct")
            )
            .select(
                col("window.start").alias("window_start"),
                "camera_id",
                "city", "district", "ward", "street",
                "latitude", "longitude",
                "rtsp_url", "risk_level",
                "avg_latency_ms", "avg_fps",
                "total_duration_sec", "violent_duration_sec", "alert_count",
                ((col("total_duration_sec") / 300.0) * 100.0).alias("uptime_percent"),
                (col("alert_count") > 0).alias("is_violent_window"),
                col("max_risk_struct.score").alias("max_risk_score"),
                col("max_risk_struct.evidence_url").alias("evidence_url")
            )
            .withColumn("time_key", (hour("window_start") * 100 + minute("window_start")).cast("int"))
            .withColumn("location_key", sha2(concat_ws("|", "city", "district", "ward", "street"), 256))
            .withColumn("camera_key", col("camera_id"))
            .withColumn("fact_id", sha2(concat_ws("-", "camera_id", "window_start"), 256))
            .withColumn("etl_ts", current_timestamp())
            .cache()
        )

        record_count = df_final_computed.count()
        print(f"TIẾN TRÌNH: Tổng hợp xong {record_count} bản ghi")

        if record_count == 0:
            df_final_computed.unpersist()
            return

        # 2.3 DIM LOCATION (CÓ LAT/LONG)
        print("ĐANG CẬP NHẬT: dim_location")

        df_final_computed \
            .select(
                "location_key",
                "city", "district", "ward", "street",
                "latitude", "longitude"
            ) \
            .distinct() \
            .write.format("iceberg") \
            .mode("overwrite") \
            .saveAsTable(DIM_LOC_TBL)

        # 2.4 DIM CAMERA
        print("ĐANG CẬP NHẬT: dim_camera")

        df_final_computed \
            .select("camera_key", "camera_id", "rtsp_url", "risk_level") \
            .withColumn("last_updated", current_timestamp()) \
            .distinct() \
            .write.format("iceberg") \
            .mode("overwrite") \
            .saveAsTable(DIM_CAM_TBL)

        # 2.5 FACT
        print("ĐANG GHI: fact_camera_monitoring")

        df_final_computed.select(
            "fact_id", "time_key", "location_key", "camera_key", "window_start",
            "total_duration_sec", "uptime_percent",
            "avg_latency_ms", "avg_fps",
            "is_violent_window", "violent_duration_sec",
            "max_risk_score", "alert_count",
            "evidence_url", "etl_ts"
        ).write.format("iceberg") \
         .mode("append") \
         .saveAsTable(FACT_TBL)

        print("THÀNH CÔNG: Gold batch hoàn tất")

        df_final_computed.unpersist()

    except Exception as e:
        print(f"LỖI GOLD BATCH: {e}")
        traceback.print_exc()

    finally:
        if spark:
            spark.stop()
            print("Spark đã dừng, giải phóng tài nguyên")


if __name__ == "__main__":
    run_gold_batch()
