import sys
import time
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, current_timestamp, sum, avg, max, 
    struct, first, hour, minute, sha2, concat_ws, when
)

# ================= 1. CẤU HÌNH =================
CATALOG = "iceberg"
NS = "default"
BRONZE_TABLE = f"{CATALOG}.{NS}.bronzeViolence"
DIM_LOC_TBL = f"{CATALOG}.{NS}.dim_location"
DIM_CAM_TBL = f"{CATALOG}.{NS}.dim_camera"
DIM_TIME_TBL = f"{CATALOG}.{NS}.dim_time"
FACT_TBL = f"{CATALOG}.{NS}.fact_camera_monitoring"
RAG_TBL = f"{CATALOG}.{NS}.violence_events_for_rag"

def create_spark():
    return SparkSession.builder \
        .appName("GoldLayer_Full_Transform") \
        .getOrCreate()

def run_gold_batch():
    spark = None
    try:
        spark = create_spark()
        raw_df = spark.read.format("iceberg").load(BRONZE_TABLE)
        if raw_df.isEmpty(): return

        # 2.2 AGGREGATE
        df_computed = (
            raw_df
            .groupBy(
                window(col("event_time"), "5 minutes"),
                "camera_id", "city", "district", "ward", "street", "rtsp_url", "risk_level"
            )
            .agg(
                first("latitude").alias("latitude"),
                first("longitude").alias("longitude"),
                avg("latency_ms").alias("avg_latency_ms"),
                avg("fps").alias("avg_fps"),
                sum(when(col("is_violent") == True, 1).otherwise(0)).alias("alert_count"),
                max(struct(col("score"), col("evidence_url"))).alias("max_risk_struct")
            )
            .select(
                col("window.start").alias("window_start"),
                "*",
                (col("alert_count") > 0).alias("is_violent_window"),
                col("max_risk_struct.score").alias("max_risk_score"),
                col("max_risk_struct.evidence_url").alias("evidence_url")
            )
            .withColumn("time_key", (hour("window_start") * 100 + minute("window_start")).cast("int"))
            .withColumn("location_key", sha2(concat_ws("|", "city", "district", "ward", "street"), 256))
            .withColumn("camera_key", col("camera_id"))
            .withColumn("fact_id", sha2(concat_ws("-", "camera_id", "window_start"), 256))
            .cache()
        )

        # 2.3 DIM TIME 
        print("ĐANG CẬP NHẬT: dim_time")
        df_computed.select("time_key", hour("window_start").alias("hour"), minute("window_start").alias("minute")) \
            .distinct().write.format("iceberg").mode("append").saveAsTable(DIM_TIME_TBL)

        # 2.4 DIM LOCATION & CAMERA 
        print("ĐANG CẬP NHẬT: dim_location & dim_camera")
        df_computed.select("location_key", "city", "district", "ward", "street", "latitude", "longitude") \
            .distinct().write.format("iceberg").mode("append").saveAsTable(DIM_LOC_TBL)
            
        df_computed.select("camera_key", "camera_id", "rtsp_url", "risk_level") \
            .distinct().write.format("iceberg").mode("append").saveAsTable(DIM_CAM_TBL)

        # 2.5 FACT
        print("ĐANG GHI: fact_camera_monitoring")
        df_computed.select(
            "fact_id", "time_key", "location_key", "camera_key", "window_start",
            "avg_latency_ms", "avg_fps", "is_violent_window", "max_risk_score", "alert_count", "evidence_url"
        ).write.format("iceberg").mode("append").saveAsTable(FACT_TBL)

        # 2.6 DATA CHO RAG CHATBOT 
        print("ĐANG GHI: violence_events_for_rag")
        df_computed.filter(col("is_violent_window") == True) \
            .select(
                col("fact_id").alias("event_id"),
                col("window_start").alias("timestamp_utc"),
                "camera_id", "city", "district", "ward", "street",
                col("max_risk_score").alias("score"),
                when(col("is_violent_window"), "VIOLENCE").alias("label")
            ).write.format("iceberg").mode("append").saveAsTable(RAG_TBL)

        print("THÀNH CÔNG: Gold batch hoàn tất")
        df_computed.unpersist()

    except Exception as e:
        print(f"LỖI: {e}")
    finally:
        if spark: spark.stop()

if __name__ == "__main__":
    run_gold_batch()