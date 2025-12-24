import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, avg, sum, max, struct, first,
    hour, minute, sha2, concat_ws, when,
    date_format
)

# ================= CONFIG =================
CATALOG = "iceberg"
NS = "default"

BRONZE_TABLE = f"{CATALOG}.{NS}.bronzeViolence"

DIM_TIME_TBL = f"{CATALOG}.{NS}.dim_time"
DIM_LOC_TBL  = f"{CATALOG}.{NS}.dim_location"
DIM_CAM_TBL  = f"{CATALOG}.{NS}.dim_camera"

FACT_TBL = f"{CATALOG}.{NS}.fact_camera_monitoring"
RAG_TBL  = f"{CATALOG}.{NS}.violence_events_for_rag"


def create_spark():
    return (
        SparkSession.builder
        .appName("GoldLayer_Rebuild_Safe")
        .getOrCreate()
    )


def run_gold_batch():
    spark = None
    try:
        spark = create_spark()

        bronze_df = spark.read.format("iceberg").load(BRONZE_TABLE)
        if bronze_df.isEmpty():
            print("Bronze rỗng – không có gì để build Gold")
            return

        # ================= AGGREGATE FACT BASE =================
        base_df = (
            bronze_df
            .groupBy(
                window(col("event_time"), "5 minutes"),
                "camera_id", "city", "district", "ward", "street",
                "rtsp_url", "risk_level"
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
            # ===== KEYS =====
            .withColumn("date_key", date_format("window_start", "yyyyMMdd").cast("int"))
            .withColumn("time_key", date_format("window_start", "yyyyMMddHHmm").cast("long"))
            .withColumn(
                "location_key",
                sha2(concat_ws("|", "city", "district", "ward", "street"), 256)
            )
            .withColumn("camera_key", col("camera_id"))
            .withColumn(
                "fact_id",
                sha2(concat_ws("-", "camera_id", "window_start"), 256)
            )
            .cache()
        )

        # ================= DIM TIME =================
        print("▶ Updating dim_time")
        new_time = (
            base_df
            .select(
                "time_key",
                "date_key",
                hour("window_start").alias("hour"),
                minute("window_start").alias("minute")
            )
            .distinct()
        )

        if spark.catalog.tableExists(DIM_TIME_TBL):
            existing = spark.read.format("iceberg").load(DIM_TIME_TBL)
            new_time = new_time.join(existing, "time_key", "left_anti")

        new_time.write.format("iceberg").mode("append").saveAsTable(DIM_TIME_TBL)

        # ================= DIM LOCATION =================
        print(" Updating dim_location")
        new_loc = (
            base_df
            .select(
                "location_key", "city", "district", "ward",
                "street", "latitude", "longitude"
            )
            .distinct()
        )

        if spark.catalog.tableExists(DIM_LOC_TBL):
            existing = spark.read.format("iceberg").load(DIM_LOC_TBL)
            new_loc = new_loc.join(existing, "location_key", "left_anti")

        new_loc.write.format("iceberg").mode("append").saveAsTable(DIM_LOC_TBL)

        # ================= DIM CAMERA =================
        print(" Updating dim_camera")
        new_cam = (
            base_df
            .select(
                "camera_key", "camera_id", "rtsp_url", "risk_level"
            )
            .distinct()
        )

        if spark.catalog.tableExists(DIM_CAM_TBL):
            existing = spark.read.format("iceberg").load(DIM_CAM_TBL)
            new_cam = new_cam.join(existing, "camera_key", "left_anti")

        new_cam.write.format("iceberg").mode("append").saveAsTable(DIM_CAM_TBL)

        # ================= FACT =================
        print(" Writing fact_camera_monitoring")
        (
            base_df
            .select(
                "fact_id", "date_key", "time_key",
                "location_key", "camera_key", "window_start",
                "avg_latency_ms", "avg_fps",
                "is_violent_window", "max_risk_score",
                "alert_count", "evidence_url"
            )
            .write.format("iceberg")
            .mode("append")
            .saveAsTable(FACT_TBL)
        )

        # ================= RAG TABLE =================
        print("▶ Writing violence_events_for_rag")
        (
            base_df
            .filter(col("is_violent_window") == True)
            .select(
                col("is_violent_window"),
                col("fact_id").alias("event_id"),
                col("window_start").alias("timestamp_utc"),
                "camera_id", "city", "district", "ward", "street",
                col("max_risk_score").alias("score")
            )
            .write.format("iceberg")
            .mode("append")
            .saveAsTable(RAG_TBL)
        )

        print("GOLD BUILD SUCCESS")
        base_df.unpersist()

    except Exception as e:
        print("ERROR:", e)
        raise
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    run_gold_batch()
