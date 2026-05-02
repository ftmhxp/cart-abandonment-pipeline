from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, hour, lit, max as spark_max,
    min as spark_min, round as spark_round, sum as spark_sum,
    when
)

import os

GCS_SILVER         = "gs://cart-pipeline-lake/silver/cart_events"
GCS_GOLD_ABANDONED = "gs://cart-pipeline-lake/gold/abandoned_carts"
GCS_GOLD_HOURLY    = "gs://cart-pipeline-lake/gold/hourly_stats"
BQ_DATASET         = "cart_pipeline"
BQ_TEMP_BUCKET     = "cart-pipeline-lake"
BQ_PROJECT         = "project-16d9dcf7-d9a8-4c74-b35"
IVY_DIR            = "/tmp/.ivy"

ABANDONMENT_WINDOW_MINUTES = int(os.getenv("ABANDONMENT_WINDOW_MINUTES", "0"))

spark = (
    SparkSession.builder
    .appName("cart_gold")
    .config("spark.jars.ivy", IVY_DIR)
    # Delta Lake
    .config("spark.sql.extensions",            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # GCS connector
    .config("spark.hadoop.fs.gs.impl",                                     "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",                  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/credentials/gcp-key.json")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ── Read Silver Delta ──────────────────────────────────────────────────────────
silver = spark.read.format("delta").load(GCS_SILVER)
silver.cache()
print(f"[gold] Silver row count: {silver.count():,}")

# ══════════════════════════════════════════════════════════════════════════════
# TABLE 1 — abandoned_carts
# Sessions with add_to_cart but NO checkout_complete within window
# ══════════════════════════════════════════════════════════════════════════════

# Per-session aggregates
session_agg = (
    silver.groupBy("session_id", "user_id")
    .agg(
        spark_sum(col("price") * col("quantity")).alias("cart_value"),
        count("*").alias("item_count"),
        spark_min("event_timestamp").alias("first_add_time"),
        spark_max("event_timestamp").alias("last_event_time"),
        # flags
        spark_sum(when(col("event_type") == "add_to_cart",       lit(1)).otherwise(lit(0))).alias("add_count"),
        spark_sum(when(col("event_type") == "checkout_complete", lit(1)).otherwise(lit(0))).alias("checkout_count"),
    )
)

# Abandoned = has add_to_cart AND no checkout_complete
#             AND inactive for >= ABANDONMENT_WINDOW_MINUTES (skipped when 0)
time_filter = (
    (col("last_event_time").cast("long") - col("first_add_time").cast("long")) / 60
    >= ABANDONMENT_WINDOW_MINUTES
) if ABANDONMENT_WINDOW_MINUTES > 0 else lit(True)

abandoned_carts = (
    session_agg
    .filter(
        (col("add_count") > 0) &
        (col("checkout_count") == 0) &
        time_filter
    )
    .select(
        col("session_id"),
        col("user_id"),
        spark_round(col("cart_value"), 2).alias("cart_value"),
        col("item_count"),
        col("first_add_time"),
        col("last_event_time"),
        spark_round(
            (col("last_event_time").cast("long") - col("first_add_time").cast("long")) / 60, 1
        ).alias("minutes_inactive"),
        lit(False).alias("recovery_sent"),
        col("last_event_time").alias("abandoned_at"),
    )
)

# Recovery logic (applied as a comment per spec — column recovery_sent stays false
# until a downstream system updates it):
#   cart_value > 100 + returning user  → 10% discount email
#   cart_value < 30  + new user        → urgency push notification
#   checkout_start but no complete     → "having trouble?" email
#   email opened + not clicked         → switch to push notification

total_abandoned  = abandoned_carts.count()
print(f"[gold] Abandoned carts:  {total_abandoned:,}")

# ── Write abandoned_carts → GCS ────────────────────────────────────────────────
(
    abandoned_carts.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(GCS_GOLD_ABANDONED)
)

# ── Write abandoned_carts → BigQuery ──────────────────────────────────────────
(
    abandoned_carts.write
    .format("bigquery")
    .option("table",              f"{BQ_DATASET}.abandoned_carts")
    .option("temporaryGcsBucket", BQ_TEMP_BUCKET)
    .option("parentProject",      BQ_PROJECT)
    .mode("overwrite")
    .save()
)

print(f"[gold] abandoned_carts written to GCS + BigQuery")

# ══════════════════════════════════════════════════════════════════════════════
# TABLE 2 — hourly_stats
# ══════════════════════════════════════════════════════════════════════════════

# Total sessions per hour
hourly_sessions = (
    silver
    .withColumn("hour", hour("event_timestamp"))
    .groupBy("hour")
    .agg(countDistinct("session_id").alias("total_sessions"))
)

# Abandoned sessions per hour (reuse session_agg logic at hour grain)
silver_with_hour = silver.withColumn("hour", hour("event_timestamp"))

abandoned_per_hour = (
    silver_with_hour
    .groupBy("session_id", "hour")
    .agg(
        spark_sum(when(col("event_type") == "add_to_cart",       lit(1)).otherwise(lit(0))).alias("add_count"),
        spark_sum(when(col("event_type") == "checkout_complete", lit(1)).otherwise(lit(0))).alias("checkout_count"),
        spark_sum(col("price") * col("quantity")).alias("cart_value"),
    )
    .filter((col("add_count") > 0) & (col("checkout_count") == 0))
    .groupBy("hour")
    .agg(
        count("session_id").alias("abandoned_sessions"),
        spark_sum("cart_value").alias("revenue_lost"),
    )
)

converted_per_hour = (
    silver_with_hour
    .groupBy("session_id", "hour")
    .agg(
        spark_sum(when(col("event_type") == "checkout_complete", lit(1)).otherwise(lit(0))).alias("checkout_count"),
    )
    .filter(col("checkout_count") > 0)
    .groupBy("hour")
    .agg(count("session_id").alias("converted_sessions"))
)

hourly_stats = (
    hourly_sessions
    .join(abandoned_per_hour, on="hour", how="left")
    .join(converted_per_hour, on="hour", how="left")
    .fillna(0, subset=["abandoned_sessions", "converted_sessions", "revenue_lost"])
    .select(
        col("hour"),
        col("total_sessions"),
        col("abandoned_sessions"),
        col("converted_sessions"),
        spark_round(
            col("abandoned_sessions") / col("total_sessions") * 100, 2
        ).alias("abandonment_rate"),
        spark_round(col("revenue_lost"), 2).alias("revenue_lost"),
    )
    .orderBy("hour")
)

# ── Write hourly_stats → GCS ───────────────────────────────────────────────────
(
    hourly_stats.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(GCS_GOLD_HOURLY)
)

# ── Write hourly_stats → BigQuery ─────────────────────────────────────────────
(
    hourly_stats.write
    .format("bigquery")
    .option("table",              f"{BQ_DATASET}.hourly_stats")
    .option("temporaryGcsBucket", BQ_TEMP_BUCKET)
    .option("parentProject",      BQ_PROJECT)
    .mode("overwrite")
    .save()
)

print(f"[gold] hourly_stats written to GCS + BigQuery")

# ── Summary ────────────────────────────────────────────────────────────────────
total_sessions  = silver.select("session_id").distinct().count()
total_converted = total_sessions - total_abandoned
abandonment_pct = round(total_abandoned / total_sessions * 100, 2) if total_sessions else 0

print(f"\n[gold] ── Pipeline Summary ─────────────────────────")
print(f"[gold]   Total sessions:    {total_sessions:,}")
print(f"[gold]   Abandoned:         {total_abandoned:,}")
print(f"[gold]   Converted:         {total_converted:,}")
print(f"[gold]   Abandonment rate:  {abandonment_pct}%")
print(f"[gold] ────────────────────────────────────────────────")
