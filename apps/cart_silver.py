from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, from_json, lit
)
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType, TimestampType
)

GCS_BRONZE = "gs://cart-pipeline-lake/bronze/cart_events"
GCS_SILVER = "gs://cart-pipeline-lake/silver/cart_events"
IVY_DIR    = "/tmp/.ivy"

spark = (
    SparkSession.builder
    .appName("cart_silver")
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

# ── Schema for raw_event JSON ──────────────────────────────────────────────────
event_schema = StructType([
    StructField("user_id",      StringType(),    True),
    StructField("session_id",   StringType(),    True),
    StructField("product_id",   StringType(),    True),
    StructField("product_name", StringType(),    True),
    StructField("category",     StringType(),    True),
    StructField("price",        DoubleType(),    True),
    StructField("quantity",     IntegerType(),   True),
    StructField("event_type",   StringType(),    True),
    StructField("timestamp",    TimestampType(), True),
])

# ── Read Bronze Delta ──────────────────────────────────────────────────────────
bronze = spark.read.format("delta").load(GCS_BRONZE)
print(f"[silver] Bronze row count: {bronze.count():,}")

# ── Parse JSON + typed columns ─────────────────────────────────────────────────
parsed = (
    bronze
    .withColumn("evt", from_json(col("raw_event"), event_schema))
    .select(
        col("evt.user_id").alias("user_id"),
        col("evt.session_id").alias("session_id"),
        col("evt.product_id").alias("product_id"),
        col("evt.product_name").alias("product_name"),
        col("evt.category").alias("category"),
        col("evt.price").cast(DoubleType()).alias("price"),
        col("evt.quantity").cast(IntegerType()).alias("quantity"),
        col("evt.event_type").alias("event_type"),
        col("evt.timestamp").alias("event_timestamp"),
    )
)

# ── Drop nulls on critical fields ─────────────────────────────────────────────
after_parse  = parsed.count()
clean        = parsed.dropna(subset=["user_id", "session_id", "event_type"])
after_nulls  = clean.count()
print(f"[silver] After parse:     {after_parse:,}")
print(f"[silver] After null drop: {after_nulls:,}  (dropped {after_parse - after_nulls:,})")

# ── Deduplicate ────────────────────────────────────────────────────────────────
deduped     = clean.dropDuplicates(["user_id", "session_id", "event_type", "event_timestamp"])
after_dedup = deduped.count()
print(f"[silver] After dedup:     {after_dedup:,}  (dropped {after_nulls - after_dedup:,})")

# ── Add Silver metadata ────────────────────────────────────────────────────────
silver = (
    deduped
    .withColumn("pipeline_layer", lit("silver"))
    .withColumn("processed_at",   current_timestamp())
)

# ── Write to GCS as Delta ──────────────────────────────────────────────────────
(
    silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("event_type")
    .save(GCS_SILVER)
)

print(f"[silver] Written {after_dedup:,} rows to {GCS_SILVER}")
