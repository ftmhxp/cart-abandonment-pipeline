from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, to_date

GCS_BRONZE     = "gs://cart-pipeline-lake/bronze/cart_events"
KAFKA_BROKERS  = "kafka:9092"
KAFKA_TOPIC    = "cart-events"
CHECKPOINT_DIR = "/tmp/checkpoints/bronze"
IVY_DIR        = "/tmp/.ivy"

spark = (
    SparkSession.builder
    .appName("cart_bronze")
    .config("spark.jars.ivy", IVY_DIR)
    # Delta Lake
    .config("spark.sql.extensions",            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # GCS connector
    .config("spark.hadoop.fs.gs.impl",                        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",     "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/credentials/gcp-key.json")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ── Read from Kafka ────────────────────────────────────────────────────────────
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

# ── Transform ──────────────────────────────────────────────────────────────────
bronze = (
    raw
    .select(
        col("value").cast("string").alias("raw_event"),
        col("offset"),
        col("partition"),
        col("timestamp").alias("kafka_timestamp"),
    )
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("pipeline_layer", lit("bronze"))
    .withColumn("ingestion_date", to_date(col("ingestion_timestamp")))
)

# ── Write to GCS as Delta ──────────────────────────────────────────────────────
query = (
    bronze.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_DIR)
    .option("path", GCS_BRONZE)
    .partitionBy("ingestion_date")
    .trigger(processingTime="30 seconds")
    .start()
)

print(f"[bronze] Streaming to {GCS_BRONZE} — waiting for data...")
query.awaitTermination()
