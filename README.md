# Cart Abandonment Pipeline

70% of online shoppers add items to their cart and never check out. This pipeline detects abandonment in real time and triggers a personalized recovery action — a discount email, an urgency push notification, or a "having trouble?" nudge — before the shopper moves on.

## How it works

```
Producer (host)
    │  fake cart events
    ▼
Kafka (Docker)
    │  real-time stream
    ▼
Spark Bronze  →  raw events landed to GCS (Delta Lake)
Spark Silver  →  parsed, cleaned, deduplicated
Spark Gold    →  abandoned sessions detected, written to BigQuery
Recovery      →  action dispatched per cart value segment
```

## Stack

| Layer | Tool |
|---|---|
| Event generation | Python + Faker |
| Message broker | Apache Kafka (KRaft, no Zookeeper) |
| Processing | Apache Spark 3.5.1 + Delta Lake 3.2.0 |
| Storage | Google Cloud Storage |
| Warehouse | BigQuery |
| Infrastructure | Terraform |
| Orchestration | Docker Compose |

## Recovery logic

| Condition | Action |
|---|---|
| Cart value > $100 | 10% discount email |
| Cart value < $30 | Urgency push notification |
| $30–$100 | Standard recovery email |
| Checkout started, not completed | "Having trouble?" email |

## Quickstart

**1. Prerequisites**
```bash
cp credentials/gcp-key.json  # add your GCP service account key
pip3 install -r requirements.txt
```

**2. Provision GCP infrastructure**
```bash
cd infra
cp terraform.tfvars.example terraform.tfvars  # fill in your project_id
terraform init && terraform apply
cd ..
```

**3. Start the stack**
```bash
docker compose up -d
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --topic cart-events --partitions 3 --replication-factor 1
```

**4. Run the pipeline**
```bash
# Terminal 1 — produce events
python3 producer/cart_producer.py

# Terminal 2 — stream to Bronze (leave running ~10 min)
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.19,com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1 \
  --conf spark.jars.ivy=/tmp/.ivy \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
  --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=/opt/credentials/gcp-key.json \
  /opt/spark-apps/cart_bronze.py

# Terminal 2 — after stopping Bronze, run Silver then Gold
docker exec spark-master /opt/spark/bin/spark-submit ... /opt/spark-apps/cart_silver.py
docker exec spark-master /opt/spark/bin/spark-submit ... /opt/spark-apps/cart_gold.py

# Dispatch recovery actions
python3 apps/cart_recovery.py
```

> Full commands with all flags: see [commands.txt](commands.txt)

## Results

```
[gold]   Total sessions:    4,819
[gold]   Abandoned:         4,608
[gold]   Converted:           211
[gold]   Abandonment rate:  95.62%

[recovery] Marked 4,608 rows as recovery_sent=true in BigQuery
[recovery]   Discount emails:      3,891
[recovery]   Urgency push notifs:    717
```

## GCS layout

```
gs://cart-pipeline-lake/
├── bronze/cart_events/   ← raw Kafka events (Delta, partitioned by date)
├── silver/cart_events/   ← cleaned and typed (Delta, partitioned by event_type)
└── gold/
    ├── abandoned_carts/  ← one row per abandoned session
    └── hourly_stats/     ← abandonment rate and revenue lost by hour
```
