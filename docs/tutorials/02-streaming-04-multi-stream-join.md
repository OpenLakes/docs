```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType, LongType
from pyspark.sql.functions import col, sum as spark_sum, count, avg, when, expr, lit, window, current_timestamp, from_json
import random
from datetime import datetime, date, timedelta
import json
import time
from kafka import KafkaProducer
```

# Pattern 2.4: Multi-Topic Stream Join

Join multiple streaming data sources with watermarks:

1. **3 Kafka Topics**: User actions, purchases, sessions
2. **Spark Streaming**: Join with time windows and watermarks
3. **Unified Activity Stream**: Combined view of user behavior
4. **Iceberg**: Store enriched activity data

## Architecture

```
Kafka (user_actions) ────┐
                          ├──→ Spark Stream Join (with watermarks)
Kafka (purchases) ────────┤        ↓
                          │    Iceberg (unified_activity)
Kafka (sessions) ─────────┘
```

**Challenge**: Join streams with different arrival times
**Solution**: Watermarks handle late-arriving data

## Parameters


```python
execution_date = "2025-01-16"
environment = "development"
database_name = "demo"
topic_actions = "user_actions"
topic_purchases = "purchases"
topic_sessions = "sessions"
iceberg_table = "unified_activity"
kafka_bootstrap_servers = "infrastructure-kafka.openlakes.svc.cluster.local:9092"
num_events_per_topic = 50
watermark_delay = "30 seconds"
streaming_duration_seconds = 40
enable_validation = True
enable_cleanup = False
```


```python
import os

from pyspark.sql import SparkSession
# Set AWS environment variables for S3FileIO
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "admin123"

spark = SparkSession.builder \
    .appName(f"Pattern-2.4-MultiStreamJoin-{environment}") \
    .config("spark.jars.packages", 
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.8.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0-preview3") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config("spark.sql.catalog.lakehouse.uri", "http://infrastructure-nessie:19120/api/v2") \
    .config("spark.sql.catalog.lakehouse.ref", "main") \
    .config("spark.sql.catalog.lakehouse.authentication.type", "NONE") \
    .config("spark.sql.catalog.lakehouse.warehouse", "s3a://openlakes/warehouse/") \
    .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://infrastructure-minio:9000") \
    .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://infrastructure-minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("✅ Spark initialized for multi-stream join")
```

## Setup: Create Iceberg Table


```python
full_database = f"lakehouse.{database_name}"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_database}")

full_table = f"{full_database}.{iceberg_table}"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {full_table} (
        user_id STRING,
        session_id STRING,
        action_type STRING,
        page_url STRING,
        purchase_amount DOUBLE,
        product_id STRING,
        device_type STRING,
        event_timestamp TIMESTAMP,
        processing_time TIMESTAMP
    ) USING iceberg
""")

print(f"✅ Iceberg table created: {full_table}")
```

## Kafka Producer: Generate 3 Streams


```python
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("📊 Generating correlated events for 3 Kafka topics...")

users = [f"user_{i}" for i in range(1, 11)]
sessions = [f"session_{i}" for i in range(1, 21)]

# Generate correlated events to ensure joins work
# Create a list of (user, session) pairs that will be reused across all 3 topics
event_pairs = []
for i in range(num_events_per_topic):
    event_pairs.append({
        "user_id": random.choice(users),
        "session_id": random.choice(sessions),
        "timestamp_offset": random.randint(0, 60)
    })

# Stream 1: User Actions (all event pairs get an action)
print(f"  Topic 1: {topic_actions} ({num_events_per_topic} events)")
for pair in event_pairs:
    event = {
        "user_id": pair["user_id"],
        "session_id": pair["session_id"],
        "action": random.choice(["view", "click", "scroll", "search"]),
        "page_url": random.choice(["/home", "/products", "/category", "/search"]),
        "timestamp": (datetime.utcnow() - timedelta(seconds=pair["timestamp_offset"])).isoformat()
    }
    producer.send(topic_actions, value=event)

# Stream 2: Purchases (only 60% of event pairs make a purchase)
print(f"  Topic 2: {topic_purchases} ({int(num_events_per_topic * 0.6)} events)")
for pair in random.sample(event_pairs, int(num_events_per_topic * 0.6)):
    event = {
        "user_id": pair["user_id"],
        "session_id": pair["session_id"],
        "product_id": f"prod_{random.randint(100, 999)}",
        "amount": round(random.uniform(19.99, 499.99), 2),
        "timestamp": (datetime.utcnow() - timedelta(seconds=pair["timestamp_offset"])).isoformat()
    }
    producer.send(topic_purchases, value=event)

# Stream 3: Sessions (all unique sessions get a session record)
print(f"  Topic 3: {topic_sessions} ({len(set(p['session_id'] for p in event_pairs))} unique sessions)")
# Get unique session_id + user_id combinations
unique_sessions = {}
for pair in event_pairs:
    key = (pair["session_id"], pair["user_id"])
    if key not in unique_sessions:
        unique_sessions[key] = pair["timestamp_offset"]

for (session_id, user_id), timestamp_offset in unique_sessions.items():
    event = {
        "session_id": session_id,
        "user_id": user_id,
        "device": random.choice(["mobile", "desktop", "tablet"]),
        "timestamp": (datetime.utcnow() - timedelta(seconds=timestamp_offset)).isoformat()
    }
    producer.send(topic_sessions, value=event)

producer.flush()
print(f"✅ Produced correlated events across 3 topics")
print(f"   - {num_events_per_topic} actions")
print(f"   - {int(num_events_per_topic * 0.6)} purchases (60% conversion)")
print(f"   - {len(unique_sessions)} unique sessions")
```

## Spark Streaming: Read 3 Streams


```python
# Stream 1: User Actions
actions_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("action", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("timestamp", StringType(), True)
])

df_actions = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", topic_actions) \
    .option("startingOffsets", "earliest") \
    .load() \
    .select(from_json(col("value").cast("string"), actions_schema).alias("data")) \
    .select(
        col("data.user_id").alias("user_id"),
        col("data.session_id").alias("session_id"),
        col("data.action").alias("action_type"),
        col("data.page_url").alias("page_url"),
        col("data.timestamp").cast(TimestampType()).alias("timestamp")
    ) \
    .withWatermark("timestamp", watermark_delay)

print("✅ Stream 1 (actions) configured with watermark")
```


```python
# Stream 2: Purchases
purchases_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

df_purchases = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", topic_purchases) \
    .option("startingOffsets", "earliest") \
    .load() \
    .select(from_json(col("value").cast("string"), purchases_schema).alias("data")) \
    .select(
        col("data.user_id").alias("user_id"),
        col("data.session_id").alias("session_id"),
        col("data.product_id").alias("product_id"),
        col("data.amount").alias("purchase_amount"),
        col("data.timestamp").cast(TimestampType()).alias("timestamp")
    ) \
    .withWatermark("timestamp", watermark_delay)

print("✅ Stream 2 (purchases) configured with watermark")
```


```python
# Stream 3: Sessions
sessions_schema = StructType([
    StructField("session_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("device", StringType(), True),
    StructField("timestamp", StringType(), True)
])

df_sessions = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", topic_sessions) \
    .option("startingOffsets", "earliest") \
    .load() \
    .select(from_json(col("value").cast("string"), sessions_schema).alias("data")) \
    .select(
        col("data.session_id").alias("session_id"),
        col("data.user_id").alias("user_id"),
        col("data.device").alias("device_type"),
        col("data.timestamp").cast(TimestampType()).alias("timestamp")
    ) \
    .withWatermark("timestamp", watermark_delay)

print("✅ Stream 3 (sessions) configured with watermark")
```

## Stream Join: Combine 3 Streams


```python
# Join actions with purchases (left join - not all actions have purchases)
df_joined_1 = df_actions.join(
    df_purchases,
    (df_actions.user_id == df_purchases.user_id) & 
    (df_actions.session_id == df_purchases.session_id),
    how="left"
).select(
    df_actions.user_id,
    df_actions.session_id,
    df_actions.action_type,
    df_actions.page_url,
    df_purchases.purchase_amount,
    df_purchases.product_id,
    df_actions.timestamp
)

# Join with sessions (inner join - enrich with device info)
df_unified = df_joined_1.join(
    df_sessions,
    (df_joined_1.user_id == df_sessions.user_id) & 
    (df_joined_1.session_id == df_sessions.session_id),
    how="inner"
).select(
    df_joined_1.user_id,
    df_joined_1.session_id,
    df_joined_1.action_type,
    df_joined_1.page_url,
    df_joined_1.purchase_amount,
    df_joined_1.product_id,
    df_sessions.device_type,
    df_joined_1.timestamp.alias("event_timestamp"),
    current_timestamp().alias("processing_time")
)

print("✅ 3-way stream join configured (actions ⟕ purchases ⟗ sessions)")
```

## Iceberg Sink: Write Unified Activity Stream


```python
query = df_unified.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", f"/tmp/checkpoint_{iceberg_table}") \
    .toTable(full_table)

print(f"✅ Multi-stream join started: 3 topics → Unified Activity → Iceberg")
print(f"⏳ Processing for {streaming_duration_seconds} seconds...\n")

time.sleep(streaming_duration_seconds)

query.stop()
print("\n✅ Streaming query stopped")
```

## Query: Unified Activity Analytics


```python
# Activity summary by device
df_device_summary = spark.sql(f"""
    SELECT 
        device_type,
        COUNT(*) as total_actions,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(purchase_amount) as purchase_count,
        COALESCE(SUM(purchase_amount), 0) as total_revenue
    FROM {full_table}
    GROUP BY device_type
    ORDER BY total_revenue DESC
""")

print("📊 Activity Summary by Device:")
df_device_summary.show()

# Purchase conversion by action type
df_conversion = spark.sql(f"""
    SELECT 
        action_type,
        COUNT(*) as actions,
        COUNT(purchase_amount) as purchases,
        ROUND(COUNT(purchase_amount) * 100.0 / COUNT(*), 2) as conversion_rate
    FROM {full_table}
    GROUP BY action_type
    ORDER BY conversion_rate DESC
""")

print("\n📊 Conversion Rate by Action Type:")
df_conversion.show()
```

## Validation


```python
if enable_validation:
    total_count = spark.sql(f"SELECT COUNT(*) FROM {full_table}").collect()[0][0]
    
    assert total_count > 0, "Should have joined some events"
    print(f"✅ Joined {total_count} unified activity records")
    
    # Verify all records have device_type (from sessions join)
    null_device_count = spark.sql(f"SELECT COUNT(*) FROM {full_table} WHERE device_type IS NULL").collect()[0][0]
    assert null_device_count == 0, "All records should have device_type (inner join)"
    print(f"✅ All records enriched with device info")
    
    # Verify some purchases exist (left join allows nulls)
    purchase_count = spark.sql(f"SELECT COUNT(*) FROM {full_table} WHERE purchase_amount IS NOT NULL").collect()[0][0]
    print(f"✅ Found {purchase_count} events with purchase data")
    
    test_passed = True
    print("\n✅ All validations passed!")
else:
    test_passed = True
```

## Summary

### ✅ Pattern 2.4: Multi-Stream Join Complete!

Demonstrated:
1. 3 Kafka topics (actions, purchases, sessions)
2. Watermarks for handling late-arriving data
3. Multi-way stream joins (left + inner)
4. Unified activity stream combining all data sources
5. Real-time conversion analytics

**Watermarks Explained**:
- Allow late data up to watermark delay (30 seconds)
- Events older than watermark are dropped
- Balances completeness vs latency

**Join Types**:
- Actions ⟕ Purchases: LEFT (not all actions have purchases)
- Result ⟗ Sessions: INNER (require device info)

**Use Cases**: Customer 360 view, user journey analytics, conversion tracking, attribution modeling


```python
if enable_cleanup:
    spark.sql(f"DROP TABLE IF EXISTS {full_table}")
    print("🧹 Cleanup completed")
else:
    print("⏭️  Cleanup skipped")
```
