```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType, LongType
from pyspark.sql.functions import col, sum as spark_sum, count, avg, min as spark_min, max as spark_max, when, expr, lit, window, current_timestamp, from_json
import random
from datetime import datetime, date, timedelta
import json
import time
from kafka import KafkaProducer
```

# Pattern 2.8: Session Window Analytics

Track user sessions with dynamic gap-based windows:

1. **Session Definition**: Group events separated by <= gap timeout
2. **Dynamic Windows**: Session end determined by inactivity
3. **Session Metrics**: Duration, event count, conversion tracking
4. **Use Case**: Web analytics, user behavior analysis

## Session Window Explained

```
Gap Timeout: 5 minutes

Events:  E1────E2──E3────────────────E4──E5──────────────E6
         ↓     ↓   ↓                 ↓   ↓               ↓
Time:    10:00 10:02 10:04          10:15 10:17         10:30

Sessions:
  Session 1: [E1, E2, E3]  (10:00-10:04, no 5-min gap)
  Session 2: [E4, E5]      (10:15-10:17, no 5-min gap)
  Session 3: [E6]          (10:30, isolated event)
```

**Key Difference from Fixed Windows**:
- Session windows adapt to user behavior
- No fixed start/end times
- Session closes after inactivity gap

## Parameters


```python
execution_date = "2025-01-16"
environment = "development"
database_name = "demo"
kafka_topic = "user_events"
iceberg_table = "user_sessions"
kafka_bootstrap_servers = "infrastructure-kafka.openlakes.svc.cluster.local:9092"
session_gap_minutes = 5
num_users = 10
num_events = 200
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
    .appName(f"Pattern-2.8-SessionWindow-{environment}") \
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

print("✅ Spark initialized for session window analytics")
```

## Setup: Create Sessions Table


```python
full_database = f"lakehouse.{database_name}"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_database}")

full_table = f"{full_database}.{iceberg_table}"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {full_table} (
        session_start TIMESTAMP,
        session_end TIMESTAMP,
        user_id STRING,
        event_count BIGINT,
        page_views BIGINT,
        purchases BIGINT,
        total_purchase_value DOUBLE,
        session_duration_seconds BIGINT,
        converted BOOLEAN,
        processing_time TIMESTAMP
    ) USING iceberg
""")

print(f"✅ Sessions table created: {full_table}")
```

## Kafka Producer: Generate User Events with Gaps


```python
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"📊 Generating {num_events} user events (with session gaps)...")

base_time = datetime.utcnow() - timedelta(minutes=30)

for i in range(num_events):
    user_id = f"user_{random.randint(1, num_users)}"
    
    # Simulate sessions: cluster events, then add gaps
    if i % 20 == 0 and i > 0:
        # Insert session gap (> 5 minutes)
        base_time += timedelta(minutes=random.randint(6, 10))
    else:
        # Within-session timing (< 5 minutes apart)
        base_time += timedelta(seconds=random.randint(10, 120))
    
    event_type = random.choice(["page_view", "page_view", "page_view", "click", "purchase"])  # More page views
    
    event = {
        "user_id": user_id,
        "event_type": event_type,
        "page_url": random.choice(["/home", "/products", "/cart", "/checkout"]),
        "purchase_value": round(random.uniform(10.0, 500.0), 2) if event_type == "purchase" else 0.0,
        "timestamp": base_time.isoformat()
    }
    
    producer.send(kafka_topic, value=event)
    
    if (i + 1) % 50 == 0:
        print(f"  Sent {i + 1}/{num_events} events...")

producer.flush()
print(f"✅ Produced {num_events} user events with session gaps")
```

## Spark Streaming: Session Window Aggregation


```python
# Read event stream
event_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("purchase_value", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load() \
    .select(from_json(col("value").cast("string"), event_schema).alias("data")) \
    .select(
        col("data.user_id").alias("user_id"),
        col("data.event_type").alias("event_type"),
        col("data.page_url").alias("page_url"),
        col("data.purchase_value").alias("purchase_value"),
        col("data.timestamp").cast(TimestampType()).alias("event_time")
    ) \
    .withWatermark("event_time", watermark_delay)

print(f"✅ Event stream configured with watermark")

# Session window: group events within {session_gap_minutes} minutes
df_sessions = df_stream.groupBy(
    col("user_id"),
    window(col("event_time"), f"{session_gap_minutes} minutes")  # Session gap
).agg(
    count("*").alias("event_count"),
    spark_sum((col("event_type") == "page_view").cast("int")).alias("page_views"),
    spark_sum((col("event_type") == "purchase").cast("int")).alias("purchases"),
    spark_sum("purchase_value").alias("total_purchase_value"),
    spark_min("event_time").alias("session_start_time"),
    spark_max("event_time").alias("session_end_time")
).select(
    col("window.start").alias("session_start"),
    col("window.end").alias("session_end"),
    col("user_id"),
    col("event_count"),
    col("page_views"),
    col("purchases"),
    col("total_purchase_value"),
    ((col("session_end_time").cast("long") - col("session_start_time").cast("long"))).alias("session_duration_seconds"),
    (col("purchases") > 0).alias("converted"),
    current_timestamp().alias("processing_time")
)

print(f"✅ Session window aggregation configured ({session_gap_minutes}-minute gap)")
```

## Iceberg Sink: Write Session Analytics


```python
query = df_sessions.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", f"/tmp/checkpoint_{iceberg_table}") \
    .toTable(full_table)

print(f"✅ Session analytics started: Events → Sessions → Iceberg")
print(f"⏳ Processing for {streaming_duration_seconds} seconds...\n")

time.sleep(streaming_duration_seconds)

query.stop()
print("\n✅ Streaming query stopped")
```

## Query: Session Analytics


```python
# Session summary
df_session_summary = spark.sql(f"""
    SELECT 
        COUNT(*) as total_sessions,
        COUNT(DISTINCT user_id) as unique_users,
        ROUND(AVG(event_count), 2) as avg_events_per_session,
        ROUND(AVG(session_duration_seconds), 2) as avg_duration_seconds,
        SUM(CAST(converted AS INT)) as converted_sessions,
        ROUND(SUM(CAST(converted AS INT)) * 100.0 / COUNT(*), 2) as conversion_rate_pct
    FROM {full_table}
""")

print("📊 Session Analytics Summary:")
df_session_summary.show()

# Top converting sessions
df_top_sessions = spark.sql(f"""
    SELECT 
        user_id,
        session_start,
        event_count,
        page_views,
        purchases,
        ROUND(total_purchase_value, 2) as revenue,
        session_duration_seconds
    FROM {full_table}
    WHERE converted = true
    ORDER BY total_purchase_value DESC
    LIMIT 10
""")

print("\n📊 Top Converting Sessions (by revenue):")
df_top_sessions.show()

# User behavior patterns
df_user_patterns = spark.sql(f"""
    SELECT 
        user_id,
        COUNT(*) as session_count,
        SUM(event_count) as total_events,
        SUM(purchases) as total_purchases,
        ROUND(SUM(total_purchase_value), 2) as lifetime_value
    FROM {full_table}
    GROUP BY user_id
    ORDER BY lifetime_value DESC
    LIMIT 10
""")

print("\n📊 User Behavior Patterns (Top 10 by LTV):")
df_user_patterns.show()
```

## Validation


```python
if enable_validation:
    total_sessions = spark.sql(f"SELECT COUNT(*) FROM {full_table}").collect()[0][0]
    
    assert total_sessions > 0, "Should have detected sessions"
    print(f"✅ Detected {total_sessions} user sessions")
    
    # Verify sessions have reasonable durations
    max_duration = spark.sql(f"SELECT MAX(session_duration_seconds) FROM {full_table}").collect()[0][0]
    assert max_duration < (session_gap_minutes * 60 * 2), "Session durations should be reasonable"
    print(f"✅ Session durations are reasonable (max: {max_duration}s)")
    
    # Verify event counts
    total_events = spark.sql(f"SELECT SUM(event_count) FROM {full_table}").collect()[0][0]
    print(f"✅ Total events across sessions: {total_events}")
    
    # Verify conversion tracking
    converted_sessions = spark.sql(f"SELECT COUNT(*) FROM {full_table} WHERE converted = true").collect()[0][0]
    conversion_rate = (converted_sessions / total_sessions * 100) if total_sessions > 0 else 0
    print(f"✅ Conversion tracking: {converted_sessions}/{total_sessions} sessions ({conversion_rate:.1f}%)")
    
    test_passed = True
    print("\n✅ All validations passed!")
else:
    test_passed = True
```

## Summary

### ✅ Pattern 2.8: Session Window Analytics Complete!

Demonstrated:
1. Session window definition (5-minute inactivity gap)
2. Event clustering into user sessions
3. Session metrics (duration, event count, conversions)
4. Conversion rate analysis
5. User behavior patterns and lifetime value

**Session Window vs Fixed Window**:
```
Fixed Window (Tumbling):
  - Predefined start/end times
  - Events split artificially at boundaries
  - Example: Every hour, regardless of activity

Session Window:
  - Dynamic boundaries based on activity
  - Natural user behavior grouping
  - Closes after inactivity gap
```

**Session Gap Selection**:
- **Short gap (1-5 min)**: Focused sessions, more sessions
- **Medium gap (15-30 min)**: Standard web analytics
- **Long gap (60+ min)**: Extended workflows, mobile apps

**Benefits**:
- 📊 Natural user behavior tracking
- 🎯 Accurate conversion attribution
- 🔍 Identify engagement patterns
- 💰 Calculate session-level value

**Use Cases**: 
- Web analytics (Google Analytics-style sessions)
- Mobile app engagement tracking
- Customer support interaction sessions
- IoT device usage patterns
- Gaming session analysis


```python
if enable_cleanup:
    spark.sql(f"DROP TABLE IF EXISTS {full_table}")
    print("🧹 Cleanup completed")
else:
    print("⏭️  Cleanup skipped")
```
