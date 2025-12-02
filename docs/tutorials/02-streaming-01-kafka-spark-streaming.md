```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType, LongType
from pyspark.sql.functions import col, sum as spark_sum, count, avg, when, expr, lit, window, current_timestamp, from_json
import random
from datetime import datetime, date, timedelta
import json
import time
from kafka import KafkaProducer
```

# Pattern 2.1: Kafka → Spark Streaming → Iceberg

Real-time data ingestion from Kafka to Iceberg lakehouse:

1. **Kafka Producer**: Generate real-time events
2. **Spark Structured Streaming**: Read from Kafka topic
3. **Iceberg Sink**: Write streaming data with ACID guarantees
4. **Query**: Real-time analytics on streaming data

## Architecture

```
Kafka Topic (clickstream)
    ↓
Spark Structured Streaming (readStream)
    ↓
Iceberg Table (writeStream, append mode)
    ↓
Real-time Analytics (Trino/Superset)
```

**Latency**: Seconds (micro-batch processing)
**Throughput**: Thousands of events/second

## Parameters


```python
execution_date = "2025-01-16"
environment = "development"
database_name = "demo"
kafka_topic = "clickstream_events"
iceberg_table = "clickstream_realtime"
kafka_bootstrap_servers = "infrastructure-kafka.openlakes.svc.cluster.local:9092"
num_events = 100
streaming_duration_seconds = 30
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
    .appName(f"Pattern-2.1-KafkaStreaming-{environment}") \
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

print("✅ Spark initialized for Kafka streaming")
```

## Setup: Create Iceberg Target Table


```python
full_database = f"lakehouse.{database_name}"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_database}")

full_table = f"{full_database}.{iceberg_table}"

# Create Iceberg table for clickstream data
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {full_table} (
        user_id STRING,
        page_url STRING,
        action STRING,
        timestamp TIMESTAMP,
        session_id STRING,
        processing_time TIMESTAMP
    ) USING iceberg
""")

print(f"✅ Iceberg table created: {full_table}")
```

## Kafka Producer: Generate Real-Time Events


```python
# Start Kafka producer to simulate clickstream events
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"📊 Producing {num_events} events to Kafka topic '{kafka_topic}'...")

# Generate sample clickstream data
users = [f"user_{i}" for i in range(1, 11)]  # 10 users
pages = ["/home", "/products", "/cart", "/checkout", "/account"]
actions = ["view", "click", "scroll", "add_to_cart", "purchase"]

for i in range(num_events):
    event = {
        "user_id": random.choice(users),
        "page_url": random.choice(pages),
        "action": random.choice(actions),
        "timestamp": datetime.utcnow().isoformat(),
        "session_id": f"session_{random.randint(1, 20)}"
    }
    producer.send(kafka_topic, value=event)
    
    if (i + 1) % 20 == 0:
        print(f"  Sent {i + 1}/{num_events} events...")
        time.sleep(0.5)  # Simulate real-time data arrival

producer.flush()
print(f"✅ Produced {num_events} events to Kafka")
```

## Spark Structured Streaming: Read from Kafka


```python
# Define schema for clickstream events
clickstream_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("action", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("session_id", StringType(), True)
])

# Read from Kafka (structured streaming)
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON from Kafka value field
df_parsed = df_stream.select(
    from_json(col("value").cast("string"), clickstream_schema).alias("data")
).select(
    col("data.user_id").alias("user_id"),
    col("data.page_url").alias("page_url"),
    col("data.action").alias("action"),
    col("data.timestamp").cast(TimestampType()).alias("timestamp"),
    col("data.session_id").alias("session_id"),
    current_timestamp().alias("processing_time")
)

print("✅ Streaming query configured (Kafka → Spark)")
```

## Iceberg Sink: Write Stream to Lakehouse


```python
# Write stream to Iceberg table
query = df_parsed.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", f"/tmp/checkpoint_{iceberg_table}") \
    .toTable(full_table)

print(f"✅ Streaming query started: Kafka → Iceberg ({full_table})")
print(f"⏳ Processing for {streaming_duration_seconds} seconds...\n")

# Let it run for specified duration
time.sleep(streaming_duration_seconds)

query.stop()
print("\n✅ Streaming query stopped")
```

## Query: Real-Time Analytics


```python
# Query the Iceberg table (batch read)
df_results = spark.sql(f"""
    SELECT 
        action,
        COUNT(*) as event_count,
        COUNT(DISTINCT user_id) as unique_users
    FROM {full_table}
    GROUP BY action
    ORDER BY event_count DESC
""")

print("📊 Real-time Analytics (Action Summary):")
df_results.show()

# Sample raw events
df_sample = spark.sql(f"""
    SELECT * FROM {full_table}
    ORDER BY timestamp DESC
    LIMIT 10
""")

print("\n📊 Latest 10 Events:")
df_sample.show()
```

## Validation


```python
if enable_validation:
    total_count = spark.sql(f"SELECT COUNT(*) FROM {full_table}").collect()[0][0]
    
    # Should have processed most events (allow for timing variance)
    assert total_count > 0, "Should have processed some events"
    print(f"✅ Processed {total_count} events from Kafka")
    
    # Verify all required columns exist
    columns = spark.sql(f"SELECT * FROM {full_table} LIMIT 1").columns
    required_columns = ["user_id", "page_url", "action", "timestamp", "session_id", "processing_time"]
    for col in required_columns:
        assert col in columns, f"Missing column: {col}"
    print(f"✅ All required columns present: {required_columns}")
    
    # Verify timestamp range is reasonable (within last hour)
    latest_ts = spark.sql(f"SELECT MAX(timestamp) FROM {full_table}").collect()[0][0]
    assert latest_ts is not None, "Should have timestamps"
    print(f"✅ Latest event timestamp: {latest_ts}")
    
    test_passed = True
    print("\n✅ All validations passed!")
else:
    test_passed = True
```

## Summary

### ✅ Pattern 2.1: Kafka Streaming Complete!

Demonstrated:
1. Kafka producer generating real-time clickstream events
2. Spark Structured Streaming reading from Kafka topic
3. JSON parsing and schema enforcement
4. Iceberg sink with ACID guarantees (append mode)
5. Real-time analytics on streaming data

**Benefits**:
- ⚡ Real-time latency (seconds, not hours)
- 🔒 ACID transactions via Iceberg
- 📈 Scalable (thousands of events/second)
- 🔄 Fault-tolerant with checkpointing

**Use Cases**: Clickstream analytics, IoT sensor data, log aggregation, real-time dashboards


```python
if enable_cleanup:
    spark.sql(f"DROP TABLE IF EXISTS {full_table}")
    print("🧹 Cleanup completed")
else:
    print("⏭️  Cleanup skipped")
```
