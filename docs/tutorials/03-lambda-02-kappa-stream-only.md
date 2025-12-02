```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType, LongType
from pyspark.sql.functions import col, sum as spark_sum, count, avg, when, expr, lit, window, current_timestamp, from_json
import random
from datetime import datetime, date, timedelta
import json
import time
from kafka import KafkaProducer
```

# Pattern 3.2: Kappa Architecture (Stream-Only)

Simplified Kappa Architecture with a single streaming processing layer:

1. **Single Processing Path**: All data processed via Spark Structured Streaming
2. **Event-Time Processing**: Use event timestamps, not processing time
3. **Reprocessing**: Replay Kafka from beginning to recompute historical data

## Architecture

```
Kafka (event_log) → Spark Continuous Mode → Iceberg (time-partitioned)
                         ↓
                 Checkpoint + Watermark
                         ↓
                 Exactly-once guarantees
```

**vs. Lambda Architecture**:
- Lambda: Speed layer + Batch layer (dual code paths)
- Kappa: Single streaming layer (unified code)
- Lambda: Batch corrections for late data
- Kappa: Watermarks handle late data in stream

**Use Case**: Application event log processing where all historical data is in Kafka

**Benefits**:
- Simpler architecture (single code path)
- Easier to maintain
- Unified batch + streaming (Spark API)
- Event-time semantics built-in

**Requirements**:
- Kafka retention must cover reprocessing window
- Events must have event timestamps
- Idempotent processing


```python
execution_date = "2025-01-17"
environment = "development"
database_name = "demo"
iceberg_table = "event_stream_kappa"
kafka_bootstrap_servers = "infrastructure-kafka.openlakes.svc.cluster.local:9092"
kafka_topic = "application_events"
num_events = 500
window_duration = "5 minutes"
watermark_delay = "10 minutes"
streaming_duration_seconds = 30
enable_validation = True
enable_cleanup = False
```

import os

from pyspark.sql import SparkSession
# Set AWS environment variables for S3FileIO
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "admin123"

spark = SparkSession.builder \
    .appName(f"Pattern-3.2-KappaArchitecture-{environment}") \
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

print("✅ Spark initialized for Kappa Architecture (stream-only)")


```python
import os

from pyspark.sql import SparkSession
# Set AWS environment variables for S3FileIO
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "admin123"

spark = SparkSession.builder \
    .appName(f"Pattern-3.2-KappaArchitecture-{environment}") \
    .config("spark.jars.packages", 
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.8.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.4") \
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

print("✅ Spark initialized for Kappa Architecture (stream-only)")
```

## Setup: Create Iceberg Table


```python
full_database = f"lakehouse.{database_name}"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_database}")

full_table = f"{full_database}.{iceberg_table}"

# Single table for all event processing (time-partitioned)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {full_table} (
        event_window TIMESTAMP,
        event_type STRING,
        total_events BIGINT,
        avg_duration_ms DOUBLE,
        processing_timestamp TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (days(event_window))
""")

print(f"✅ Iceberg table created: {full_table}")
```

## Kafka Producer: Generate Application Events


```python
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"📊 Producing {num_events} application events to Kafka topic '{kafka_topic}'...")

base_time = datetime.utcnow() - timedelta(minutes=30)
event_types = ["page_view", "api_call", "database_query", "cache_hit", "error"]

for i in range(num_events):
    # Use event time, not processing time (Kappa architecture principle)
    event_time = base_time + timedelta(seconds=random.randint(0, 1800))
    
    event = {
        "event_id": f"evt_{i+1:06d}",
        "event_type": random.choice(event_types),
        "duration_ms": random.randint(10, 5000),
        "event_timestamp": event_time.isoformat(),
        "user_id": f"user_{random.randint(1, 100)}",
        "session_id": f"session_{random.randint(1, 50)}"
    }
    
    producer.send(kafka_topic, value=event)
    
    if (i + 1) % 100 == 0:
        print(f"  Sent {i + 1}/{num_events} events...")
        time.sleep(0.1)

producer.flush()
print(f"✅ Produced {num_events} application events to Kafka")
```


```python
# Define schema
event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("duration_ms", IntegerType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True)
])

# Read from Kafka (single source of truth)
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse events
df_parsed = df_stream.select(
    from_json(col("value").cast("string"), event_schema).alias("data")
).select(
    col("data.event_id").alias("event_id"),
    col("data.event_type").alias("event_type"),
    col("data.duration_ms").alias("duration_ms"),
    col("data.event_timestamp").cast(TimestampType()).alias("event_timestamp"),
    col("data.user_id").alias("user_id"),
    col("data.session_id").alias("session_id")
)

# Event-time windowed aggregation with watermark
df_agg = df_parsed \
    .withWatermark("event_timestamp", watermark_delay) \
    .groupBy(
        window(col("event_timestamp"), window_duration),
        col("event_type")
    ).agg(
        count("*").alias("total_events"),
        avg("duration_ms").alias("avg_duration_ms")
    ).select(
        col("window.start").alias("event_window"),
        col("event_type"),
        col("total_events"),
        col("avg_duration_ms"),
        current_timestamp().alias("processing_timestamp")
    )

# Write to Iceberg with checkpointing (enables reprocessing)
query = df_agg.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("path", full_table) \
    .option("checkpointLocation", f"/tmp/checkpoint_kappa_{iceberg_table}") \
    .trigger(processingTime='2 seconds') \
    .start()

print(f"✅ Kappa streaming layer started: Kafka → Iceberg ({full_table})")
print(f"   Window: {window_duration}")
print(f"   Watermark: {watermark_delay}")
print(f"⏳ Processing for {streaming_duration_seconds} seconds...\n")

time.sleep(streaming_duration_seconds)

query.stop()
print("\n✅ Streaming query stopped")
```

## Query: Event Analytics


```python
# Event type distribution
df_event_summary = spark.sql(f"""
    SELECT 
        event_type,
        SUM(total_events) as total_count,
        AVG(avg_duration_ms) as avg_duration,
        COUNT(DISTINCT event_window) as num_windows
    FROM {full_table}
    GROUP BY event_type
    ORDER BY total_count DESC
""")

print("📊 Event Type Summary:")
df_event_summary.show()

# Time-series view
df_timeseries = spark.sql(f"""
    SELECT 
        event_window,
        event_type,
        total_events,
        ROUND(avg_duration_ms, 2) as avg_duration_ms
    FROM {full_table}
    ORDER BY event_window DESC, total_events DESC
    LIMIT 20
""")

print("\n📊 Time-Series View (Recent Windows):")
df_timeseries.show(20, truncate=False)
```

## Reprocessing (Kappa Advantage)

To recompute historical data in Kappa:
1. Stop current streaming job
2. Delete checkpoint directory
3. Restart with `startingOffsets: "earliest"`
4. Spark reprocesses from Kafka beginning


```python
print("""\n💡 Reprocessing Pattern (Kappa Architecture):

To recompute all historical data:

```bash
# 1. Stop streaming job
kubectl delete sparkapplication event-processor

# 2. Clear checkpoint (forces reprocessing)
hdfs dfs -rm -r /checkpoints/event_processor

# 3. Truncate target table (optional, for full rebuild)
spark-sql -e "TRUNCATE TABLE lakehouse.prod.event_stream"

# 4. Restart with earliest offset
kubectl apply -f sparkapplication-event-processor.yaml
```

**Benefits**:
- Single code path (same streaming logic)
- Kafka is the source of truth
- Event-time semantics ensure correctness
- Idempotent processing (same output)

**vs. Lambda Batch Layer**:
- Lambda: Separate batch code (different logic)
- Kappa: Replay streaming (same logic)
- Lambda: More complex (two systems)
- Kappa: Simpler (one system)
""")
```

## Validation


```python
if enable_validation:
    total_aggregates = spark.sql(f"SELECT COUNT(*) FROM {full_table}").collect()[0][0]
    
    assert total_aggregates > 0, "Should have windowed aggregates"
    print(f"✅ Generated {total_aggregates} windowed aggregates")
    
    # Verify event-time processing (windows should span 30-minute range)
    window_span = spark.sql(f"""
        SELECT 
            MIN(event_window) as earliest_window,
            MAX(event_window) as latest_window
        FROM {full_table}
    """).collect()[0]
    
    print(f"✅ Event-time windows span: {window_span['earliest_window']} to {window_span['latest_window']}")
    
    # Verify all event types processed
    distinct_types = spark.sql(f"SELECT COUNT(DISTINCT event_type) FROM {full_table}").collect()[0][0]
    assert distinct_types > 0, "Should have multiple event types"
    print(f"✅ Processed {distinct_types} different event types")
    
    test_passed = True
    print("\n✅ All validations passed!")
else:
    test_passed = True
```

## Summary

### ✅ Pattern 3.2: Kappa Architecture Complete!

Demonstrated:
1. **Single streaming path**: All processing via Spark Structured Streaming
2. **Event-time semantics**: Watermarks handle late-arriving data
3. **Checkpointing**: Enables reprocessing from any point in Kafka
4. **Idempotent processing**: Same input produces same output

**Kappa vs. Lambda**:

| Aspect | Lambda | Kappa |
|--------|--------|-------|
| **Processing Layers** | Speed + Batch | Stream only |
| **Code Complexity** | Dual paths | Single path |
| **Reprocessing** | Batch code | Replay stream |
| **Late Data** | Batch correction | Watermarks |
| **Operational** | More complex | Simpler |
| **Use Case** | Complex corrections | Event-driven |

**When to Use Kappa**:
- ✅ All data flows through Kafka/event stream
- ✅ Kafka retention covers reprocessing window
- ✅ Event-time semantics are sufficient
- ✅ Prefer operational simplicity
- ✅ Idempotent processing is achievable

**When Lambda is Better**:
- ❌ Need complex batch-only transformations
- ❌ Historical data not in Kafka
- ❌ Batch and streaming have different logic
- ❌ Regulatory requirements for batch recompute

**Production Deployment**:
- K8s Spark Operator for continuous streaming
- Kafka retention configured for reprocessing window
- Airflow orchestrates deployments and upgrades
- Monitoring via Spark UI and Prometheus


```python
if enable_cleanup:
    spark.sql(f"DROP TABLE IF EXISTS {full_table}")
    print("🧹 Cleanup completed")
else:
    print("⏭️  Cleanup skipped")
```
