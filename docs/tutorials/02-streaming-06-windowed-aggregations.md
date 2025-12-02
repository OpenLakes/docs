```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType, LongType
from pyspark.sql.functions import col, sum as spark_sum, count, avg, min as spark_min, max as spark_max, when, expr, lit, window, current_timestamp, from_json
import random
from datetime import datetime, date, timedelta
import json
import time
from kafka import KafkaProducer
```

# Pattern 2.6: Stream Aggregation (Windowed)

Real-time aggregations over time windows:

1. **Tumbling Windows**: Fixed non-overlapping intervals (every 5 minutes)
2. **Sliding Windows**: Overlapping intervals (5-min window, 1-min slide)
3. **Stateful Aggregation**: Maintain running counts/sums
4. **Watermarks**: Handle late-arriving data

## Use Cases

- **Hourly website traffic**: Page views per hour
- **5-minute sensor averages**: Temperature/pressure readings
- **Real-time dashboards**: Live metrics updated every minute
- **Fraud detection**: Transaction counts per minute per user

## Window Types

```
Tumbling (non-overlapping):
[00:00-00:05] [00:05-00:10] [00:10-00:15]

Sliding (overlapping):
[00:00-00:05]
  [00:01-00:06]
    [00:02-00:07]
```

## Parameters


```python
execution_date = "2025-01-16"
environment = "development"
database_name = "demo"
kafka_topic = "sensor_readings"
tumbling_table = "sensor_5min_avg"
sliding_table = "sensor_5min_slide1min"
kafka_bootstrap_servers = "infrastructure-kafka.openlakes.svc.cluster.local:9092"
num_sensors = 5
num_readings = 300
window_duration = "5 minutes"
slide_duration = "1 minute"
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
    .appName(f"Pattern-2.6-WindowedAgg-{environment}") \
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

print("✅ Spark initialized for windowed aggregations")
```

## Setup: Create Iceberg Tables for Aggregations


```python
full_database = f"lakehouse.{database_name}"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_database}")

# Tumbling window results
full_tumbling = f"{full_database}.{tumbling_table}"
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {full_tumbling} (
        window_start TIMESTAMP,
        window_end TIMESTAMP,
        sensor_id STRING,
        avg_temperature DOUBLE,
        reading_count BIGINT,
        min_temperature DOUBLE,
        max_temperature DOUBLE,
        processing_time TIMESTAMP
    ) USING iceberg
""")

# Sliding window results
full_sliding = f"{full_database}.{sliding_table}"
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {full_sliding} (
        window_start TIMESTAMP,
        window_end TIMESTAMP,
        sensor_id STRING,
        avg_temperature DOUBLE,
        reading_count BIGINT,
        processing_time TIMESTAMP
    ) USING iceberg
""")

print(f"✅ Created tables for tumbling and sliding window results")
```

## Kafka Producer: Generate Timestamped Sensor Data


```python
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"📊 Producing {num_readings} sensor readings (spread over 15 minutes)...")

# Generate events spread over time
base_time = datetime.utcnow() - timedelta(minutes=15)

for i in range(num_readings):
    # Spread events across 15 minutes
    event_time = base_time + timedelta(seconds=i * 3)  # Every 3 seconds
    
    reading = {
        "sensor_id": f"sensor_{random.randint(1, num_sensors)}",
        "temperature": round(random.uniform(60.0, 90.0), 2),
        "timestamp": event_time.isoformat()
    }
    
    producer.send(kafka_topic, value=reading)
    
    if (i + 1) % 100 == 0:
        print(f"  Sent {i + 1}/{num_readings} readings...")

producer.flush()
print(f"✅ Produced {num_readings} sensor readings")
```

## Pattern 1: Tumbling Window (Non-Overlapping 5-min intervals)


```python
# Read stream
sensor_schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load() \
    .select(from_json(col("value").cast("string"), sensor_schema).alias("data")) \
    .select(
        col("data.sensor_id").alias("sensor_id"),
        col("data.temperature").alias("temperature"),
        col("data.timestamp").cast(TimestampType()).alias("event_time")
    ) \
    .withWatermark("event_time", watermark_delay)

# Tumbling window aggregation
df_tumbling_agg = df_stream.groupBy(
    window(col("event_time"), window_duration),  # 5-minute tumbling windows
    col("sensor_id")
).agg(
    avg("temperature").alias("avg_temperature"),
    count("*").alias("reading_count"),
    spark_min("temperature").alias("min_temperature"),
    spark_max("temperature").alias("max_temperature")
).select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("sensor_id"),
    col("avg_temperature"),
    col("reading_count"),
    col("min_temperature"),
    col("max_temperature"),
    current_timestamp().alias("processing_time")
)

print("✅ Tumbling window aggregation configured (5-minute windows)")
```

## Write Tumbling Window Results


```python
query_tumbling = df_tumbling_agg.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", f"/tmp/checkpoint_{tumbling_table}") \
    .toTable(full_tumbling)

print(f"✅ Tumbling window query started")
print(f"⏳ Processing for {streaming_duration_seconds} seconds...\n")

time.sleep(streaming_duration_seconds)

query_tumbling.stop()
print("\n✅ Tumbling window query stopped")
```


```python
# Generate fresh data for second query
print(f"\n📊 Generating fresh data for sliding window query...")
print(f"   Producing {num_readings} additional sensor readings...")

base_time = datetime.utcnow() - timedelta(minutes=15)

for i in range(num_readings):
    event_time = base_time + timedelta(seconds=i * 3)
    
    reading = {
        "sensor_id": f"sensor_{random.randint(1, num_sensors)}",
        "temperature": round(random.uniform(60.0, 90.0), 2),
        "timestamp": event_time.isoformat()
    }
    
    producer.send(kafka_topic, value=reading)
    
    if (i + 1) % 100 == 0:
        print(f"  Sent {i + 1}/{num_readings} readings...")

producer.flush()
print(f"✅ Produced {num_readings} additional readings for sliding window query\n")
```

## Pattern 2: Sliding Window (Overlapping 5-min windows, 1-min slide)


```python
# Re-read stream for sliding window
df_stream_2 = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load() \
    .select(from_json(col("value").cast("string"), sensor_schema).alias("data")) \
    .select(
        col("data.sensor_id").alias("sensor_id"),
        col("data.temperature").alias("temperature"),
        col("data.timestamp").cast(TimestampType()).alias("event_time")
    ) \
    .withWatermark("event_time", watermark_delay)

# Sliding window aggregation
df_sliding_agg = df_stream_2.groupBy(
    window(col("event_time"), window_duration, slide_duration),  # 5-min window, 1-min slide
    col("sensor_id")
).agg(
    avg("temperature").alias("avg_temperature"),
    count("*").alias("reading_count")
).select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("sensor_id"),
    col("avg_temperature"),
    col("reading_count"),
    current_timestamp().alias("processing_time")
)

print("✅ Sliding window aggregation configured (5-min window, 1-min slide)")
```

## Write Sliding Window Results


```python
query_sliding = df_sliding_agg.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", f"/tmp/checkpoint_{sliding_table}") \
    .toTable(full_sliding)

print(f"✅ Sliding window query started")
print(f"⏳ Processing for {streaming_duration_seconds} seconds...\n")

time.sleep(streaming_duration_seconds)

query_sliding.stop()
print("\n✅ Sliding window query stopped")
```

## Query: Compare Tumbling vs Sliding Results


```python
# Tumbling window results
df_tumbling_results = spark.sql(f"""
    SELECT 
        window_start,
        window_end,
        sensor_id,
        ROUND(avg_temperature, 2) as avg_temp,
        reading_count,
        ROUND(min_temperature, 2) as min_temp,
        ROUND(max_temperature, 2) as max_temp
    FROM {full_tumbling}
    ORDER BY window_start, sensor_id
""")

print("📊 Tumbling Window Results (5-min non-overlapping):")
df_tumbling_results.show(10)

# Sliding window results
df_sliding_results = spark.sql(f"""
    SELECT 
        window_start,
        window_end,
        sensor_id,
        ROUND(avg_temperature, 2) as avg_temp,
        reading_count
    FROM {full_sliding}
    WHERE sensor_id = 'sensor_1'
    ORDER BY window_start
""")

print("\n📊 Sliding Window Results (sensor_1, overlapping windows):")
df_sliding_results.show(10)
```

## Validation


```python
if enable_validation:
    tumbling_count = spark.sql(f"SELECT COUNT(*) FROM {full_tumbling}").collect()[0][0]
    sliding_count = spark.sql(f"SELECT COUNT(*) FROM {full_sliding}").collect()[0][0]
    
    assert tumbling_count > 0, "Should have tumbling window results"
    assert sliding_count > 0, "Should have sliding window results"
    print(f"✅ Tumbling windows: {tumbling_count} aggregates")
    print(f"✅ Sliding windows: {sliding_count} aggregates")
    
    # Sliding should have more results (overlapping windows)
    assert sliding_count >= tumbling_count, "Sliding windows should have more results (overlapping)"
    print(f"✅ Sliding has {sliding_count / tumbling_count:.1f}x more results (due to overlap)")
    
    # Verify aggregates are reasonable
    avg_temp = spark.sql(f"SELECT AVG(avg_temperature) FROM {full_tumbling}").collect()[0][0]
    assert 60 <= avg_temp <= 90, "Average temperature should be in expected range"
    print(f"✅ Average temperature across windows: {avg_temp:.2f}°F")
    
    test_passed = True
    print("\n✅ All validations passed!")
else:
    test_passed = True
```

## Summary

### ✅ Pattern 2.6: Windowed Aggregations Complete!

Demonstrated:
1. Tumbling windows (non-overlapping 5-min intervals)
2. Sliding windows (overlapping 5-min windows, 1-min slide)
3. Stateful aggregations (avg, count, min, max)
4. Watermarks for late data handling
5. Real-time metrics for dashboards

**Tumbling vs Sliding**:
```
Tumbling: [00:00-00:05] [00:05-00:10] [00:10-00:15]
  - Non-overlapping, mutually exclusive
  - Each event appears in exactly 1 window
  - Less output, more efficient

Sliding: [00:00-00:05]
           [00:01-00:06]
             [00:02-00:07]
  - Overlapping windows
  - Each event appears in multiple windows
  - Smoother trends, more output
```

**When to Use**:
- Tumbling: Hourly reports, daily summaries, distinct time periods
- Sliding: Moving averages, smoothed trends, real-time dashboards

**Use Cases**: Website traffic metrics, IoT sensor averages, financial tick data, real-time monitoring dashboards


```python
if enable_cleanup:
    spark.sql(f"DROP TABLE IF EXISTS {full_tumbling}")
    spark.sql(f"DROP TABLE IF EXISTS {full_sliding}")
    print("🧹 Cleanup completed")
else:
    print("⏭️  Cleanup skipped")
```
