```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType, LongType
from pyspark.sql.functions import col, sum as spark_sum, count, avg, when, expr, lit, window, current_timestamp, from_json, to_date, count_distinct
import random
from datetime import datetime, date, timedelta
import json
import time
from kafka import KafkaProducer
```

# Pattern 3.1: Lambda Architecture (Speed + Batch Layers)

Classic Lambda Architecture with separate speed and batch processing layers:

1. **Speed Layer**: Real-time Spark Streaming → Iceberg (hourly partitions)
2. **Batch Layer**: Daily Spark job → Iceberg (recomputed daily partitions)
3. **Serving Layer**: Query merges recent streaming data with batch-corrected history

## Architecture

```
Kafka (order_events)
    ↓
Speed Layer: Spark Streaming → lakehouse.demo.orders_speed (hourly)
    ↓
Batch Layer: Spark Batch → lakehouse.demo.orders_batch (daily recompute)
    ↓
Serving: UNION recent speed + historical batch → Unified view
```

**Use Case**: E-commerce order analytics where speed layer shows immediate trends, batch layer corrects for late-arriving data and deduplication

**Benefits**:
- Real-time visibility (speed layer)
- Historical accuracy (batch layer recomputes)
- Best of both worlds (lambda pattern)

**Tradeoffs**:
- Dual code paths (streaming + batch)
- Higher operational complexity
- Storage overhead (speed + batch tables)


```python
execution_date = "2025-01-17"
environment = "development"
database_name = "demo"
speed_table = "orders_speed"
batch_table = "orders_batch"
kafka_bootstrap_servers = "infrastructure-kafka.openlakes.svc.cluster.local:9092"
kafka_topic = "order_events"
num_events = 100
streaming_duration_seconds = 30
speed_checkpoint_interval = "10 seconds"
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
    .appName(f"Pattern-3.1-LambdaArchitecture-{environment}") \
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

print("✅ Spark initialized for Lambda Architecture")


```python
import os

from pyspark.sql import SparkSession
# Set AWS environment variables for S3FileIO
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "admin123"

spark = SparkSession.builder \
    .appName(f"Pattern-3.1-LambdaArchitecture-{environment}") \
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

print("✅ Spark initialized for Lambda Architecture")
```

## Setup: Create Iceberg Tables


```python
full_database = f"lakehouse.{database_name}"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_database}")

full_speed_table = f"{full_database}.{speed_table}"
full_batch_table = f"{full_database}.{batch_table}"

# Speed layer table (streaming writes, hourly aggregates)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {full_speed_table} (
        order_hour TIMESTAMP,
        total_orders BIGINT,
        total_revenue DOUBLE,
        avg_order_value DOUBLE,
        processing_timestamp TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (days(order_hour))
""")

# Batch layer table (batch recompute, daily aggregates)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {full_batch_table} (
        order_date DATE,
        total_orders BIGINT,
        total_revenue DOUBLE,
        avg_order_value DOUBLE,
        unique_customers BIGINT,
        batch_run_timestamp TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (order_date)
""")

print(f"✅ Created Lambda tables:")
print(f"   Speed: {full_speed_table}")
print(f"   Batch: {full_batch_table}")
```


```python
# Start Kafka producer to simulate order events
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"📊 Producing {num_events} order events to Kafka topic '{kafka_topic}'...")

# Generate sample order data
customers = [f"customer_{i}" for i in range(1, 21)]  # 20 customers
statuses = ["completed", "pending", "cancelled"]

for i in range(num_events):
    event = {
        "order_id": f"order_{i + 1}",
        "customer_id": random.choice(customers),
        "order_value": round(random.uniform(10.0, 500.0), 2),
        "timestamp": datetime.utcnow().isoformat(),
        "status": random.choice(statuses)
    }
    producer.send(kafka_topic, value=event)
    
    if (i + 1) % 20 == 0:
        print(f"  Sent {i + 1}/{num_events} events...")
        time.sleep(0.5)

producer.flush()
print(f"✅ Produced {num_events} events to Kafka")
```

## Kafka Producer: Generate Order Events


```python
<cell_type>markdown</cell_type>## Speed Layer: Spark Streaming → Iceberg

Real-time processing with hourly windows for immediate visibility.
```

# Define schema
order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_value", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("status", StringType(), True)
])

# Read from Kafka
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse and filter completed orders
df_parsed = df_stream.select(
    from_json(col("value").cast("string"), order_schema).alias("data")
).select(
    col("data.order_id").alias("order_id"),
    col("data.customer_id").alias("customer_id"),
    col("data.order_value").alias("order_value"),
    col("data.timestamp").cast(TimestampType()).alias("order_timestamp"),
    col("data.status").alias("status")
).filter(col("status") == "completed")

# Hourly windowed aggregation (speed layer)
df_speed_agg = df_parsed \
    .withWatermark("order_timestamp", "10 minutes") \
    .groupBy(window(col("order_timestamp"), "1 hour")) \
    .agg(
        count("*").alias("total_orders"),
        spark_sum("order_value").alias("total_revenue"),
        avg("order_value").alias("avg_order_value")
    ).select(
        col("window.start").alias("order_hour"),
        col("total_orders"),
        col("total_revenue"),
        col("avg_order_value"),
        current_timestamp().alias("processing_timestamp")
    )

# Write speed layer to Iceberg
query_speed = df_speed_agg.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", f"/tmp/checkpoint_speed_{speed_table}") \
    .trigger(processingTime=speed_checkpoint_interval) \
    .toTable(full_speed_table)

print(f"✅ Speed layer started: Kafka → Iceberg ({full_speed_table})")
print(f"⏳ Processing for {streaming_duration_seconds} seconds...\n")

time.sleep(streaming_duration_seconds)

query_speed.stop()
print("\n✅ Speed layer stopped")

# Simulate batch processing: Read all Kafka data for the day
df_batch_raw = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

# Parse orders
df_batch_parsed = df_batch_raw.select(
    from_json(col("value").cast("string"), order_schema).alias("data")
).select(
    col("data.order_id").alias("order_id"),
    col("data.customer_id").alias("customer_id"),
    col("data.order_value").alias("order_value"),
    col("data.timestamp").cast(TimestampType()).alias("order_timestamp"),
    col("data.status").alias("status")
).filter(col("status") == "completed")

# Daily aggregation with deduplication (batch layer accuracy)
df_batch_agg = df_batch_parsed \
    .dropDuplicates(["order_id"]) \
    .withColumn("order_date", to_date(col("order_timestamp"))) \
    .groupBy("order_date") \
    .agg(
        count("*").alias("total_orders"),
        spark_sum("order_value").alias("total_revenue"),
        avg("order_value").alias("avg_order_value"),
        count_distinct("customer_id").alias("unique_customers")
    ).withColumn("batch_run_timestamp", current_timestamp())

# Write batch layer to Iceberg (overwrites daily partition)
df_batch_agg.writeTo(full_batch_table) \
    .using("iceberg") \
    .overwritePartitions()

print(f"✅ Batch layer complete: Recomputed daily aggregates → {full_batch_table}")


```python
# Simulate batch processing: Read all Kafka data for the day
df_batch_raw = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

# Parse orders
df_batch_parsed = df_batch_raw.select(
    from_json(col("value").cast("string"), order_schema).alias("data")
).select(
    col("data.order_id").alias("order_id"),
    col("data.customer_id").alias("customer_id"),
    col("data.order_value").alias("order_value"),
    col("data.timestamp").cast(TimestampType()).alias("order_timestamp"),
    col("data.status").alias("status")
).filter(col("status") == "completed")

# Daily aggregation with deduplication (batch layer accuracy)
df_batch_agg = df_batch_parsed \
    .dropDuplicates(["order_id"]) \
    .withColumn("order_date", to_date(col("order_timestamp"))) \
    .groupBy("order_date") \
    .agg(
        count("*").alias("total_orders"),
        _sum("order_value").alias("total_revenue"),
        avg("order_value").alias("avg_order_value"),
        col("customer_id").alias("unique_customers").cast("long")  # Simplified for demo
    ).withColumn("batch_run_timestamp", current_timestamp())

# Write batch layer to Iceberg (overwrites daily partition)
df_batch_agg.writeTo(full_batch_table) \
    .using("iceberg") \
    .overwritePartitions()

print(f"✅ Batch layer complete: Recomputed daily aggregates → {full_batch_table}")
```

## Serving Layer: Unified Query (Speed + Batch)

Query strategy:
- **Recent data** (last 24 hours): Use speed layer (real-time)
- **Historical data** (> 24 hours): Use batch layer (accurate)


```python
# Speed layer: Recent hourly data
df_speed_recent = spark.sql(f"""
    SELECT 
        order_hour,
        total_orders,
        total_revenue,
        avg_order_value,
        'speed' as source
    FROM {full_speed_table}
    WHERE order_hour >= CURRENT_TIMESTAMP - INTERVAL 24 HOURS
    ORDER BY order_hour DESC
""")

print("📊 Speed Layer (Recent 24h - Hourly Aggregates):")
df_speed_recent.show(10)

# Batch layer: Historical daily data
df_batch_historical = spark.sql(f"""
    SELECT 
        order_date,
        total_orders,
        total_revenue,
        avg_order_value,
        unique_customers,
        'batch' as source
    FROM {full_batch_table}
    ORDER BY order_date DESC
""")

print("\n📊 Batch Layer (Daily Aggregates - Deduplicated):")
df_batch_historical.show(10)
```

## Validation


```python
if enable_validation:
    speed_count = spark.sql(f"SELECT COUNT(*) FROM {full_speed_table}").collect()[0][0]
    batch_count = spark.sql(f"SELECT COUNT(*) FROM {full_batch_table}").collect()[0][0]
    
    assert speed_count > 0, "Speed layer should have records"
    print(f"✅ Speed layer: {speed_count} hourly aggregates")
    
    assert batch_count > 0, "Batch layer should have records"
    print(f"✅ Batch layer: {batch_count} daily aggregates")
    
    # Verify batch layer has unique customers metric
    unique_cols = spark.sql(f"DESCRIBE {full_batch_table}").select("col_name").rdd.flatMap(lambda x: x).collect()
    assert "unique_customers" in unique_cols, "Batch layer should have unique_customers"
    print(f"✅ Batch layer enriched with additional metrics")
    
    test_passed = True
    print("\n✅ All validations passed!")
else:
    test_passed = True
```

## Summary

### ✅ Pattern 3.1: Lambda Architecture Complete!

Demonstrated:
1. **Speed Layer**: Spark Streaming with hourly windows for real-time visibility
2. **Batch Layer**: Spark batch job with daily recomputation for accuracy
3. **Serving Layer**: Unified query combining recent speed + historical batch

**Lambda Pattern Benefits**:
- ⚡ Real-time insights (speed layer, sub-minute)
- 🎯 Historical accuracy (batch layer, handles late data)
- 📊 Enriched metrics (batch can add complex calculations)
- 🔄 Automatic correction (batch overwrites speed after 24h)

**When to Use Lambda**:
- Need real-time dashboards but also accuracy guarantees
- Late-arriving data is common
- Complex deduplication or enrichment logic
- Regulatory requirements for recomputation

**vs. Kappa Architecture**:
- Lambda: Dual code paths (more complex, more flexible)
- Kappa: Single streaming path (simpler, less overhead)
- Lambda: Better for complex batch corrections
- Kappa: Better for pure event-time processing

**Production Deployment**:
- Speed layer: Continuous streaming (K8s Spark Operator)
- Batch layer: Airflow daily job (overwrites old speed partitions)
- Serving: Trino federated query or Iceberg MERGE


```python
if enable_cleanup:
    spark.sql(f"DROP TABLE IF EXISTS {full_speed_table}")
    spark.sql(f"DROP TABLE IF EXISTS {full_batch_table}")
    print("🧹 Cleanup completed")
else:
    print("⏭️  Cleanup skipped")
```
