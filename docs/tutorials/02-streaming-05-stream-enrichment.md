```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType, LongType
from pyspark.sql.functions import col, sum as spark_sum, count, avg, when, expr, lit, window, current_timestamp, from_json, broadcast
import random
from datetime import datetime, date, timedelta
import json
import time
from kafka import KafkaProducer
```

# Pattern 2.5: Stream Enrichment with Dimension Lookup

Enrich streaming events with reference data:

1. **Kafka Stream**: Raw clickstream events (user_id only)
2. **Iceberg Dimension Table**: Customer attributes (name, tier, region)
3. **Broadcast Join**: Enrich stream with dimension data
4. **Enriched Stream**: Full customer context for each event

## Architecture

```
Kafka (raw_clicks) ──────┐
                          ├──→ Spark Stream + Broadcast Join
Iceberg (customers) ─────┘        ↓
    (dimension table)         Iceberg (enriched_clicks)
```

**Use Case**: Add customer attributes to every clickstream event without impacting stream latency

## Parameters


```python
execution_date = "2025-01-16"
environment = "development"
database_name = "demo"
kafka_topic = "raw_clickstream"
dimension_table = "customer_dim"
enriched_table = "clickstream_enriched"
kafka_bootstrap_servers = "infrastructure-kafka.openlakes.svc.cluster.local:9092"
num_customers = 100
num_click_events = 200
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
    .appName(f"Pattern-2.5-StreamEnrichment-{environment}") \
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

print("✅ Spark initialized for stream enrichment")
```

## Setup: Create Dimension Table (Customer Master Data)


```python
full_database = f"lakehouse.{database_name}"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_database}")

# Create customer dimension table
customer_data = []
for i in range(1, num_customers + 1):
    customer_data.append((
        f"cust_{i:04d}",
        f"Customer {i}",
        random.choice(["Gold", "Silver", "Bronze"]),
        random.choice(["North", "South", "East", "West"]),
        random.choice(["Enterprise", "SMB", "Startup"])
    ))

df_customers = spark.createDataFrame(
    customer_data,
    ["customer_id", "customer_name", "tier", "region", "segment"]
)

full_dim_table = f"{full_database}.{dimension_table}"
df_customers.writeTo(full_dim_table).using("iceberg").createOrReplace()

print(f"✅ Created dimension table with {num_customers} customers: {full_dim_table}")
df_customers.show(5)
```

## Setup: Create Enriched Table


```python
full_enriched_table = f"{full_database}.{enriched_table}"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {full_enriched_table} (
        event_id STRING,
        customer_id STRING,
        customer_name STRING,
        tier STRING,
        region STRING,
        segment STRING,
        page_url STRING,
        event_type STRING,
        event_timestamp TIMESTAMP,
        processing_time TIMESTAMP
    ) USING iceberg
""")

print(f"✅ Enriched table created: {full_enriched_table}")
```

## Kafka Producer: Generate Raw Events (No Customer Details)


```python
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"📊 Producing {num_click_events} raw clickstream events (customer_id only)...")

for i in range(num_click_events):
    event = {
        "event_id": f"evt_{i:06d}",
        "customer_id": f"cust_{random.randint(1, num_customers):04d}",
        "page_url": random.choice(["/home", "/products", "/pricing", "/contact", "/docs"]),
        "event_type": random.choice(["page_view", "click", "download", "signup"]),
        "timestamp": datetime.utcnow().isoformat()
    }
    producer.send(kafka_topic, value=event)
    
    if (i + 1) % 50 == 0:
        print(f"  Sent {i + 1}/{num_click_events} events...")
        time.sleep(0.3)

producer.flush()
print(f"✅ Produced {num_click_events} raw events to Kafka")
```

## Spark Streaming: Read + Enrich with Broadcast Join


```python
# Read raw stream
raw_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", StringType(), True)
])

df_raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load() \
    .select(from_json(col("value").cast("string"), raw_schema).alias("data")) \
    .select(
        col("data.event_id").alias("event_id"),
        col("data.customer_id").alias("customer_id"),
        col("data.page_url").alias("page_url"),
        col("data.event_type").alias("event_type"),
        col("data.timestamp").cast(TimestampType()).alias("event_timestamp")
    )

print("✅ Raw stream configured")

# Load dimension table (batch read for broadcast join)
df_dim = spark.read.format("iceberg").load(full_dim_table)
print(f"✅ Dimension table loaded ({df_dim.count()} customers)")

# Enrich stream with broadcast join
df_enriched = df_raw_stream.join(
    broadcast(df_dim),  # Broadcast small dimension table to all workers
    on="customer_id",
    how="inner"
).select(
    col("event_id"),
    col("customer_id"),
    col("customer_name"),
    col("tier"),
    col("region"),
    col("segment"),
    col("page_url"),
    col("event_type"),
    col("event_timestamp"),
    current_timestamp().alias("processing_time")
)

print("✅ Stream enrichment configured (broadcast join with dimension table)")
```

## Iceberg Sink: Write Enriched Stream


```python
query = df_enriched.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", f"/tmp/checkpoint_{enriched_table}") \
    .toTable(full_enriched_table)

print(f"✅ Enrichment pipeline started: Raw Events + Customer Dim → Enriched")
print(f"⏳ Processing for {streaming_duration_seconds} seconds...\n")

time.sleep(streaming_duration_seconds)

query.stop()
print("\n✅ Streaming query stopped")
```

## Query: Analytics on Enriched Data


```python
# Event distribution by customer tier
df_tier_summary = spark.sql(f"""
    SELECT 
        tier,
        COUNT(*) as event_count,
        COUNT(DISTINCT customer_id) as unique_customers
    FROM {full_enriched_table}
    GROUP BY tier
    ORDER BY event_count DESC
""")

print("📊 Event Distribution by Customer Tier:")
df_tier_summary.show()

# Regional activity
df_regional = spark.sql(f"""
    SELECT 
        region,
        segment,
        COUNT(*) as events,
        COUNT(DISTINCT customer_id) as customers
    FROM {full_enriched_table}
    GROUP BY region, segment
    ORDER BY events DESC
    LIMIT 10
""")

print("\n📊 Top Regional Activity (with Segment):")
df_regional.show()

# Sample enriched events
df_sample = spark.sql(f"""
    SELECT 
        event_id,
        customer_name,
        tier,
        region,
        event_type,
        page_url
    FROM {full_enriched_table}
    LIMIT 5
""")

print("\n📊 Sample Enriched Events:")
df_sample.show(truncate=False)
```

## Validation


```python
if enable_validation:
    enriched_count = spark.sql(f"SELECT COUNT(*) FROM {full_enriched_table}").collect()[0][0]
    
    assert enriched_count > 0, "Should have enriched some events"
    print(f"✅ Enriched {enriched_count} events")
    
    # Verify all events have customer attributes
    null_tier_count = spark.sql(f"SELECT COUNT(*) FROM {full_enriched_table} WHERE tier IS NULL").collect()[0][0]
    assert null_tier_count == 0, "All events should have tier (from dimension join)"
    print(f"✅ All events enriched with customer attributes")
    
    # Verify dimension columns exist
    columns = spark.sql(f"SELECT * FROM {full_enriched_table} LIMIT 1").columns
    dim_columns = ["customer_name", "tier", "region", "segment"]
    for col_name in dim_columns:
        assert col_name in columns, f"Missing dimension column: {col_name}"
    print(f"✅ All dimension attributes present: {dim_columns}")
    
    test_passed = True
    print("\n✅ All validations passed!")
else:
    test_passed = True
```

## Summary

### ✅ Pattern 2.5: Stream Enrichment Complete!

Demonstrated:
1. Dimension table with customer master data
2. Raw event stream (minimal data - customer_id only)
3. Broadcast join (efficient for small dimension tables)
4. Enriched stream with full customer context
5. Analytics segmented by customer attributes

**Broadcast Join Explained**:
```python
df.join(broadcast(dimension_table), on="key")  # Efficient!
```
- Dimension table copied to all worker nodes
- No shuffle required (very fast)
- Works best for small-medium dimension tables (<1GB)

**Benefits**:
- ⚡ Minimal latency (broadcast join is fast)
- 📊 Rich analytics (segment by customer attributes)
- 🔄 Separation of concerns (transactional events + reference data)
- 💾 Storage efficient (don't duplicate dimension data in stream)

**Use Cases**: Clickstream enrichment, IoT sensor enrichment (add location/device metadata), log enrichment (add user/org details)


```python
if enable_cleanup:
    spark.sql(f"DROP TABLE IF EXISTS {full_dim_table}")
    spark.sql(f"DROP TABLE IF EXISTS {full_enriched_table}")
    print("🧹 Cleanup completed")
else:
    print("⏭️  Cleanup skipped")
```
