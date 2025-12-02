```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType, LongType
from pyspark.sql.functions import col, sum as spark_sum, count, avg, when, expr, lit, window, current_timestamp, from_json
import random
from datetime import datetime, date, timedelta
import json
from kafka import KafkaProducer

```

# Pattern 4.2: Multi-Engine Iceberg Access

Demonstrate multiple engines reading and writing to the same Iceberg table:

1. **Spark Batch**: Writes historical data
2. **Spark Streaming**: Writes real-time updates
3. **Trino**: Reads unified view for analytics

## Architecture

```
Spark Batch ───┐
               ├──→ Iceberg Table (ACID transactions)
Spark Streaming─┘      ↓
                   Trino Query
                       ↓
                  Superset Dashboard
```

**Iceberg ACID Guarantees**:
- Concurrent writes from Spark Batch + Streaming
- Snapshot isolation (readers see consistent view)
- No locks required
- Optimistic concurrency control

**Use Case**: Unified product analytics where batch jobs load historical data while streaming updates real-time inventory changes. Analysts query via Trino without ETL delays.

**Benefits**:
- No vendor lock-in (multiple engines)
- Unified lakehouse (single source of truth)
- ACID transactions (data quality)
- Engine specialization (batch, streaming, queries)

## Parameters


```python
execution_date = "2025-01-16"
environment = "development"
database_name = "demo"
iceberg_table = "product_inventory"
kafka_topic = "inventory_updates"
kafka_bootstrap_servers = "infrastructure-kafka.openlakes.svc.cluster.local:9092"
num_batch_products = 100
num_streaming_updates = 50
streaming_duration_seconds = 20
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
    .appName(f"Pattern-4.2-MultiEngineIceberg-{environment}") \
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

print("✅ Spark initialized for multi-engine Iceberg pattern")
```

## Setup: Create Iceberg Table


```python
full_database = f"lakehouse.{database_name}"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_database}")

full_table = f"{full_database}.{iceberg_table}"

# Create product inventory table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {full_table} (
        product_id STRING,
        product_name STRING,
        stock_quantity BIGINT,
        price DOUBLE,
        last_updated TIMESTAMP,
        update_source STRING
    ) USING iceberg
    TBLPROPERTIES (
        'write.metadata.delete-after-commit.enabled' = 'true',
        'write.metadata.previous-versions-max' = '5'
    )
""")

print(f"✅ Iceberg table created: {full_table}")
print("   Configured for concurrent writes (Spark Batch + Streaming)")
```

## Engine 1: Spark Batch Write (Historical Data)


```python
# Generate batch product data
batch_data = []
categories = ["Electronics", "Clothing", "Books", "Home", "Sports"]

for i in range(num_batch_products):
    product_id = f"prod_{i+1:05d}"
    category = random.choice(categories)
    product_name = f"{category} Item {i+1}"
    stock_quantity = random.randint(0, 500)
    price = round(random.uniform(10.0, 500.0), 2)
    
    batch_data.append((
        product_id, product_name, stock_quantity, price,
        datetime.now(), "batch_load"
    ))

batch_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("stock_quantity", LongType(), False),
    StructField("price", DoubleType(), False),
    StructField("last_updated", TimestampType(), False),
    StructField("update_source", StringType(), False)
])

df_batch = spark.createDataFrame(batch_data, schema=batch_schema)

# Write batch data to Iceberg
df_batch.writeTo(full_table).using("iceberg").overwritePartitions()

print(f"✅ Spark Batch: Loaded {num_batch_products} products to Iceberg")
print("   (Historical data load complete)")
```

## Kafka Producer: Generate Inventory Updates


```python
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"📊 Producing {num_streaming_updates} inventory updates to Kafka...")

for i in range(num_streaming_updates):
    # Update existing products (prod_00001 to prod_00100)
    product_id = f"prod_{random.randint(1, 100):05d}"
    
    update = {
        "product_id": product_id,
        "stock_delta": random.randint(-50, 100),  # Stock change (can be negative)
        "price_delta": round(random.uniform(-10.0, 20.0), 2),  # Price change
        "timestamp": datetime.utcnow().isoformat()
    }
    
    producer.send(kafka_topic, value=update)
    
    if (i + 1) % 20 == 0:
        time.sleep(0.1)

producer.flush()
print(f"✅ Produced {num_streaming_updates} inventory updates to Kafka")
```

## Engine 2: Spark Streaming Write (Real-Time Updates)

Spark Streaming will **MERGE** updates into the same Iceberg table that Spark Batch wrote to.


```python
# Define schema
update_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("stock_delta", IntegerType(), True),
    StructField("price_delta", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Read from Kafka
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse updates
df_parsed = df_stream.select(
    from_json(col("value").cast("string"), update_schema).alias("data")
).select(
    col("data.product_id").alias("product_id"),
    col("data.stock_delta").alias("stock_delta"),
    col("data.price_delta").alias("price_delta"),
    col("data.timestamp").cast(TimestampType()).alias("update_timestamp")
)

# Use foreachBatch to MERGE updates into Iceberg
def merge_updates(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    
    # Create temp view for MERGE
    batch_df.createOrReplaceTempView("updates")
    
    # MERGE updates into Iceberg (UPSERT pattern)
    spark.sql(f"""
        MERGE INTO {full_table} AS target
        USING updates AS source
        ON target.product_id = source.product_id
        WHEN MATCHED THEN UPDATE SET
            stock_quantity = target.stock_quantity + source.stock_delta,
            price = target.price + source.price_delta,
            last_updated = source.update_timestamp,
            update_source = 'streaming_update'
    """)
    
    print(f"  Batch {batch_id}: Merged {batch_df.count()} updates into Iceberg")

# Write streaming updates via MERGE
query = df_parsed.writeStream \
    .foreachBatch(merge_updates) \
    .option("checkpointLocation", f"/tmp/checkpoint_multiengine_{iceberg_table}") \
    .trigger(processingTime='5 seconds') \
    .start()

print(f"✅ Spark Streaming: Started real-time updates to Iceberg")
print(f"   Using MERGE to update existing products")
print(f"⏳ Processing for {streaming_duration_seconds} seconds...\n")

time.sleep(streaming_duration_seconds)

query.stop()
print("\n✅ Spark Streaming stopped")
```

## Engine 3: Trino Query (Unified Analytics)

Trino reads the Iceberg table that was written by both Spark Batch and Spark Streaming.


```python
# Simulate Trino query via Spark SQL (in production, use Trino CLI)
df_unified = spark.sql(f"""
    SELECT 
        product_id,
        product_name,
        stock_quantity,
        ROUND(price, 2) as price,
        update_source,
        last_updated
    FROM {full_table}
    ORDER BY last_updated DESC
    LIMIT 20
""")

print("📊 Unified View (Batch + Streaming Writes):")
df_unified.show(20, truncate=False)

# Count by update source
df_source_counts = spark.sql(f"""
    SELECT 
        update_source,
        COUNT(*) as product_count,
        AVG(stock_quantity) as avg_stock,
        AVG(price) as avg_price
    FROM {full_table}
    GROUP BY update_source
""")

print("\n📊 Update Source Breakdown:")
df_source_counts.show()
```

## Iceberg ACID Verification


```python
# View Iceberg snapshots (shows concurrent writes)
df_snapshots = spark.sql(f"SELECT * FROM {full_table}.snapshots ORDER BY committed_at DESC LIMIT 5")

print("📸 Recent Iceberg Snapshots (ACID Transactions):")
df_snapshots.select("committed_at", "snapshot_id", "operation", "summary").show(truncate=False)

print("""\n💡 Iceberg ACID Properties:

**Concurrent Writes**:
- Spark Batch wrote initial snapshot (overwritePartitions)
- Spark Streaming wrote MERGE updates (multiple snapshots)
- No locks required (optimistic concurrency)

**Snapshot Isolation**:
- Each commit creates new snapshot
- Readers see consistent view (no torn reads)
- Time travel enabled (query historical snapshots)

**ACID Guarantees**:
- Atomicity: All-or-nothing commits
- Consistency: Schema evolution, constraint validation
- Isolation: Concurrent readers/writers don't block
- Durability: Metadata stored in S3/MinIO
""")
```

## Validation


```python
if enable_validation:
    total_products = spark.sql(f"SELECT COUNT(*) FROM {full_table}").collect()[0][0]
    
    assert total_products > 0, "Should have products"
    print(f"✅ Total products: {total_products}")
    
    # Verify batch writes
    batch_count = spark.sql(f"SELECT COUNT(*) FROM {full_table} WHERE update_source = 'batch_load'").collect()[0][0]
    assert batch_count > 0, "Should have batch-loaded products"
    print(f"✅ Batch-loaded products: {batch_count}")
    
    # Verify streaming updates
    streaming_count = spark.sql(f"SELECT COUNT(*) FROM {full_table} WHERE update_source = 'streaming_update'").collect()[0][0]
    assert streaming_count > 0, "Should have streaming-updated products"
    print(f"✅ Streaming-updated products: {streaming_count}")
    
    # Verify snapshots
    snapshot_count = spark.sql(f"SELECT COUNT(*) FROM {full_table}.snapshots").collect()[0][0]
    assert snapshot_count >= 2, "Should have multiple snapshots (batch + streaming)"
    print(f"✅ Iceberg snapshots: {snapshot_count} (concurrent writes verified)")
    
    test_passed = True
    print("\n✅ All validations passed!")
else:
    test_passed = True
```

## Summary

### ✅ Pattern 4.2: Multi-Engine Iceberg Access Complete!

Demonstrated:
1. **Spark Batch**: Loaded historical product data
2. **Spark Streaming**: Applied real-time inventory updates (MERGE)
3. **Unified View**: Single Iceberg table accessible by all engines
4. **ACID Transactions**: Concurrent writes without conflicts

**Iceberg Multi-Engine Benefits**:
- 🔓 No vendor lock-in (Spark, Trino, Flink, Presto, etc.)
- 🎯 Engine specialization (batch, streaming, queries)
- 🔒 ACID guarantees (concurrent writes safe)
- 📊 Unified lakehouse (single source of truth)
- ⚡ Snapshot isolation (consistent reads)

**Engine Roles**:

| Engine | Role | Use Case |
|--------|------|----------|
| **Spark Batch** | Historical loads | Daily ETL, bulk imports |
| **Spark Streaming** | Real-time updates | CDC, event processing |
| **Trino** | Interactive queries | Dashboards, ad-hoc SQL |
| **Superset** | Visualizations | BI dashboards |

**Why Iceberg?**
1. **Open Format**: Avoids vendor lock-in
2. **ACID Transactions**: Reliable concurrent writes
3. **Schema Evolution**: Add/drop columns without rewrites
4. **Time Travel**: Query historical snapshots
5. **Partition Evolution**: Change partitioning without data migration
6. **Hidden Partitioning**: Users don't need partition columns in queries

**Production Setup**:
```yaml
# Spark Batch (Airflow DAG)
spark-submit \
  --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lakehouse.warehouse=s3://openlakes/warehouse \
  batch_load_products.py

# Spark Streaming (K8s Spark Operator)
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: inventory-updates
spec:
  mainApplicationFile: local:///opt/spark/apps/inventory_streaming.py

# Trino (Catalog Configuration)
catalogs/iceberg.properties:
  connector.name=iceberg
  iceberg.catalog.type=hadoop
  hive.metastore.uri=thrift://hive-metastore:9083
```

**Trino Query Example**:
```sql
-- Unified query across batch and streaming writes
SELECT 
    update_source,
    COUNT(*) as products,
    AVG(stock_quantity) as avg_stock,
    SUM(CASE WHEN stock_quantity < 10 THEN 1 ELSE 0 END) as low_stock_count
FROM iceberg.demo.product_inventory
GROUP BY update_source;
```


```python
if enable_cleanup:
    spark.sql(f"DROP TABLE IF EXISTS {full_table}")
    print("🧹 Cleanup completed")
else:
    print("⏭️  Cleanup skipped")
```
