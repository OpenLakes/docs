# Storage Performance Test 1: Direct MinIO Access (Baseline)

This notebook demonstrates **baseline performance** using direct MinIO S3A access without any caching layer.

**Architecture**:
```
Spark → Nessie (catalog) → Iceberg → Direct MinIO (s3a://)
```

**Key Characteristics**:
- Every read goes to MinIO (no caching)
- Consistent latency (~50-200ms per read)
- Repeat queries have same performance as first query
- Good for: Write-heavy workloads, one-time scans

## Prerequisites

- OpenLakes Core deployed
- Alluxio **disabled** (default configuration)
- Nessie + Iceberg configured

## Step 1: Initialize Spark with Direct MinIO Access


```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, expr
import time
import os

# Set AWS environment variables for S3FileIO
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "admin123"

print("═" * 70)
print("Creating Spark Session with DIRECT MINIO ACCESS")
print("Storage Mode: No caching - every read goes to MinIO")
print("═" * 70)

spark = SparkSession.builder \
    .appName("Storage-Performance-Direct-MinIO") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.8.0") \
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

print("\n✅ Spark session created")
print(f"   App Name: {spark.sparkContext.appName}")
print(f"   Spark Version: {spark.version}")
print(f"   Storage Mode: Direct MinIO (s3a://)")
print(f"   Warehouse: s3a://openlakes/warehouse/")
```

## Step 2: Create Database and Test Data


```python
# Create database
spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.performance_test")
print("✅ Database created: lakehouse.performance_test")

# Generate 100,000 rows of test data (~10MB)
print("\n📊 Generating test data (100,000 rows)...")

df_large = spark.range(0, 100000).select(
    col("id"),
    (rand() * 1000).cast("int").alias("product_id"),
    (rand() * 100).alias("amount"),
    expr("concat('category_', cast(rand() * 10 as int))").alias("category"),
    expr("concat('user_', cast(rand() * 5000 as int))").alias("user_id"),
    expr("date_sub(current_date(), cast(rand() * 365 as int))").alias("date")
)

print(f"✅ Generated {df_large.count():,} rows of test data")
print("\nSample data:")
df_large.show(5)
```

## Step 3: Write to Iceberg Table (Direct MinIO)

**Data Flow**: Spark → Iceberg → MinIO

No caching layer - data written directly to MinIO S3 storage.


```python
print("📝 Writing to Iceberg table (Direct MinIO)...")

start_time = time.time()

df_large.writeTo("lakehouse.performance_test.transactions_direct") \
    .using("iceberg") \
    .tableProperty("write.format.default", "parquet") \
    .tableProperty("write.parquet.compression-codec", "snappy") \
    .createOrReplace()

write_time = time.time() - start_time

print(f"\n✅ Write completed")
print(f"   Rows written: {df_large.count():,}")
print(f"   Write time: {write_time:.2f} seconds")
print(f"   Storage: Direct MinIO (s3a://openlakes/warehouse/)")
```

## Step 4: Performance Test - First Read

**Expected**: Moderate performance (~50-200ms) reading from MinIO network storage


```python
print("\n" + "═" * 70)
print("TEST 1: FIRST READ (Direct MinIO)")
print("═" * 70)

start_time = time.time()

df_read1 = spark.table("lakehouse.performance_test.transactions_direct")
count1 = df_read1.count()

read_time_1 = time.time() - start_time

print(f"\n📊 First Read Results:")
print(f"   Rows read: {count1:,}")
print(f"   Read time: {read_time_1:.3f} seconds")
print(f"   Source: MinIO network storage")
print(f"   Caching: None (direct S3A access)")
```

## Step 5: Performance Test - Second Read (3 seconds later)

**Expected**: **Same performance** as first read - no caching, data still fetched from MinIO


```python
# Wait 3 seconds (simulating repeat access)
print("\n⏳ Waiting 3 seconds before second read...")
time.sleep(3)

print("\n" + "═" * 70)
print("TEST 2: SECOND READ (3 seconds later)")
print("═" * 70)

start_time = time.time()

df_read2 = spark.table("lakehouse.performance_test.transactions_direct")
count2 = df_read2.count()

read_time_2 = time.time() - start_time

print(f"\n📊 Second Read Results:")
print(f"   Rows read: {count2:,}")
print(f"   Read time: {read_time_2:.3f} seconds")
print(f"   Source: MinIO network storage (no cache)")
print(f"   Performance change: {((read_time_2 / read_time_1) - 1) * 100:+.1f}%")
```

## Step 6: Performance Test - Aggregation Query

Test read performance for analytical queries


```python
print("\n" + "═" * 70)
print("TEST 3: AGGREGATION QUERY (Sum by category)")
print("═" * 70)

start_time = time.time()

result = spark.sql("""
    SELECT 
        category,
        COUNT(*) as count,
        SUM(amount) as total_amount,
        AVG(amount) as avg_amount
    FROM lakehouse.performance_test.transactions_direct
    GROUP BY category
    ORDER BY category
""")

result.show()

agg_time_1 = time.time() - start_time

print(f"\n📊 Aggregation Results:")
print(f"   Query time: {agg_time_1:.3f} seconds")
print(f"   Source: MinIO (full table scan)")
```

## Step 7: Repeat Aggregation (Test Cache Impact)

**Expected**: **Same performance** - no caching layer, must re-scan from MinIO


```python
print("\n⏳ Waiting 3 seconds before repeat aggregation...")
time.sleep(3)

print("\n" + "═" * 70)
print("TEST 4: REPEAT AGGREGATION (3 seconds later)")
print("═" * 70)

start_time = time.time()

result = spark.sql("""
    SELECT 
        category,
        COUNT(*) as count,
        SUM(amount) as total_amount,
        AVG(amount) as avg_amount
    FROM lakehouse.performance_test.transactions_direct
    GROUP BY category
    ORDER BY category
""")

result.show()

agg_time_2 = time.time() - start_time

print(f"\n📊 Repeat Aggregation Results:")
print(f"   Query time: {agg_time_2:.3f} seconds")
print(f"   Source: MinIO (full table scan again)")
print(f"   Performance change: {((agg_time_2 / agg_time_1) - 1) * 100:+.1f}%")
```

## Summary: Direct MinIO Performance

### Key Metrics

| Operation | Time (seconds) | Notes |
|-----------|---------------|-------|
| Write (100K rows) | {write_time:.2f}s | Direct to MinIO |
| First Read | {read_time_1:.3f}s | From MinIO network storage |
| Second Read | {read_time_2:.3f}s | From MinIO (no cache benefit) |
| First Aggregation | {agg_time_1:.3f}s | Full table scan from MinIO |
| Repeat Aggregation | {agg_time_2:.3f}s | Full table scan again |

### Characteristics

✅ **Pros**:
- Simple architecture (no caching complexity)
- Predictable performance
- Good for write-heavy workloads
- No cache invalidation concerns

❌ **Cons**:
- **No speedup for repeat queries** (every read goes to MinIO)
- Network latency on every access
- Slower for frequently accessed data
- No benefit from local NVMe/SSD storage

### Best Use Cases

- **One-time ETL jobs** (data scanned once)
- **Write-heavy pipelines** (more writes than reads)
- **Archival queries** (rarely accessed historical data)
- **Sequential processing** (each dataset read once)

---

**Next**: Run `02-iceberg-alluxio-tiering.ipynb` to see **50-1000x speedup** with Alluxio transparent caching!


```python
# Print final performance summary
print("\n" + "═" * 70)
print("DIRECT MINIO PERFORMANCE SUMMARY")
print("═" * 70)

print(f"\n📊 Performance Metrics:")
print(f"   Write time:           {write_time:.2f} seconds")
print(f"   First read:           {read_time_1:.3f} seconds")
print(f"   Second read:          {read_time_2:.3f} seconds ({((read_time_2 / read_time_1) - 1) * 100:+.1f}%)")
print(f"   First aggregation:    {agg_time_1:.3f} seconds")
print(f"   Repeat aggregation:   {agg_time_2:.3f} seconds ({((agg_time_2 / agg_time_1) - 1) * 100:+.1f}%)")

print(f"\n🔍 Key Insight:")
print(f"   Repeat queries show NO significant speedup (±10% variance is normal)")
print(f"   Every read goes to MinIO network storage")
print(f"\n➡️  Compare with notebook 02 to see Alluxio caching benefits!")
```
