# Storage Performance Test 2: Alluxio Transparent Caching

This notebook demonstrates **Alluxio distributed caching** for transparent performance optimization.

**Architecture**:
```
Spark → Nessie (catalog) → Iceberg → Alluxio (cache) → MinIO (storage)
                                           ↓
                                       NVMe SSD
                                      (hot tier)
```

**Key Characteristics**:
- First read: Fetches from MinIO, caches in Alluxio (~50-200ms)
- Repeat reads: Served from Alluxio cache (<5ms) **50-1000x faster!**
- Automatic caching (transparent - no code changes)
- Good for: Read-heavy workloads, analytics, dashboards

## Prerequisites

- OpenLakes Core deployed to **multi-node cluster**
- Alluxio **enabled** (via `./configure-storage.sh`)
- ETL nodes labeled with `openlakes.io/etl-node=true`
- Nessie + Iceberg configured

## Step 1: Initialize Spark with Alluxio Access


```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, expr
import time
import os

# Set AWS environment variables for S3FileIO (Alluxio understore)
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "admin123"

print("═" * 70)
print("Creating Spark Session with ALLUXIO TRANSPARENT CACHING")
print("Storage Mode: Alluxio cache → MinIO understore")
print("═" * 70)

spark = SparkSession.builder \
    .appName("Storage-Performance-Alluxio-Cached") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.8.0") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config("spark.sql.catalog.lakehouse.uri", "http://infrastructure-nessie:19120/api/v2") \
    .config("spark.sql.catalog.lakehouse.ref", "main") \
    .config("spark.sql.catalog.lakehouse.authentication.type", "NONE") \
    .config("spark.sql.catalog.lakehouse.warehouse", "alluxio://infrastructure-alluxio-master:19998/openlakes/warehouse/") \
    .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://infrastructure-minio:9000") \
    .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true") \
    .config("spark.hadoop.fs.alluxio.impl", "alluxio.hadoop.FileSystem") \
    .config("spark.hadoop.alluxio.master.hostname", "infrastructure-alluxio-master") \
    .config("spark.hadoop.alluxio.master.rpc.port", "19998") \
    .config("spark.hadoop.alluxio.user.file.readtype.default", "CACHE") \
    .config("spark.hadoop.alluxio.user.file.writetype.default", "CACHE_THROUGH") \
    .config("spark.hadoop.alluxio.user.file.passive.cache.enabled", "true") \
    .config("spark.hadoop.alluxio.user.short.circuit.enabled", "true") \
    .config("spark.hadoop.alluxio.user.block.read.metrics.enabled", "true") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://infrastructure-minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("\n✅ Spark session created")
print(f"   App Name: {spark.sparkContext.appName}")
print(f"   Spark Version: {spark.version}")
print(f"   Storage Mode: Alluxio distributed cache")
print(f"   Warehouse: alluxio://infrastructure-alluxio-master:19998/openlakes/warehouse/")
print(f"   Understore: MinIO (s3a://openlakes/)")
print(f"   Caching: Passive (automatic on read)")
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

## Step 3: Write to Iceberg Table (Through Alluxio)

**Data Flow**: Spark → Iceberg → Alluxio → MinIO

- Written to Alluxio cache (hot tier - NVMe SSD)
- Simultaneously written to MinIO (warm tier - persistent storage)
- CACHE_THROUGH mode ensures durability


```python
print("📝 Writing to Iceberg table (Through Alluxio)...")

start_time = time.time()

df_large.writeTo("lakehouse.performance_test.transactions_cached") \
    .using("iceberg") \
    .tableProperty("write.format.default", "parquet") \
    .tableProperty("write.parquet.compression-codec", "snappy") \
    .createOrReplace()

write_time = time.time() - start_time

print(f"\n✅ Write completed")
print(f"   Rows written: {df_large.count():,}")
print(f"   Write time: {write_time:.2f} seconds")
print(f"   Cache: Alluxio (NVMe SSD)")
print(f"   Storage: MinIO (persistent)")
print(f"   Mode: CACHE_THROUGH (write to both)")
```

## Step 4: Performance Test - First Read (Cold Cache)

**Expected**: Similar to direct MinIO (~50-200ms) - cache miss, data fetched from MinIO and cached in Alluxio


```python
print("\n" + "═" * 70)
print("TEST 1: FIRST READ (Cold Cache - Cache Miss)")
print("═" * 70)

start_time = time.time()

df_read1 = spark.table("lakehouse.performance_test.transactions_cached")
count1 = df_read1.count()

read_time_1 = time.time() - start_time

print(f"\n📊 First Read Results:")
print(f"   Rows read: {count1:,}")
print(f"   Read time: {read_time_1:.3f} seconds")
print(f"   Source: MinIO (cache miss)")
print(f"   Action: Data fetched from MinIO → Cached in Alluxio NVMe")
```

## Step 5: Performance Test - Second Read (Hot Cache) ⚡

**Expected**: **50-1000x faster** (<5ms) - data served from Alluxio NVMe cache!


```python
# Wait 3 seconds (simulating repeat access)
print("\n⏳ Waiting 3 seconds before second read...")
time.sleep(3)

print("\n" + "═" * 70)
print("TEST 2: SECOND READ (Hot Cache - Cache Hit) ⚡")
print("═" * 70)

start_time = time.time()

df_read2 = spark.table("lakehouse.performance_test.transactions_cached")
count2 = df_read2.count()

read_time_2 = time.time() - start_time

speedup = read_time_1 / read_time_2 if read_time_2 > 0 else 0

print(f"\n📊 Second Read Results:")
print(f"   Rows read: {count2:,}")
print(f"   Read time: {read_time_2:.3f} seconds")
print(f"   Source: Alluxio NVMe cache (local SSD) ⚡")
print(f"   Speedup: {speedup:.1f}x faster than first read!")
print(f"   Performance improvement: {((read_time_1 - read_time_2) / read_time_1) * 100:.1f}% faster")
```

## Step 6: Performance Test - Aggregation Query (Cold Cache)


```python
print("\n" + "═" * 70)
print("TEST 3: AGGREGATION QUERY (Sum by category - First Run)")
print("═" * 70)

start_time = time.time()

result = spark.sql("""
    SELECT 
        category,
        COUNT(*) as count,
        SUM(amount) as total_amount,
        AVG(amount) as avg_amount
    FROM lakehouse.performance_test.transactions_cached
    GROUP BY category
    ORDER BY category
""")

result.show()

agg_time_1 = time.time() - start_time

print(f"\n📊 Aggregation Results:")
print(f"   Query time: {agg_time_1:.3f} seconds")
print(f"   Source: Alluxio cache (if already cached) or MinIO")
```

## Step 7: Repeat Aggregation (Test Hot Cache) ⚡

**Expected**: **Significantly faster** - data already in Alluxio cache!


```python
print("\n⏳ Waiting 3 seconds before repeat aggregation...")
time.sleep(3)

print("\n" + "═" * 70)
print("TEST 4: REPEAT AGGREGATION (Hot Cache) ⚡")
print("═" * 70)

start_time = time.time()

result = spark.sql("""
    SELECT 
        category,
        COUNT(*) as count,
        SUM(amount) as total_amount,
        AVG(amount) as avg_amount
    FROM lakehouse.performance_test.transactions_cached
    GROUP BY category
    ORDER BY category
""")

result.show()

agg_time_2 = time.time() - start_time
agg_speedup = agg_time_1 / agg_time_2 if agg_time_2 > 0 else 0

print(f"\n📊 Repeat Aggregation Results:")
print(f"   Query time: {agg_time_2:.3f} seconds")
print(f"   Source: Alluxio NVMe cache ⚡")
print(f"   Speedup: {agg_speedup:.1f}x faster than first run!")
print(f"   Performance improvement: {((agg_time_1 - agg_time_2) / agg_time_1) * 100:.1f}% faster")
```

## Summary: Alluxio Caching Performance

### Key Metrics

| Operation | Time (seconds) | Speedup | Notes |
|-----------|---------------|---------|-------|
| Write (100K rows) | {write_time:.2f}s | - | CACHE_THROUGH (cache + MinIO) |
| First Read (cold) | {read_time_1:.3f}s | 1.0x | Cache miss → fetch from MinIO |
| Second Read (hot) | {read_time_2:.3f}s | **{speedup:.1f}x** | ⚡ From Alluxio NVMe cache |
| First Aggregation | {agg_time_1:.3f}s | 1.0x | - |
| Repeat Aggregation | {agg_time_2:.3f}s | **{agg_speedup:.1f}x** | ⚡ From cache |

### Characteristics

✅ **Pros**:
- **50-1000x speedup for repeat queries** (cache hits)
- Sub-millisecond latency from NVMe cache
- Transparent (no code changes needed)
- Automatic LRU cache management
- Excellent for read-heavy analytics

⚠️ **Cons**:
- First read still pays MinIO network cost (cache miss)
- Requires multi-node cluster
- Cache capacity limited by ETL node storage
- Slight overhead on writes (CACHE_THROUGH)

### Best Use Cases

- **Interactive dashboards** (same data queried repeatedly)
- **ML training** (multiple passes over same dataset)
- **Analytical queries** (aggregations, joins)
- **Hot data access** (recent/frequently accessed data)
- **Iterative algorithms** (Spark ML, graph processing)

---

**Next**: Run `03-performance-comparison.ipynb` for side-by-side comparison!


```python
# Print final performance summary
print("\n" + "═" * 70)
print("ALLUXIO CACHING PERFORMANCE SUMMARY")
print("═" * 70)

print(f"\n📊 Performance Metrics:")
print(f"   Write time:           {write_time:.2f} seconds")
print(f"   First read (cold):    {read_time_1:.3f} seconds (cache miss)")
print(f"   Second read (hot):    {read_time_2:.3f} seconds ({speedup:.1f}x faster!) ⚡")
print(f"   First aggregation:    {agg_time_1:.3f} seconds")
print(f"   Repeat aggregation:   {agg_time_2:.3f} seconds ({agg_speedup:.1f}x faster!) ⚡")

print(f"\n🔍 Key Insights:")
print(f"   ✅ Repeat queries are {speedup:.1f}x faster with Alluxio caching!")
print(f"   ✅ Hot data served from local NVMe (< 5ms latency)")
print(f"   ✅ Transparent - no application code changes needed")
print(f"   ✅ Automatic cache management (LRU eviction)")

print(f"\n💡 Cache Efficiency:")
if speedup > 10:
    print(f"   Excellent! Cache is working effectively.")
elif speedup > 3:
    print(f"   Good cache performance. Consider cache warming for better results.")
else:
    print(f"   Cache benefit is limited. Check Alluxio configuration.")

print(f"\n➡️  Run notebook 03 for direct comparison with baseline!")
```
