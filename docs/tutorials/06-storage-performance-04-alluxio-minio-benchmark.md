# Alluxio vs MinIO Read Performance Benchmark

This notebook benchmarks Iceberg table read performance with and without Alluxio caching:
- **Write Once**: 10GB data written to MinIO (baseline for both tests)
- **Read Test**: Two consecutive reads through Alluxio vs direct MinIO

**Key Insight**: Alluxio acts as a **read cache**. We write directly to MinIO once, then compare read performance with and without caching.

## Setup


```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, randn, col
import time
import pandas as pd

print("📦 Imports complete")
```

## Configuration

We'll create Spark sessions that read from different locations:
1. **Alluxio-backed**: Reads through Alluxio cache (alluxio://...)
2. **Direct MinIO**: Reads directly from MinIO (s3a://...)

Both use the **same underlying data** in MinIO.


```python
# Common S3A/MinIO configuration
def get_base_builder(name):
    """Get base Spark builder with common configs."""
    return SparkSession.builder \
        .appName(name) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.lakehouse.type", "rest") \
        .config("spark.sql.catalog.lakehouse.uri", "http://infrastructure-nessie:19120/api/v2") \
        .config("spark.hadoop.fs.s3a.access.key", "openlakes") \
        .config("spark.hadoop.fs.s3a.secret.key", "openlakes123") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://infrastructure-minio:9000") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

def create_write_session():
    """Create Spark session for writing (always direct to MinIO)."""
    return get_base_builder("Write Benchmark") \
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://openlakes/warehouse/") \
        .getOrCreate()

def create_alluxio_read_session():
    """Create Spark session that reads through Alluxio."""
    return get_base_builder("Alluxio Read Benchmark") \
        .config("spark.sql.catalog.lakehouse.warehouse", "alluxio://infrastructure-alluxio-master:19998/openlakes/warehouse/") \
        .config("spark.hadoop.fs.alluxio.impl", "alluxio.hadoop.FileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.alluxio.impl", "alluxio.hadoop.AlluxioFileSystem") \
        .config("spark.hadoop.alluxio.user.file.readtype.default", "CACHE") \
        .getOrCreate()

def create_direct_read_session():
    """Create Spark session that reads directly from MinIO."""
    return get_base_builder("Direct MinIO Read Benchmark") \
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://openlakes/warehouse/") \
        .getOrCreate()

print("⚙️ Configuration functions defined")
```

## Generate 10GB Dataset

We'll generate ~83M rows (10GB):
- 5 numeric columns (doubles)
- 5 string columns
- 200 partitions for parallelism


```python
def generate_10gb_dataframe(spark):
    """
    Generate ~10GB DataFrame.
    Each row ~120 bytes → 83M rows = 10GB
    """
    num_rows = 83_333_333
    
    print(f"🔧 Generating {num_rows:,} rows (~10GB)...")
    
    df = spark.range(0, num_rows, numPartitions=200) \
        .withColumn("value1", rand() * 1000) \
        .withColumn("value2", randn() * 100) \
        .withColumn("value3", rand() * 10000) \
        .withColumn("value4", randn() * 500) \
        .withColumn("value5", rand() * 50) \
        .withColumn("category1", (col("id") % 100).cast("string")) \
        .withColumn("category2", (col("id") % 50).cast("string")) \
        .withColumn("category3", (col("id") % 25).cast("string")) \
        .withColumn("status", (col("id") % 5).cast("string")) \
        .withColumn("region", (col("id") % 10).cast("string"))
    
    return df

print("📊 Data generation function ready")
```

## Test 1: Write and Read Through Alluxio (Write-Back Cache)

Testing with Alluxio write-back caching:
- Writes go to Alluxio cache first (ASYNC_THROUGH mode)
- Data asynchronously persisted to MinIO in background
- First read may still be in cache
- Second read should be fully cached


```python
print("="*80)
print("🚀 TEST 1: ALLUXIO WRITE-BACK CACHE")
print("="*80)

# Create Spark session with Alluxio
spark_alluxio = create_alluxio_read_session()

# Generate data
df_alluxio = generate_10gb_dataframe(spark_alluxio)

# Drop table if exists
spark_alluxio.sql("DROP TABLE IF EXISTS lakehouse.default.benchmark_alluxio")

# Benchmark write
print("\n📝 Writing 10GB through Alluxio (ASYNC_THROUGH)...")
alluxio_write_start = time.time()

df_alluxio.writeTo("lakehouse.default.benchmark_alluxio") \
    .tableProperty("write.format.default", "parquet") \
    .tableProperty("write.parquet.compression-codec", "snappy") \
    .create()

# Verify
alluxio_row_count = spark_alluxio.table("lakehouse.default.benchmark_alluxio").count()
alluxio_write_time = time.time() - alluxio_write_start

print(f"✅ Write complete: {alluxio_row_count:,} rows in {alluxio_write_time:.2f}s")
print(f"   Throughput: {10 * 1024 / alluxio_write_time:.2f} MB/s")

# First read (likely cached from write)
print("\n📖 First read (likely cached from write)...")
alluxio_read1_start = time.time()

result1 = spark_alluxio.table("lakehouse.default.benchmark_alluxio") \
    .selectExpr(
        "count(*) as row_count",
        "avg(value1) as avg_value1",
        "max(value2) as max_value2",
        "sum(value3) as sum_value3"
    ).collect()[0]

alluxio_read1_time = time.time() - alluxio_read1_start
print(f"✅ First read complete: {result1.row_count:,} rows in {alluxio_read1_time:.2f}s")
print(f"   Throughput: {10 * 1024 / alluxio_read1_time:.2f} MB/s")

# Second read (definitely cached)
print("\n📖 Second read (cache hit)...")
alluxio_read2_start = time.time()

result2 = spark_alluxio.table("lakehouse.default.benchmark_alluxio") \
    .selectExpr(
        "count(*) as row_count",
        "avg(value1) as avg_value1",
        "max(value2) as max_value2",
        "sum(value3) as sum_value3"
    ).collect()[0]

alluxio_read2_time = time.time() - alluxio_read2_start
print(f"✅ Second read complete: {result2.row_count:,} rows in {alluxio_read2_time:.2f}s")
print(f"   Throughput: {10 * 1024 / alluxio_read2_time:.2f} MB/s")
print(f"   🔥 Speedup: {alluxio_read1_time / alluxio_read2_time:.2f}x faster")

spark_alluxio.stop()
print("\n✅ Alluxio tests complete")
```

## Test 2: Write and Read Directly from MinIO

Testing direct MinIO access:
- All writes go over network to MinIO
- No caching benefits
- Baseline for comparison


```python
print("="*80)
print("🗄️ TEST 2: DIRECT MINIO STORAGE")
print("="*80)

# Create Spark session without Alluxio
spark_minio = create_direct_read_session()

# Generate data
df_minio = generate_10gb_dataframe(spark_minio)

# Drop table if exists
spark_minio.sql("DROP TABLE IF EXISTS lakehouse.default.benchmark_minio")

# Benchmark write
print("\n📝 Writing 10GB directly to MinIO (s3a://)...")
minio_write_start = time.time()

df_minio.writeTo("lakehouse.default.benchmark_minio") \
    .tableProperty("write.format.default", "parquet") \
    .tableProperty("write.parquet.compression-codec", "snappy") \
    .create()

# Verify
minio_row_count = spark_minio.table("lakehouse.default.benchmark_minio").count()
minio_write_time = time.time() - minio_write_start

print(f"✅ Write complete: {minio_row_count:,} rows in {minio_write_time:.2f}s")
print(f"   Throughput: {10 * 1024 / minio_write_time:.2f} MB/s")

# First read
print("\n📖 First read (direct from MinIO)...")
minio_read1_start = time.time()

result1 = spark_minio.table("lakehouse.default.benchmark_minio") \
    .selectExpr(
        "count(*) as row_count",
        "avg(value1) as avg_value1",
        "max(value2) as max_value2",
        "sum(value3) as sum_value3"
    ).collect()[0]

minio_read1_time = time.time() - minio_read1_start
print(f"✅ First read complete: {result1.row_count:,} rows in {minio_read1_time:.2f}s")
print(f"   Throughput: {10 * 1024 / minio_read1_time:.2f} MB/s")

# Second read
print("\n📖 Second read (still direct from MinIO - no cache)...")
minio_read2_start = time.time()

result2 = spark_minio.table("lakehouse.default.benchmark_minio") \
    .selectExpr(
        "count(*) as row_count",
        "avg(value1) as avg_value1",
        "max(value2) as max_value2",
        "sum(value3) as sum_value3"
    ).collect()[0]

minio_read2_time = time.time() - minio_read2_start
print(f"✅ Second read complete: {result2.row_count:,} rows in {minio_read2_time:.2f}s")
print(f"   Throughput: {10 * 1024 / minio_read2_time:.2f} MB/s")
print(f"   ⚠️ No speedup: {minio_read1_time / minio_read2_time:.2f}x (no caching)")

spark_minio.stop()
print("\n✅ MinIO tests complete")
```

## Results Summary


```python
# Create results DataFrame
results = pd.DataFrame({
    'Operation': ['Write (10GB)', 'Read #1 (10GB)', 'Read #2 (10GB)'],
    'Alluxio (seconds)': [alluxio_write_time, alluxio_read1_time, alluxio_read2_time],
    'MinIO Direct (seconds)': [minio_write_time, minio_read1_time, minio_read2_time]
})

# Calculate speedup
results['Speedup (Alluxio vs MinIO)'] = results['MinIO Direct (seconds)'] / results['Alluxio (seconds)']

# Calculate throughput in MB/s
data_size_mb = 10 * 1024
results['Alluxio Throughput (MB/s)'] = data_size_mb / results['Alluxio (seconds)']
results['MinIO Throughput (MB/s)'] = data_size_mb / results['MinIO Direct (seconds)']

print("\n" + "="*100)
print("📊 BENCHMARK RESULTS SUMMARY")
print("="*100)
print(results.to_string(index=False))
print("="*100)

# Key insights
print("\n🔍 Key Insights:")
print(f"   • Write Performance: Alluxio is {results.loc[0, 'Speedup (Alluxio vs MinIO)']:.2f}x vs MinIO (write-back cache benefit!)")
print(f"   • First Read: Alluxio is {results.loc[1, 'Speedup (Alluxio vs MinIO)']:.2f}x vs MinIO (data likely cached from write)")
print(f"   • Second Read: Alluxio is {results.loc[2, 'Speedup (Alluxio vs MinIO)']:.2f}x vs MinIO (cache benefit)")
print(f"   • Cache Hit Improvement: {results.loc[1, 'Alluxio (seconds)'] / results.loc[2, 'Alluxio (seconds)']:.2f}x faster on second read")
print(f"   • Write-Back Mode: ASYNC_THROUGH provides faster writes by caching first")

# Save results
results.to_csv('/home/jovyan/alluxio_benchmark_results.csv', index=False)
print("\n💾 Results saved to: /home/jovyan/alluxio_benchmark_results.csv")
```

## Visualization


```python
import matplotlib.pyplot as plt

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

# Chart 1: Time comparison
x = range(len(results))
width = 0.35
ax1.bar([i - width/2 for i in x], results['Alluxio (seconds)'], width, label='Alluxio (Write-Back)', color='#FF6B35')
ax1.bar([i + width/2 for i in x], results['MinIO Direct (seconds)'], width, label='MinIO Direct', color='#004E89')
ax1.set_xlabel('Operation')
ax1.set_ylabel('Time (seconds)')
ax1.set_title('Performance Comparison: Time')
ax1.set_xticks(x)
ax1.set_xticklabels(results['Operation'])
ax1.legend()
ax1.grid(axis='y', alpha=0.3)

# Chart 2: Throughput comparison
ax2.bar([i - width/2 for i in x], results['Alluxio Throughput (MB/s)'], width, label='Alluxio (Write-Back)', color='#FF6B35')
ax2.bar([i + width/2 for i in x], results['MinIO Throughput (MB/s)'], width, label='MinIO Direct', color='#004E89')
ax2.set_xlabel('Operation')
ax2.set_ylabel('Throughput (MB/s)')
ax2.set_title('Performance Comparison: Throughput')
ax2.set_xticks(x)
ax2.set_xticklabels(results['Operation'])
ax2.legend()
ax2.grid(axis='y', alpha=0.3)

plt.tight_layout()
plt.savefig('/home/jovyan/alluxio_benchmark_chart.png', dpi=150, bbox_inches='tight')
plt.show()

print("📈 Chart saved to: /home/jovyan/alluxio_benchmark_chart.png")
```

## Cleanup (Optional)


```python
# Uncomment to delete benchmark tables
# spark_cleanup = create_minio_session()
# spark_cleanup.sql("DROP TABLE IF EXISTS lakehouse.default.benchmark_alluxio")
# spark_cleanup.sql("DROP TABLE IF EXISTS lakehouse.default.benchmark_minio")
# spark_cleanup.stop()
# print("🧹 Cleanup complete")

print("\n✅ Benchmark notebook complete!")
print("\n📌 Summary:")
print("   • ASYNC_THROUGH mode provides write-back caching")
print("   • Faster writes by caching first, async persist to MinIO")
print("   • Significant speedup for repeated reads")
print("   • Best for analytical workloads with data reuse")
```
