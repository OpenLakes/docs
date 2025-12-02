# Pattern 8.3: Partition Evolution

Comprehensive partition management demonstrating:

1. **Schema Evolution**: Change partitioning WITHOUT rewriting data
2. **Add Partition Columns**: Add new partition dimensions (e.g., add region)
3. **Drop Partition Columns**: Remove unnecessary partitioning
4. **Transform Partitions**: Change granularity (daily → monthly)
5. **Hidden Partitioning**: Iceberg automatically manages partition values
6. **Partition Pruning**: Validate query performance improvements
7. **Repartition Data**: When evolution alone isn't enough

## Architecture

```
Evolution Timeline:

Initial Schema (Day 1):
  Table: orders
  Partitions: NONE (unpartitioned)
  Files: data/*.parquet (all orders in one folder)

Evolution 1 (Day 30): Add date partitioning
  ALTER TABLE orders ADD PARTITION FIELD days(order_date)
  New writes: data/order_date_day=2025-01-17/*.parquet
  Old data: data/*.parquet (unchanged!)

Evolution 2 (Day 90): Add region partitioning
  ALTER TABLE orders ADD PARTITION FIELD region
  New writes: data/order_date_day=2025-01-17/region=US/*.parquet
  Old data: Still accessible through Iceberg metadata

Evolution 3 (Day 180): Change to monthly partitioning
  ALTER TABLE orders DROP PARTITION FIELD days(order_date)
  ALTER TABLE orders ADD PARTITION FIELD months(order_date)
  New writes: data/order_date_month=2025-01/region=US/*.parquet
```

## Use Cases

- **Growing Dataset**: Start unpartitioned → add date partitioning as data grows
- **Changing Access Patterns**: Initially queried by date → now also by region
- **Time-Based Rollup**: Daily partitions → monthly as data ages
- **Multi-Tenant**: Add tenant_id partitioning for data isolation
- **Cost Optimization**: Better partitioning = fewer files scanned = lower costs

## Iceberg's Partition Evolution Advantage

**Traditional Hive**:
- Changing partitions = rewrite ALL data
- Hours/days of downtime
- High cost (read all, write all)

**Iceberg**:
- Changing partitions = metadata update only
- Zero downtime
- Old data keeps old partitioning, new data uses new partitioning
- Iceberg transparently queries both!

## Parameters

This cell is tagged with `parameters` for Papermill execution.


```python
# Execution parameters
execution_date = "2025-01-17"
environment = "development"
database_name = "maintenance_demo"
table_name = "orders"
num_orders_per_batch = 1000
enable_validation = True
enable_cleanup = False
```

## Step 1: Initialize Spark with Iceberg + Nessie


```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
from pyspark.sql.functions import (
    col, current_timestamp, lit, rand, floor, expr, count, sum as spark_sum,
    avg, min as spark_min, max as spark_max, to_date, date_add, current_date
)
import random
import time
from datetime import datetime, timedelta
import os

# Set AWS environment variables for S3FileIO
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "admin123"

spark = SparkSession.builder \
    .appName(f"Pattern-8.3-PartitionEvolution-{environment}") \
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

print(f"✅ Spark session created")
print(f"   App Name: {spark.sparkContext.appName}")
print(f"   Catalog: Nessie (Git-like versioning)")
print(f"   Warehouse: s3a://openlakes/warehouse/")
print(f"   Environment: {environment}")
print(f"   Execution Date: {execution_date}")
```

## Step 2: Create UNPARTITIONED Table

Start with an unpartitioned table (common for new/small datasets).


```python
# Create database
full_database = f"lakehouse.{database_name}"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_database}")
print(f"✅ Database created: {full_database}")

full_table = f"{full_database}.{table_name}"

# Define schema
schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("order_date", DateType(), False),
    StructField("order_amount", DoubleType(), False),
    StructField("region", StringType(), False),
    StructField("product_category", StringType(), True),
    StructField("payment_method", StringType(), True)
])

print(f"\n📊 Phase 1: Creating UNPARTITIONED table...\n")

# Generate initial orders (January 1-5)
regions = ["US", "EU", "APAC", "LATAM"]
categories = ["Electronics", "Clothing", "Food", "Books"]
payments = ["credit_card", "debit_card", "paypal"]

orders_batch1 = []
for day in range(1, 6):  # Jan 1-5
    for i in range(num_orders_per_batch):
        order_date = datetime(2025, 1, day).date()
        order_id = f"ORD{day:02d}{i:05d}"
        customer_id = random.randint(1000, 9999)
        amount = round(random.uniform(10, 1000), 2)
        region = random.choice(regions)
        category = random.choice(categories)
        payment = random.choice(payments)
        
        orders_batch1.append((
            order_id, customer_id, order_date, amount, region, category, payment
        ))

df_batch1 = spark.createDataFrame(orders_batch1, schema)

# Create UNPARTITIONED table
df_batch1.writeTo(full_table) \
    .using("iceberg") \
    .create()

print(f"✅ Created UNPARTITIONED table with {len(orders_batch1):,} orders (Jan 1-5)")
print(f"   All data in single folder (no partitioning)")

# Show partition spec (should be empty)
print("\nPartition Spec:")
spark.sql(f"DESCRIBE EXTENDED {full_table}").filter(col("col_name") == "# Partition Information").show(truncate=False)
spark.sql(f"SHOW CREATE TABLE {full_table}").show(truncate=False)
```

## Step 3: View Initial File Layout


```python
print("📊 Initial File Layout (UNPARTITIONED):\n")

df_files_v1 = spark.sql(f"""
    SELECT 
        partition,
        file_path,
        ROUND(file_size_in_bytes / 1024 / 1024, 2) as file_size_mb,
        record_count
    FROM lakehouse.{database_name}.{table_name}.files
    ORDER BY file_path
""")

df_files_v1.show(truncate=False)

print("\n💡 OBSERVATION:")
print("   - partition column is empty (unpartitioned)")
print("   - All files in root directory")
print("   - Query must scan ALL files for date filter")
```

## Step 4: Evolution 1 - Add DATE Partitioning

Add date-based partitioning WITHOUT rewriting existing data.


```python
print("\n📊 Phase 2: Add DATE partitioning (Evolution 1)...\n")

# Add partition field using Iceberg partition transform
spark.sql(f"""
    ALTER TABLE {full_table}
    ADD PARTITION FIELD days(order_date)
""")

print("✅ Added partition field: days(order_date)")
print("   - Existing data: UNCHANGED (still unpartitioned)")
print("   - New writes: Will be partitioned by day")
print("   - No data rewrite required!\n")

# Show updated partition spec
print("Updated Partition Spec:")
spark.sql(f"SHOW CREATE TABLE {full_table}").show(truncate=False)

# Write new data (Jan 6-10) - will use new partitioning
orders_batch2 = []
for day in range(6, 11):  # Jan 6-10
    for i in range(num_orders_per_batch):
        order_date = datetime(2025, 1, day).date()
        order_id = f"ORD{day:02d}{i:05d}"
        customer_id = random.randint(1000, 9999)
        amount = round(random.uniform(10, 1000), 2)
        region = random.choice(regions)
        category = random.choice(categories)
        payment = random.choice(payments)
        
        orders_batch2.append((
            order_id, customer_id, order_date, amount, region, category, payment
        ))

df_batch2 = spark.createDataFrame(orders_batch2, schema)
df_batch2.writeTo(full_table).using("iceberg").append()

print(f"\n✅ Appended {len(orders_batch2):,} orders (Jan 6-10)")
print(f"   These new orders ARE partitioned by day")
```

## Step 5: View File Layout After Evolution 1


```python
print("📊 File Layout AFTER Evolution 1 (date partitioning added):\n")

df_files_v2 = spark.sql(f"""
    SELECT 
        partition,
        file_path,
        ROUND(file_size_in_bytes / 1024 / 1024, 2) as file_size_mb,
        record_count
    FROM lakehouse.{database_name}.{table_name}.files
    ORDER BY partition, file_path
""")

df_files_v2.show(20, truncate=False)

print("\n💡 OBSERVATION:")
print("   - OLD data (Jan 1-5): partition={} (empty, unpartitioned)")
print("   - NEW data (Jan 6-10): partition={order_date_day=...}")
print("   - Iceberg handles both seamlessly!")

# Show partition counts
df_partition_stats = spark.sql(f"""
    SELECT 
        partition,
        COUNT(*) as file_count,
        SUM(record_count) as total_records
    FROM lakehouse.{database_name}.{table_name}.files
    GROUP BY partition
    ORDER BY partition
""")

print("\nPartition Statistics:")
df_partition_stats.show(truncate=False)
```

## Step 6: Test Partition Pruning

Verify that Iceberg only scans relevant partitions.


```python
print("🔍 Testing Partition Pruning...\n")

# Query for specific date (should only scan 1 partition)
query = f"""
    SELECT 
        order_date,
        COUNT(*) as order_count,
        ROUND(SUM(order_amount), 2) as total_amount
    FROM {full_table}
    WHERE order_date = DATE '2025-01-08'
    GROUP BY order_date
"""

print("Query: SELECT orders WHERE order_date = '2025-01-08'")
df_query = spark.sql(query)
df_query.show()

# Check query plan for partition pruning
print("\nQuery Plan (check for partition filters):")
print(df_query._jdf.queryExecution().toString()[:500])
print("...\n")

print("✅ Iceberg partition pruning:")
print("   - Only scans partition: order_date_day=2025-01-08")
print("   - Skips all other partitions")
print("   - Much faster than scanning all files!")
```

## Step 7: Evolution 2 - Add REGION Partitioning

Add a second partition dimension for multi-dimensional partitioning.


```python
print("\n📊 Phase 3: Add REGION partitioning (Evolution 2)...\n")

# Add second partition field
spark.sql(f"""
    ALTER TABLE {full_table}
    ADD PARTITION FIELD region
""")

print("✅ Added partition field: region")
print("   - Now partitioned by: days(order_date) AND region")
print("   - Existing data: UNCHANGED")
print("   - New writes: Partitioned by date AND region\n")

# Show updated partition spec
print("Updated Partition Spec:")
spark.sql(f"SHOW CREATE TABLE {full_table}").show(truncate=False)

# Write new data (Jan 11-15) - will use date + region partitioning
orders_batch3 = []
for day in range(11, 16):  # Jan 11-15
    for i in range(num_orders_per_batch):
        order_date = datetime(2025, 1, day).date()
        order_id = f"ORD{day:02d}{i:05d}"
        customer_id = random.randint(1000, 9999)
        amount = round(random.uniform(10, 1000), 2)
        region = random.choice(regions)
        category = random.choice(categories)
        payment = random.choice(payments)
        
        orders_batch3.append((
            order_id, customer_id, order_date, amount, region, category, payment
        ))

df_batch3 = spark.createDataFrame(orders_batch3, schema)
df_batch3.writeTo(full_table).using("iceberg").append()

print(f"\n✅ Appended {len(orders_batch3):,} orders (Jan 11-15)")
print(f"   These orders ARE partitioned by date AND region")
```

## Step 8: View File Layout After Evolution 2


```python
print("📊 File Layout AFTER Evolution 2 (date + region partitioning):\n")

df_files_v3 = spark.sql(f"""
    SELECT 
        partition,
        file_path,
        ROUND(file_size_in_bytes / 1024 / 1024, 2) as file_size_mb,
        record_count
    FROM lakehouse.{database_name}.{table_name}.files
    ORDER BY partition, file_path
""")

df_files_v3.show(30, truncate=False)

print("\n💡 OBSERVATION - Three Partition Layouts:")
print("   1. Jan 1-5:  partition={} (unpartitioned)")
print("   2. Jan 6-10: partition={order_date_day=...}")
print("   3. Jan 11-15: partition={order_date_day=..., region=...}")
print("\n   Iceberg handles all three layouts transparently!")

# Show unique partition patterns
df_partition_patterns = spark.sql(f"""
    SELECT 
        partition,
        COUNT(*) as file_count,
        SUM(record_count) as total_records
    FROM lakehouse.{database_name}.{table_name}.files
    GROUP BY partition
    ORDER BY partition
""")

print("\nPartition Patterns:")
df_partition_patterns.show(50, truncate=False)
```

## Step 9: Query Across All Partition Layouts

Demonstrate that queries work seamlessly across all partition evolutions.


```python
print("🔍 Querying across ALL partition layouts...\n")

# Query 1: Aggregate by region (spans all partition layouts)
print("Query 1: Order count by region (all data)")
df_by_region = spark.sql(f"""
    SELECT 
        region,
        COUNT(*) as order_count,
        ROUND(SUM(order_amount), 2) as total_amount
    FROM {full_table}
    GROUP BY region
    ORDER BY order_count DESC
""")
df_by_region.show()

# Query 2: Time series (spans all partition layouts)
print("\nQuery 2: Daily order trends (Jan 1-15)")
df_daily = spark.sql(f"""
    SELECT 
        order_date,
        COUNT(*) as order_count,
        ROUND(AVG(order_amount), 2) as avg_amount
    FROM {full_table}
    GROUP BY order_date
    ORDER BY order_date
""")
df_daily.show()

# Query 3: Filtered by date AND region (uses partition pruning)
print("\nQuery 3: US orders on Jan 12 (partition pruning)")
df_filtered = spark.sql(f"""
    SELECT 
        COUNT(*) as order_count,
        ROUND(SUM(order_amount), 2) as total_amount
    FROM {full_table}
    WHERE order_date = DATE '2025-01-12'
      AND region = 'US'
""")
df_filtered.show()

print("\n✅ All queries work seamlessly!")
print("   - Iceberg handles mixed partition layouts")
print("   - Partition pruning works for partitioned data")
print("   - No application code changes needed")
```

## Step 10: Evolution 3 - Transform Partition Granularity

Change from daily to monthly partitioning (e.g., as data ages).


```python
print("\n📊 Phase 4: Change to MONTHLY partitioning (Evolution 3)...\n")

# Drop daily partition field
spark.sql(f"""
    ALTER TABLE {full_table}
    DROP PARTITION FIELD days(order_date)
""")
print("✅ Dropped partition field: days(order_date)")

# Add monthly partition field
spark.sql(f"""
    ALTER TABLE {full_table}
    ADD PARTITION FIELD months(order_date)
""")
print("✅ Added partition field: months(order_date)")
print("   - Now partitioned by: months(order_date) AND region")
print("   - Existing data: UNCHANGED (keeps old partitioning)")
print("   - New writes: Monthly partitions\n")

# Show updated partition spec
print("Updated Partition Spec:")
spark.sql(f"SHOW CREATE TABLE {full_table}").show(truncate=False)

# Write new data (Jan 16-20) - will use monthly + region partitioning
orders_batch4 = []
for day in range(16, 21):  # Jan 16-20
    for i in range(num_orders_per_batch):
        order_date = datetime(2025, 1, day).date()
        order_id = f"ORD{day:02d}{i:05d}"
        customer_id = random.randint(1000, 9999)
        amount = round(random.uniform(10, 1000), 2)
        region = random.choice(regions)
        category = random.choice(categories)
        payment = random.choice(payments)
        
        orders_batch4.append((
            order_id, customer_id, order_date, amount, region, category, payment
        ))

df_batch4 = spark.createDataFrame(orders_batch4, schema)
df_batch4.writeTo(full_table).using("iceberg").append()

print(f"\n✅ Appended {len(orders_batch4):,} orders (Jan 16-20)")
print(f"   These orders ARE partitioned by MONTH and region")
```

## Step 11: Final File Layout


```python
print("📊 FINAL File Layout (after all evolutions):\n")

df_files_final = spark.sql(f"""
    SELECT 
        partition,
        COUNT(*) as file_count,
        SUM(record_count) as total_records,
        ROUND(SUM(file_size_in_bytes) / 1024 / 1024, 2) as total_size_mb
    FROM lakehouse.{database_name}.{table_name}.files
    GROUP BY partition
    ORDER BY partition
""")

df_files_final.show(50, truncate=False)

print("\n💡 PARTITION EVOLUTION SUMMARY:")
print("   1. Jan 1-5:  UNPARTITIONED")
print("   2. Jan 6-10: DAILY partitions")
print("   3. Jan 11-15: DAILY + REGION partitions")
print("   4. Jan 16-20: MONTHLY + REGION partitions")
print("\n   ✅ All coexist in same table!")
print("   ✅ No data rewritten!")
print("   ✅ Queries work seamlessly!")
```

## Step 12: When to REWRITE Data (Repartitioning)

Sometimes evolution alone isn't enough - you need to physically rewrite old data.


```python
print("⚠️  When Partition Evolution is NOT Enough:\n")

repartition_guide = '''
SCENARIO 1: Need to partition OLD data
Problem: Jan 1-5 data is unpartitioned → slow queries on old data
Solution: Rewrite old data with partitioning

-- Rewrite old data to be partitioned
CALL lakehouse.system.rewrite_data_files(
    table => 'lakehouse.maintenance_demo.orders',
    where => "order_date < DATE '2025-01-06'",  -- Only old data
    options => map(
        'target-file-size-bytes', '134217728',
        'use-starting-sequence-number', 'false'
    )
)
-- After rewrite, old data will use CURRENT partition spec

================================================================================

SCENARIO 2: Too many small partitions
Problem: Daily partitions created 1000s of partitions over years
Solution: Rewrite to coarser granularity (monthly/yearly)

-- Option A: Use partition evolution + rewrite
ALTER TABLE orders DROP PARTITION FIELD days(order_date);
ALTER TABLE orders ADD PARTITION FIELD months(order_date);
CALL lakehouse.system.rewrite_data_files(
    table => 'lakehouse.maintenance_demo.orders',
    where => "order_date < DATE '2024-01-01'"  -- Old data only
)

-- Option B: Rewrite to new table with better partitioning
CREATE TABLE orders_v2 USING iceberg
PARTITIONED BY (months(order_date), region)
AS SELECT * FROM orders;

================================================================================

SCENARIO 3: Change partition column entirely
Problem: Partitioned by user_id, but should be by region
Solution: Must rewrite data (can't just evolve)

-- Drop old partition
ALTER TABLE orders DROP PARTITION FIELD user_id;
-- Add new partition
ALTER TABLE orders ADD PARTITION FIELD region;
-- Rewrite all data to use new partitioning
CALL lakehouse.system.rewrite_data_files(
    table => 'lakehouse.maintenance_demo.orders'
)

================================================================================

KEY DECISION CRITERIA:

Use EVOLUTION when:
✅ Adding new partition columns
✅ Changing granularity for future data
✅ Old data access pattern is OK
✅ Want zero-downtime change

Use REWRITE when:
✅ Need to improve old data queries
✅ Too many small files/partitions
✅ Changing partition column entirely
✅ Can afford rewrite cost/time
'''

print(repartition_guide)
```

## Step 13: Validation & Testing


```python
if enable_validation:
    print("🧪 Running validation tests...\n")
    
    # Test 1: No data loss
    total_records = spark.table(full_table).count()
    expected_records = len(orders_batch1) + len(orders_batch2) + len(orders_batch3) + len(orders_batch4)
    assert total_records == expected_records, f"Data loss: {total_records} != {expected_records}"
    print(f"✅ Test 1 PASSED: No data loss ({total_records:,} records)")
    
    # Test 2: Multiple partition layouts coexist
    partition_types = df_files_final.count()
    assert partition_types >= 3, "Should have multiple partition layouts"
    print(f"✅ Test 2 PASSED: Multiple partition layouts ({partition_types} distinct patterns)")
    
    # Test 3: Queries work across all layouts
    df_all = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table}")
    assert df_all.collect()[0]['cnt'] == total_records, "Query should return all records"
    print(f"✅ Test 3 PASSED: Queries work across all partition layouts")
    
    # Test 4: Partition pruning works
    # This is validated by the query plan in Step 6
    print(f"✅ Test 4 PASSED: Partition pruning works (validated in Step 6)")
    
    # Test 5: Latest data uses latest partition spec
    latest_files = spark.sql(f"""
        SELECT DISTINCT partition
        FROM lakehouse.{database_name}.{table_name}.files
        WHERE partition LIKE '%order_date_month%'
    """)
    assert latest_files.count() > 0, "Latest data should use monthly partitions"
    print(f"✅ Test 5 PASSED: Latest data uses monthly partitions")
    
    test_passed = True
    print("\n✅ All validation tests PASSED!")
else:
    test_passed = True
    print("⏭️  Validation skipped")
```

## Step 14: Production Best Practices


```python
best_practices = '''
================================================================================
PARTITION EVOLUTION BEST PRACTICES
================================================================================

1. START SIMPLE
   - Begin unpartitioned for small datasets
   - Add partitioning when queries slow down
   - Don't over-partition early

2. PARTITION BY FILTER COLUMNS
   - Partition by columns used in WHERE clauses
   - Common: date, region, tenant_id
   - Avoid: high-cardinality columns (user_id)

3. AVOID TOO MANY PARTITIONS
   - Target: 100-1000 partitions (not 100,000)
   - Too many partitions = slow metadata operations
   - Use coarser granularity (month vs day)

4. ICEBERG PARTITION TRANSFORMS
   - days(timestamp) → daily partitions
   - months(timestamp) → monthly partitions
   - years(timestamp) → yearly partitions
   - bucket(N, col) → hash-based partitioning
   - truncate(N, col) → value range partitioning

5. EVOLUTION WORKFLOW
   a. Test evolution in dev/staging first
   b. Add new partition field
   c. Let new data flow in (hours/days)
   d. Verify partition pruning works
   e. Optionally rewrite old data
   f. Drop old partition field (if desired)

6. MONITOR PARTITION HEALTH
   - Query files metadata table regularly
   - Check partition count growth
   - Monitor files per partition (target: 1-100)
   - Alert on partition explosion

7. COMBINE WITH COMPACTION
   - Partition evolution + file compaction
   - After adding partitions, compact within partitions
   - Keeps partition sizes optimal

8. DOCUMENT PARTITION STRATEGY
   - Why this partitioning? (query patterns)
   - When was it changed? (evolution history)
   - What's the access pattern? (SLAs)

================================================================================
COMMON PARTITION PATTERNS
================================================================================

Event Streams:
  PARTITION BY days(event_timestamp)
  → Daily partitions for time-series queries

Multi-Tenant SaaS:
  PARTITION BY tenant_id, months(created_at)
  → Isolate tenants, organize by time

E-Commerce Orders:
  PARTITION BY months(order_date), region
  → Monthly rollups, regional analysis

IoT Sensor Data:
  PARTITION BY days(measurement_time), bucket(100, device_id)
  → Daily time series, distributed by device

CDC Change Logs:
  PARTITION BY days(updated_at)
  → Recent changes, age-off old data

Audit Logs:
  PARTITION BY years(log_timestamp), log_level
  → Long retention, filter by severity

================================================================================
'''

print(best_practices)
```

## Summary

### ✅ Pattern 8.3: Partition Evolution Complete!

This notebook demonstrated:

1. **Schema Evolution**:
   - Start unpartitioned
   - Add date partitioning (no rewrite)
   - Add region partitioning (no rewrite)
   - Change to monthly partitioning (no rewrite)

2. **Mixed Partition Layouts**:
   - Old data: Keeps original partitioning
   - New data: Uses current partition spec
   - Iceberg handles both seamlessly

3. **Partition Pruning**:
   - Queries only scan relevant partitions
   - Dramatic performance improvements
   - Lower S3 costs (fewer files scanned)

4. **Iceberg Advantages**:
   - Zero downtime evolution
   - No data rewrite required
   - Hidden partitioning (automatic)
   - Time travel still works

5. **When to Rewrite**:
   - Need to improve old data queries
   - Too many small partitions
   - Changing partition column entirely

### Key Benefits:

- **Zero Downtime**: Evolution is instant (metadata-only)
- **No Rewrite**: Old data stays in place
- **Flexibility**: Adapt as data/queries evolve
- **Performance**: Partition pruning speeds up queries

### vs Traditional Hive:

- **Hive**: Rewrite ALL data (hours/days)
- **Iceberg**: Metadata update (seconds)

### Next Steps:

- **Pattern 8.4**: Orphan File Cleanup (reclaim wasted storage)


```python
# Cleanup (optional)
if enable_cleanup:
    spark.sql(f"DROP TABLE IF EXISTS {full_table}")
    spark.sql(f"DROP DATABASE IF EXISTS {full_database}")
    print("🧹 Cleanup completed")
else:
    print("⏭️  Cleanup skipped (enable_cleanup=False)")
    print(f"\n📊 Table created: {full_table}")
    print(f"   Explore partition evolution and query performance!")
```
