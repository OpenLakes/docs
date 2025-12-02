```python
from pyspark.sql.functions import col, sum as spark_sum, count, avg, max as spark_max, min as spark_min, when, expr, lit, window, current_timestamp, from_json
import random
from datetime import datetime, date, timedelta
```

# Pattern 1.6: Batch Aggregation Pipeline

Pre-compute summaries for fast analytics dashboards:

1. **Raw Transactions**: High-volume transactional data
2. **Aggregate**: Daily summaries with GroupBy
3. **Store Summaries**: Pre-computed analytics table
4. **Fast Queries**: Dashboard queries use summaries (100x-1000x faster)

## Problem vs Solution

**Problem**: Dashboard queries on billions of raw transactions are SLOW
```sql
-- Scans 1 billion rows every query!
SELECT product_id, SUM(amount) FROM transactions GROUP BY product_id
```

**Solution**: Pre-compute daily summaries (massive data reduction)
```sql
-- Scans only 1000 daily summaries!
SELECT product_id, SUM(daily_total) FROM daily_summaries GROUP BY product_id
```

**Result**: 100x-1000x faster queries, predictable performance

## Parameters


```python
execution_date = "2025-01-16"
environment = "development"
database_name = "demo"
raw_transactions_table = "sales_transactions_raw"
daily_summary_table = "sales_daily_summary"
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
    .appName(f"Pattern-1.6-BatchAgg-{environment}") \
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

print("✅ Spark initialized for batch aggregation")
```

## Raw Transactions: High-Volume Data


```python
full_database = f"lakehouse.{database_name}"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_database}")

# Generate 1000 raw transactions across 3 days
random.seed(42)
base_date = date(2025, 1, 14)

transactions = []
for day_offset in range(3):  # 3 days
    transaction_date = base_date + timedelta(days=day_offset)
    for _ in range(333):  # ~333 transactions per day
        product_id = random.choice([101, 102, 103])  # 3 products
        quantity = random.randint(1, 10)
        unit_price = random.choice([29.99, 49.99, 99.99])
        amount = quantity * unit_price
        transactions.append((transaction_date, product_id, quantity, unit_price, amount))

df_transactions = spark.createDataFrame(
    transactions,
    ["transaction_date", "product_id", "quantity", "unit_price", "amount"]
)

full_raw_table = f"{full_database}.{raw_transactions_table}"
df_transactions.writeTo(full_raw_table).using("iceberg").createOrReplace()

print(f"✅ Generated {df_transactions.count()} raw transactions")
print("\n📊 Sample transactions:")
df_transactions.show(10)
```

## Aggregate: Compute Daily Summaries


```python
# GroupBy aggregation: Reduce 1000 rows → ~9 daily summaries (3 days × 3 products)
df_daily_summary = df_transactions.groupBy("transaction_date", "product_id").agg(
    count("*").alias("transaction_count"),
    spark_sum("quantity").alias("total_quantity"),
    spark_sum("amount").alias("total_revenue"),
    avg("amount").alias("avg_transaction_value"),
    spark_max("amount").alias("max_transaction_value"),
    spark_min("amount").alias("min_transaction_value"),
)

print(f"✅ Aggregated {df_transactions.count()} transactions → {df_daily_summary.count()} daily summaries")
print(f"📊 Data reduction: {df_transactions.count() / df_daily_summary.count():.1f}x compression")
print("\n📊 Daily summaries:")
df_daily_summary.orderBy("transaction_date", "product_id").show()
```

## Store Summaries: Pre-Computed Analytics Table


```python
full_summary_table = f"{full_database}.{daily_summary_table}"
df_daily_summary.writeTo(full_summary_table).using("iceberg").createOrReplace()

print(f"✅ Stored daily summaries in {full_summary_table}")
print("🎉 Dashboards can now query pre-computed summaries instead of raw transactions!")
```

## Query Performance Comparison


```python
# Query 1: Total revenue per product (from RAW transactions)
df_from_raw = spark.sql(f"""
    SELECT 
        product_id,
        SUM(amount) as total_revenue
    FROM {full_raw_table}
    GROUP BY product_id
    ORDER BY product_id
""")

print("📊 Query 1: Total revenue per product (from RAW data):")
print(f"   Scanned {df_transactions.count()} raw transaction rows")
df_from_raw.show()

# Query 2: Total revenue per product (from SUMMARY - MUCH FASTER!)
df_from_summary = spark.sql(f"""
    SELECT 
        product_id,
        SUM(total_revenue) as total_revenue
    FROM {full_summary_table}
    GROUP BY product_id
    ORDER BY product_id
""")

print("\n📊 Query 2: Total revenue per product (from SUMMARY - FAST!):")
print(f"   Scanned only {df_daily_summary.count()} summary rows")
print(f"   ⚡ {df_transactions.count() / df_daily_summary.count():.1f}x fewer rows to scan!")
df_from_summary.show()
```

## Validation


```python
if enable_validation:
    summary_count = df_daily_summary.count()
    assert summary_count == 9, f"Expected 9 daily summaries (3 days × 3 products), got {summary_count}"
    print(f"✅ {summary_count} daily summaries created (3 days × 3 products)")
    
    # Verify raw vs summary totals match
    raw_total = df_from_raw.select(spark_sum("total_revenue")).collect()[0][0]
    summary_total = df_from_summary.select(spark_sum("total_revenue")).collect()[0][0]
    assert abs(raw_total - summary_total) < 0.01, "Raw and summary totals should match"
    print(f"✅ Aggregation accuracy verified: ${raw_total:,.2f} (both methods)")
    
    # Verify data reduction
    reduction_factor = df_transactions.count() / df_daily_summary.count()
    assert reduction_factor > 50, "Should have significant data reduction"
    print(f"✅ Data reduction: {reduction_factor:.1f}x (query performance boost)")
    
    test_passed = True
    print("\n✅ All validations passed!")
else:
    test_passed = True
```

## Summary

### ✅ Pattern 1.6: Batch Aggregation Complete!

Demonstrated:
1. High-volume raw transaction data (1000 rows)
2. GroupBy aggregation (1000 rows → 9 summaries)
3. Pre-computed analytics table storage
4. Query performance comparison (100x+ data reduction)

**Benefits**: 
- ⚡ Fast dashboard queries (pre-computed)
- 📉 Massive data reduction (100x-1000x compression)
- 🎯 Predictable query performance
- 💰 Cost effective (fewer compute resources)

**Tradeoffs**:
- ⚠️ Storage overhead (duplicate data: raw + aggregated)
- ⚠️ Freshness lag (aggregations run on schedule)
- ⚠️ Inflexible dimensions (can't change grouping ad-hoc)

**Use Cases**: Sales dashboards, web analytics, IoT sensor summaries, financial reports


```python
if enable_cleanup:
    spark.sql(f"DROP TABLE IF EXISTS {full_raw_table}")
    spark.sql(f"DROP TABLE IF EXISTS {full_summary_table}")
    print("🧹 Cleanup completed")
else:
    print("⏭️  Cleanup skipped")
```
