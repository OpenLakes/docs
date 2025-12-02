```
from pyspark.sql.functions import col, sum as spark_sum, count, avg, when, expr, lit, window, current_timestamp, from_json
from datetime import datetime, date, timedelta

```

# Pattern 1.4: SCD Type 2 - Historical Change Tracking

Track all changes over time with effective dates:

1. **Initial Load**: Baseline product catalog
2. **Incremental Update**: Receive changed records
3. **SCD Type 2 Merge**: Preserve history
4. **Time Travel**: Query historical states

## SCD Type 2 Example

When price changes $100 → $120:

```
product_id | price | effective_from | effective_to | is_current
123        | 100   | 2025-01-01     | 2025-01-15   | false
123        | 120   | 2025-01-16     | 9999-12-31   | true
```

## Parameters


```
execution_date = "2025-01-16"
environment = "development"
database_name = "demo"
scd2_table = "product_catalog_scd2"
enable_validation = True
enable_cleanup = False
```


```
import os

from pyspark.sql import SparkSession
# Set AWS environment variables for S3FileIO
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "admin123"

spark = SparkSession.builder \
    .appName(f"Pattern-1.4-SCD2-{environment}") \
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

print("✅ Spark initialized for SCD Type 2")
```

## Initial Load: Baseline Data


```
full_database = f"lakehouse.{database_name}"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_database}")

baseline_date = date(2025, 1, 1)
end_date = date(9999, 12, 31)

initial_data = [
    (101, "Laptop Pro", 1299.99, baseline_date, end_date, True),
    (102, "Mouse", 29.99, baseline_date, end_date, True),
]

df_initial = spark.createDataFrame(initial_data, ["product_id", "product_name", "price", "effective_from", "effective_to", "is_current"])

full_scd2 = f"{full_database}.{scd2_table}"
df_initial.writeTo(full_scd2).using("iceberg").createOrReplace()

print(f"✅ Initial load: {df_initial.count()} products")
df_initial.show()
```

## Incremental Update: Price Changes


```
change_date = date(2025, 1, 16)

# Product 101: Price drop $1299.99 → $1199.99
changes_data = [
    (101, "Laptop Pro", 1199.99),
]

df_changes = spark.createDataFrame(changes_data, ["product_id", "product_name", "price"])
print(f"📊 Changes received ({change_date}):")
df_changes.show()
```

## SCD Type 2 Merge: Preserve History


```
# Register for SQL
df_changes.createOrReplaceTempView("incoming_changes")

# Expire old records
spark.sql(f"""
    MERGE INTO {full_scd2} AS target
    USING incoming_changes AS source
    ON target.product_id = source.product_id 
       AND target.is_current = true
       AND target.price != source.price
    WHEN MATCHED THEN UPDATE SET
        effective_to = DATE '{change_date}' - INTERVAL 1 DAY,
        is_current = false
""")

print("✅ Expired old records")

# Insert new versions
df_new = df_changes \
    .withColumn("effective_from", lit(change_date)) \
    .withColumn("effective_to", lit(end_date)) \
    .withColumn("is_current", lit(True))

df_new.writeTo(full_scd2).using("iceberg").append()

print(f"✅ Inserted new versions")
print(f"🎉 SCD Type 2 merge completed!")
```

## Query Historical Data


```
# Current state
df_current = spark.sql(f"""
    SELECT * FROM {full_scd2}
    WHERE is_current = true
    ORDER BY product_id
""")

print("📊 Current State:")
df_current.show()

# All history
df_history = spark.sql(f"""
    SELECT * FROM {full_scd2}
    WHERE product_id = 101
    ORDER BY effective_from
""")

print("📊 Product 101 History:")
df_history.show()
```

## Validation


```
if enable_validation:
    current_count = df_current.count()
    assert current_count == 2, f"Expected 2 current records, got {current_count}"
    print(f"✅ {current_count} current records")
    
    historical_count = spark.sql(f"SELECT COUNT(*) FROM {full_scd2} WHERE is_current = false").collect()[0][0]
    assert historical_count == 1, f"Expected 1 historical record"
    print(f"✅ {historical_count} historical record (expired version)")
    
    new_price = df_current.filter(col("product_id") == 101).select("price").collect()[0][0]
    assert new_price == 1199.99
    print(f"✅ Laptop price updated to ${new_price}")
    
    test_passed = True
    print("\n✅ All validations passed!")
else:
    test_passed = True
```

## Summary

### ✅ Pattern 1.4: SCD Type 2 Complete!

Demonstrated:
1. Initial baseline load
2. Price change tracking with effective dates
3. Historical query capabilities

**Benefits**: Complete audit trail, point-in-time queries, regulatory compliance


```
if enable_cleanup:
    spark.sql(f"DROP TABLE IF EXISTS {full_scd2}")
    print("🧹 Cleanup completed")
else:
    print("⏭️  Cleanup skipped")
```
