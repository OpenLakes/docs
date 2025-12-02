```python
from pyspark.sql.functions import col, sum as spark_sum, count, avg, when, expr, lit, window, current_timestamp, from_json

```

# Pattern 1.5: SCD Type 1 - Current State Only (Overwrite)

Maintain current state without history - ideal for reference data:

1. **Initial Load**: Reference data baseline
2. **Update**: Receive corrections or updates
3. **SCD Type 1 Merge**: Overwrite old values
4. **Query**: Always see current state only

## SCD Type 1 Example

When currency description changes:

```
Before:
code | name                  | symbol
USD  | US Dollar (old desc)  | $

After:
code | name                  | symbol
USD  | US Dollar (corrected) | $
```

**History is lost** - only current state preserved.

## Parameters


```python
execution_date = "2025-01-16"
environment = "development"
database_name = "demo"
reference_table = "currency_codes_scd1"
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
    .appName(f"Pattern-1.5-SCD1-{environment}") \
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

print("✅ Spark initialized for SCD Type 1")
```

## Initial Load: Reference Data Baseline


```python
full_database = f"lakehouse.{database_name}"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_database}")

# Initial currency reference data
initial_data = [
    ("USD", "US Dollar", "$", "United States"),
    ("EUR", "Euro", "€", "European Union"),
    ("GBP", "British Pound", "£", "United Kingdom"),
]

df_initial = spark.createDataFrame(initial_data, ["code", "name", "symbol", "region"])

full_table = f"{full_database}.{reference_table}"
df_initial.writeTo(full_table).using("iceberg").createOrReplace()

print(f"✅ Initial load: {df_initial.count()} currency codes")
df_initial.show()
```

## Update: Corrections and New Entries


```python
# Updates received:
# - EUR: Corrected name format
# - GBP: Updated region name
# - JPY: New currency added

updates_data = [
    ("EUR", "Euro (EUR)", "€", "European Union"),      # Name format corrected
    ("GBP", "British Pound", "£", "Great Britain"),    # Region updated
    ("JPY", "Japanese Yen", "¥", "Japan"),             # New entry
]

df_updates = spark.createDataFrame(updates_data, ["code", "name", "symbol", "region"])
print("📊 Updates received:")
df_updates.show()
```

## SCD Type 1 Merge: Overwrite Changes


```python
# Register for SQL
df_updates.createOrReplaceTempView("currency_updates")

# SCD Type 1: Update existing + insert new
spark.sql(f"""
    MERGE INTO {full_table} AS target
    USING currency_updates AS source
    ON target.code = source.code
    WHEN MATCHED THEN UPDATE SET
        name = source.name,
        symbol = source.symbol,
        region = source.region
    WHEN NOT MATCHED THEN INSERT *
""")

print("✅ SCD Type 1 merge completed!")
print("⚠️  Old values overwritten - history NOT preserved")
```

## Query Current State


```python
df_current = spark.sql(f"""
    SELECT * FROM {full_table}
    ORDER BY code
""")

print("📊 Current State (after SCD Type 1 merge):")
df_current.show()
```

## Validation


```python
if enable_validation:
    total_count = df_current.count()
    assert total_count == 4, f"Expected 4 currencies, got {total_count}"
    print(f"✅ {total_count} currencies in table")
    
    # Verify EUR correction
    eur_name = df_current.filter(col("code") == "EUR").select("name").collect()[0][0]
    assert "(EUR)" in eur_name, "EUR name should include (EUR)"
    print(f"✅ EUR name corrected: {eur_name}")
    
    # Verify GBP region update
    gbp_region = df_current.filter(col("code") == "GBP").select("region").collect()[0][0]
    assert gbp_region == "Great Britain", "GBP region should be updated"
    print(f"✅ GBP region updated: {gbp_region}")
    
    # Verify JPY was added
    jpy_exists = df_current.filter(col("code") == "JPY").count() == 1
    assert jpy_exists, "JPY should be added"
    print("✅ JPY currency added")
    
    test_passed = True
    print("\n✅ All validations passed!")
else:
    test_passed = True
```

## Summary

### ✅ Pattern 1.5: SCD Type 1 Complete!

Demonstrated:
1. Initial reference data load
2. SCD Type 1 merge (overwrite + insert)
3. Current state query

**Benefits**: Simple, storage-efficient, always current

**Tradeoffs**: No history, no audit trail, can't query past states

**Use Cases**: Reference data, typo corrections, cosmetic changes


```python
if enable_cleanup:
    spark.sql(f"DROP TABLE IF EXISTS {full_table}")
    print("🧹 Cleanup completed")
else:
    print("⏭️  Cleanup skipped")
```
