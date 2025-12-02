```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType, LongType
from pyspark.sql.functions import col, sum as spark_sum, count, avg, when, expr, lit, window, current_timestamp, from_json

```

# Pattern 1.2: ELT - Extract, Load, Transform (SQL-based)

This notebook demonstrates the ELT pattern where transformation happens in the query engine:

1. **Extract**: Generate sample customer data (simulates Meltano extraction)
2. **Load**: Write raw data directly to Iceberg (no transformation)
3. **Transform**: Use Trino SQL to transform and create curated tables
4. **Validate**: Query curated data to verify transformations

## Architecture

```
Meltano → Iceberg (raw) → Trino (SQL transforms) → Iceberg (transformed)
```

## Why ELT vs ETL?

- **ELT**: Load raw data first, transform with SQL later
  - Benefits: Preserves raw data, leverages SQL skills, query engine optimizations
- **ETL**: Transform before loading
  - Benefits: Complex transformations, data quality at source

## Parameters


```python
# Execution parameters (development defaults)
execution_date = "2025-01-16"
environment = "development"
database_name = "demo"
raw_table = "customers_raw"
transformed_table = "customers_segmented"
enable_validation = True
enable_cleanup = False
```

## Step 1: Initialize Spark Session (Minimal Setup)


```python
import os

from pyspark.sql import SparkSession
# Set AWS environment variables for S3FileIO
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "admin123"

spark = SparkSession.builder \
    .appName(f"Pattern-1.2-ELT-{environment}-{execution_date}") \
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

print(f"✅ Spark session created (for loading only)")
print(f"   Environment: {environment}")
```

## Step 2: Extract - Generate Sample Customer Data


```python
full_database = f"lakehouse.{database_name}"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_database}")

schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("total_purchases", IntegerType(), False),
    StructField("total_spent", DoubleType(), False),
    StructField("country", StringType(), False)
])

source_data = [
    (1, "Alice", "alice@example.com", 25, 1250.50, "USA"),
    (2, "Bob", "bob@example.com", 8, 320.00, "Canada"),
    (3, "Carol", "carol@example.com", 42, 2100.75, "USA"),
    (4, "David", "david@example.com", 3, 75.25, "UK"),
]

df_raw = spark.createDataFrame(source_data, schema)
print("📊 Raw Customer Data:")
df_raw.show()
```

## Step 3: Load - Write Raw Data to Iceberg


```python
full_raw_table = f"{full_database}.{raw_table}"
df_raw.writeTo(full_raw_table).using("iceberg").createOrReplace()

raw_count = df_raw.count()
print(f"✅ Loaded {raw_count} raw rows (ELT: no transformation yet)")
```

## Step 4: Transform - Use Trino SQL

Segment customers based on spending using SQL CREATE TABLE AS SELECT.


```python
import subprocess, sys
subprocess.check_call([sys.executable, "-m", "pip", "install", "trino", "--quiet"])

import trino

conn = trino.dbapi.connect(
    host='compute-trino',
    port=8080,
    user='jupyter',
    catalog='lakehouse',
    schema=database_name
)

print("✅ Connected to Trino")
```


```python
full_transformed_table = f"{full_database}.{transformed_table}"

transform_sql = f"""
CREATE OR REPLACE TABLE {full_transformed_table} AS
SELECT 
    customer_id,
    name,
    email,
    country,
    total_purchases,
    total_spent,
    ROUND(total_spent / total_purchases, 2) as avg_order_value,
    CASE 
        WHEN total_spent >= 2000 THEN 'VIP'
        WHEN total_spent >= 500 THEN 'Premium'
        ELSE 'Standard'
    END as customer_segment
FROM {full_raw_table}
"""

cursor = conn.cursor()
cursor.execute(transform_sql)

print(f"✅ Trino SQL transformation completed")
print(f"   Segmented customers by spending")
```

## Step 5: Validate


```python
if enable_validation:
    cursor.execute(f"SELECT COUNT(*) FROM {full_raw_table}")
    raw_table_count = cursor.fetchone()[0]
    
    cursor.execute(f"SELECT COUNT(*) FROM {full_transformed_table}")
    transformed_table_count = cursor.fetchone()[0]
    
    assert raw_table_count == transformed_table_count
    print(f"✅ Validation passed: Row counts match ({raw_table_count} rows)")
    
    test_passed = True
    print(f"\n✅ All validations passed!")
else:
    test_passed = True
```

## Summary

### ✅ Pattern 1.2: ELT Completed!

This demonstrated the ELT pattern:
1. **Extract**: Generated sample data
2. **Load**: Wrote raw data directly (no transformation)
3. **Transform**: Used Trino SQL to segment customers
4. **Validate**: Verified data integrity

### ELT Benefits:
- ✅ Raw data preserved
- ✅ SQL-based transformations
- ✅ Query engine optimizations


```python
if enable_cleanup:
    cursor.execute(f"DROP TABLE IF EXISTS {full_raw_table}")
    cursor.execute(f"DROP TABLE IF EXISTS {full_transformed_table}")
    spark.sql(f"DROP DATABASE IF EXISTS {full_database}")
    print("🧹 Cleanup completed")
else:
    print("⏭️  Cleanup skipped")
```
