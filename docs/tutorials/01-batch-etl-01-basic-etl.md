# Pattern 1.1: Basic ETL - Extract, Transform, Load

This notebook demonstrates a complete batch ETL pipeline:

1. **Extract**: Generate sample product data (simulates Meltano extraction)
2. **Transform**: Aggregate products by category using Spark
3. **Load**: Write to Iceberg tables on MinIO S3
4. **Validate**: Query with Trino to verify engine interoperability

## Architecture

```
Source Data → Iceberg (staging) → Spark (transform) → Iceberg (curated) → Trino/Superset
                      ↓
            Hadoop Catalog (file-based metadata on S3)
```

## Components Used

- **Spark 4.1.0**: ETL processing engine
- **Apache Iceberg 1.8.0**: Lakehouse table format with Hadoop catalog
- **MinIO**: S3-compatible object storage
- **Trino**: SQL query engine for validation

## Prerequisites

- OpenLakes Core deployed (Layers 01, 02)
- JupyterHub connected to Spark cluster
- Iceberg and S3A libraries available

## Note on Nessie Integration

Project Nessie (deployed in Layer 01) requires the `iceberg-nessie` JAR for full integration.
This will be added in a future update to enable Git-like versioning for Iceberg tables.
Currently using Nessie catalog (Git-like versioning) which works for multi-engine access.

## Parameters

This cell is tagged with `parameters` for Papermill execution.
When run from Airflow, these values will be overridden with production settings.


```python
# Execution parameters (development defaults)
execution_date = "2025-01-16"
environment = "development"
database_name = "demo"
source_table = "products_source"
target_table = "products_aggregated"
enable_validation = True
enable_cleanup = False
```

## Step 1: Initialize Spark Session with Iceberg Support


```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import sum as spark_sum, count, avg, col

# Create Spark session with Iceberg support via Hadoop catalog
# Note: JARs are pre-installed in the Docker image, no need to download via spark.jars.packages
# Using Hadoop catalog as Nessie requires iceberg-nessie JAR (will add in future update)
spark = SparkSession.builder \
    .appName(f"Pattern-1.1-BasicETL-{environment}-{execution_date}") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config("spark.sql.catalog.lakehouse.uri", "http://infrastructure-nessie:19120/api/v2") \
    .config("spark.sql.catalog.lakehouse.ref", "main") \
    .config("spark.sql.catalog.lakehouse.warehouse", "s3a://openlakes/warehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://infrastructure-minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print(f"✅ Spark session created with Iceberg + Hadoop catalog")
print(f"   App Name: {spark.sparkContext.appName}")
print(f"   Spark Version: {spark.version}")
print(f"   Catalog: Nessie")
print(f"   Warehouse: s3a://openlakes/warehouse/")
print(f"   Environment: {environment}")
print(f"   Execution Date: {execution_date}")
```

## Step 2: Extract - Create Source Data

In a real pipeline, this would be data extracted by Meltano from an external source.
For this demo, we generate sample product data.


```python
# Create database
full_database = f"lakehouse.{database_name}"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_database}")
print(f"✅ Database created: {full_database}")

# Define schema for product data
schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("quantity", IntegerType(), False)
])

# Sample product data (simulates Meltano extraction)
source_data = [
    (1, "Laptop", "Electronics", 999.99, 50),
    (2, "Mouse", "Electronics", 29.99, 200),
    (3, "Keyboard", "Electronics", 79.99, 150),
    (4, "Monitor", "Electronics", 299.99, 75),
    (5, "Desk", "Furniture", 199.99, 30),
    (6, "Chair", "Furniture", 149.99, 45),
    (7, "Notebook", "Stationery", 4.99, 500),
    (8, "Pen", "Stationery", 1.99, 1000)
]

df_source = spark.createDataFrame(source_data, schema)

print(f"\n📊 Source Data (Extract phase):")
df_source.show()
```

## Step 3: Load - Write Source Data to Iceberg (Staging)

Write extracted data to staging Iceberg table on MinIO.


```python
# Write to Iceberg staging table on MinIO
full_source_table = f"{full_database}.{source_table}"
df_source.writeTo(full_source_table) \
    .using("iceberg") \
    .createOrReplace()

source_count = df_source.count()
print(f"✅ Written {source_count} rows to {full_source_table} (Iceberg on MinIO)")

# Verify write
df_verify = spark.table(full_source_table)
print(f"✅ Verified: {df_verify.count()} rows in source table")
```

## Step 4: Transform - Aggregate by Category

Perform ETL transformation using Spark:
- Group products by category
- Calculate metrics: count, total quantity, average price, total value


```python
# Read source table
df_source = spark.table(full_source_table)
print(f"✅ Read {df_source.count()} rows from source table")

# Transform: Aggregate by category
df_transformed = df_source.groupBy("category").agg(
    count("*").alias("product_count"),
    spark_sum("quantity").alias("total_quantity"),
    avg("price").alias("avg_price"),
    spark_sum(col("price") * col("quantity")).alias("total_value")
)

print(f"\n📊 Transformed Data (Aggregated by Category):")
df_transformed.show()
```

## Step 5: Load - Write Transformed Data to Iceberg (Curated)

Write aggregated data to curated Iceberg table.


```python
# Write transformed data to target Iceberg table
full_target_table = f"{full_database}.{target_table}"
df_transformed.writeTo(full_target_table) \
    .using("iceberg") \
    .createOrReplace()

target_count = df_transformed.count()
print(f"✅ Written {target_count} rows to {full_target_table}")
print(f"\n🎉 ETL Transformation completed!")
print(f"   Source: {full_source_table} ({source_count} products)")
print(f"   Target: {full_target_table} ({target_count} categories)")
```

## Step 6: Validate with Trino (Engine Interoperability)

Query the Iceberg tables with Trino to demonstrate that multiple engines
can read the same data (Spark writes, Trino reads).


```python
if enable_validation:
    # Install Trino Python client if not already available
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "trino", "--quiet"])
    
    import trino
    
    # Connect to Trino
    conn = trino.dbapi.connect(
        host='compute-trino',
        port=8080,
        user='jupyter',
        catalog='lakehouse',
        schema=database_name
    )
    
    print("✅ Connected to Trino")
else:
    print("⏭️  Validation skipped (enable_validation=False)")
```


```python
if enable_validation:
    # Validate: Electronics category should have 4 products
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT product_count 
        FROM {full_target_table}
        WHERE category = 'Electronics'
    """)
    electronics_count = cursor.fetchone()[0]
    
    assert electronics_count == 4, f"Expected 4 Electronics products, got {electronics_count}"
    print("✅ Data validation passed: Electronics has 4 products")
    
    # Set success marker for Papermill
    test_passed = True
    print(f"\n✅ All validations passed!")
else:
    test_passed = True
    print("⏭️  Validation skipped")
```

## Summary

### ✅ Pattern 1.1: Basic ETL Completed!

This notebook demonstrated:

1. **Extract**: Generated sample product data (simulates Meltano)
2. **Transform**: Aggregated data by category using Spark
3. **Load**: Wrote to Iceberg tables on MinIO (staging + curated)
4. **Validate**: Queried with Trino for engine interoperability

### Papermill Integration:

This notebook can be executed:
- **Interactively** in JupyterHub with development defaults
- **Programmatically** via Airflow PapermillOperator with production parameters


```python
# Cleanup (optional)
if enable_cleanup:
    spark.sql(f"DROP TABLE IF EXISTS {full_source_table}")
    spark.sql(f"DROP TABLE IF EXISTS {full_target_table}")
    spark.sql(f"DROP DATABASE IF EXISTS {full_database}")
    print("🧹 Cleanup completed")
else:
    print("⏭️  Cleanup skipped (enable_cleanup=False)")
```
