# Pattern 7.2: Schema Evolution & Compatibility

Demonstrates safe schema evolution using Apache Iceberg:

1. **Additive Changes**: ADD COLUMN (backward compatible)
2. **Column Removal**: DROP COLUMN (forward compatible)
3. **Column Renaming**: RENAME COLUMN (metadata-only operation)
4. **Type Evolution**: Safe type promotions (INT → LONG, FLOAT → DOUBLE)
5. **Partition Evolution**: Change partitioning without data rewrite
6. **Compatibility Testing**: Validate backward/forward compatibility

## Architecture

```
Schema V1 (Initial)
    ↓
ALTER TABLE ADD COLUMN (backward compatible)
    ↓
Schema V2 (Old readers can still read)
    ↓
ALTER TABLE RENAME COLUMN (metadata-only)
    ↓
Schema V3 (No data rewrite needed)
    ↓
ALTER TABLE ALTER COLUMN TYPE (safe promotion)
    ↓
Schema V4 (Type evolution)
```

## Iceberg Schema Evolution Benefits

- **No Table Rewrites**: Schema changes are metadata-only operations
- **ACID Guarantees**: Schema evolution is transactional
- **Version Control**: Track schema changes with Nessie
- **Multi-Engine Support**: Spark, Trino, Flink all see consistent schema
- **Safe Defaults**: New columns automatically get NULL for old data

## Use Cases

- **Agile Development**: Evolve schemas as requirements change
- **Data Mesh**: Decentralized teams evolve schemas independently
- **Legacy Migration**: Gradually transform old schemas to new
- **A/B Testing**: Test schema changes without breaking production

## Parameters


```python
# Execution parameters
execution_date = "2025-01-17"
environment = "development"
database_name = "governance_demo"
table_name = "products_evolving"
partitioned_table = "sales_partitioned"
num_initial_records = 100
num_evolved_records = 50
enable_validation = True
enable_cleanup = False
```

## Step 1: Initialize Spark with Iceberg + Nessie


```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, DateType, TimestampType
from pyspark.sql.functions import (
    col, lit, current_timestamp, current_date, 
    year, month, dayofmonth, to_date, date_format
)
import random
from datetime import datetime, timedelta
import os

# Set AWS environment variables for S3FileIO
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "admin123"

spark = SparkSession.builder \
    .appName(f"Pattern-7.2-SchemaEvolution-{environment}") \
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
```

## Step 2: Create Initial Schema (V1)

Start with a simple product catalog schema.


```python
# Create database
full_database = f"lakehouse.{database_name}"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_database}")
print(f"✅ Database created: {full_database}")

full_table = f"{full_database}.{table_name}"

# Create initial table with Schema V1
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {full_table} (
        product_id INT,
        product_name STRING,
        category STRING,
        price DOUBLE,
        created_at TIMESTAMP
    ) USING iceberg
""")

print(f"\n✅ Schema V1 created: {full_table}")
print(f"\n📋 Initial Schema:")
spark.sql(f"DESCRIBE {full_table}").show(truncate=False)
```

## Step 3: Insert Initial Data (Schema V1)


```python
# Generate initial product data
def generate_products_v1(num_records):
    products = []
    categories = ["Electronics", "Clothing", "Home", "Sports", "Books"]
    
    for i in range(1, num_records + 1):
        products.append((
            i,
            f"Product {i}",
            random.choice(categories),
            round(random.uniform(10.0, 500.0), 2),
            datetime.now() - timedelta(days=random.randint(1, 365))
        ))
    
    return products

schema_v1 = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("created_at", TimestampType(), False)
])

products_v1 = generate_products_v1(num_initial_records)
df_v1 = spark.createDataFrame(products_v1, schema_v1)

df_v1.writeTo(full_table).append()

count_v1 = spark.table(full_table).count()
print(f"✅ Inserted {count_v1} records with Schema V1")
print(f"\n📊 Sample data:")
spark.table(full_table).show(5, truncate=False)
```

## Step 4: Schema Evolution - ADD COLUMN (Backward Compatible)

Add new columns without breaking existing readers.

**Backward Compatible**: Old readers (expecting V1 schema) can still read the table.


```python
# Add new columns: stock_quantity, supplier_id, last_updated
spark.sql(f"""
    ALTER TABLE {full_table}
    ADD COLUMNS (
        stock_quantity INT COMMENT 'Available inventory',
        supplier_id STRING COMMENT 'Supplier reference',
        last_updated TIMESTAMP COMMENT 'Last modification timestamp'
    )
""")

print(f"✅ Schema V2: Added 3 new columns (backward compatible)")
print(f"\n📋 Updated Schema:")
spark.sql(f"DESCRIBE {full_table}").show(truncate=False)

# Verify old data still readable (new columns are NULL)
print(f"\n📊 Old data with new schema (NULL for new columns):")
spark.table(full_table).show(5, truncate=False)

# Insert new data with V2 schema
print(f"\n✅ Inserting new records with V2 schema...")
spark.sql(f"""
    INSERT INTO {full_table}
    VALUES 
        (1001, 'New Product A', 'Electronics', 299.99, CURRENT_TIMESTAMP, 150, 'SUP-001', CURRENT_TIMESTAMP),
        (1002, 'New Product B', 'Clothing', 49.99, CURRENT_TIMESTAMP, 500, 'SUP-002', CURRENT_TIMESTAMP),
        (1003, 'New Product C', 'Home', 89.99, CURRENT_TIMESTAMP, 75, 'SUP-003', CURRENT_TIMESTAMP)
""")

print(f"✅ Inserted 3 records with V2 schema")
print(f"\n📊 New data (V2 schema with all columns populated):")
spark.sql(f"""
    SELECT * FROM {full_table}
    WHERE product_id >= 1001
""").show(truncate=False)
```

## Step 5: Schema Evolution - RENAME COLUMN (Metadata-Only)

Rename columns without rewriting data.

**Metadata-Only**: No data files are rewritten, only metadata updated.


```python
# Rename columns for better clarity
spark.sql(f"ALTER TABLE {full_table} RENAME COLUMN product_name TO name")
spark.sql(f"ALTER TABLE {full_table} RENAME COLUMN stock_quantity TO inventory_count")

print(f"✅ Schema V3: Renamed columns (metadata-only operation)")
print(f"   - product_name → name")
print(f"   - stock_quantity → inventory_count")
print(f"\n📋 Updated Schema:")
spark.sql(f"DESCRIBE {full_table}").show(truncate=False)

# Verify data still accessible with new column names
print(f"\n📊 Data with renamed columns:")
spark.table(full_table).select(
    "product_id", "name", "category", "price", "inventory_count", "supplier_id"
).show(5, truncate=False)
```

## Step 6: Schema Evolution - ALTER COLUMN TYPE (Safe Type Promotion)

Promote column types safely:
- INT → LONG (safe)
- FLOAT → DOUBLE (safe)

**Unsafe promotions** (e.g., LONG → INT, STRING → INT) are rejected by Iceberg.


```python
# Promote product_id from INT to LONG (safe evolution)
spark.sql(f"""
    ALTER TABLE {full_table}
    ALTER COLUMN product_id TYPE BIGINT
""")

print(f"✅ Schema V4: Type evolution (INT → LONG for product_id)")
print(f"\n📋 Updated Schema:")
spark.sql(f"DESCRIBE {full_table}").show(truncate=False)

# Verify data integrity after type promotion
print(f"\n📊 Data after type promotion:")
spark.table(full_table).select("product_id", "name", "price").show(5, truncate=False)

# Demonstrate safe vs unsafe type evolution
print(f"\n💡 Safe Type Promotions:")
print(f"   ✅ INT → LONG")
print(f"   ✅ FLOAT → DOUBLE")
print(f"   ✅ DECIMAL(10,2) → DECIMAL(20,2)")
print(f"\n⚠️  Unsafe Type Changes (Rejected by Iceberg):")
print(f"   ❌ LONG → INT (data loss)")
print(f"   ❌ DOUBLE → FLOAT (precision loss)")
print(f"   ❌ STRING → INT (type incompatibility)")
```

## Step 7: Schema Evolution - DROP COLUMN (Forward Compatible)

Remove columns that are no longer needed.

**Forward Compatible**: New readers (expecting V5 schema) won't see the dropped column.

**Note**: Data is not deleted, just hidden. Can be recovered if needed.


```python
# Drop supplier_id column (no longer needed)
spark.sql(f"""
    ALTER TABLE {full_table}
    DROP COLUMN supplier_id
""")

print(f"✅ Schema V5: Dropped column 'supplier_id' (forward compatible)")
print(f"\n📋 Updated Schema:")
spark.sql(f"DESCRIBE {full_table}").show(truncate=False)

# Verify column is no longer accessible
print(f"\n📊 Data after column drop:")
spark.table(full_table).show(5, truncate=False)

print(f"\n💡 Note: Data files are NOT rewritten. Column is just hidden from readers.")
```

## Step 8: Partition Evolution (Change Partitioning Strategy)

Iceberg allows changing partitioning without rewriting data.

Create a sales table and demonstrate partition evolution.


```python
full_partitioned_table = f"{full_database}.{partitioned_table}"

# Create table partitioned by date
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {full_partitioned_table} (
        transaction_id BIGINT,
        customer_id STRING,
        product_id INT,
        amount DOUBLE,
        transaction_date DATE
    ) USING iceberg
    PARTITIONED BY (days(transaction_date))
""")

print(f"✅ Created partitioned table: {full_partitioned_table}")
print(f"   Initial partitioning: DAYS(transaction_date)")

# Insert sample sales data
def generate_sales_data(num_records):
    sales = []
    base_date = datetime(2024, 1, 1)
    
    for i in range(1, num_records + 1):
        txn_date = base_date + timedelta(days=random.randint(0, 365))
        sales.append((
            i,
            f"CUST{random.randint(1, 100):04d}",
            random.randint(1, 1000),
            round(random.uniform(10.0, 1000.0), 2),
            txn_date.date()
        ))
    
    return sales

sales_schema = StructType([
    StructField("transaction_id", LongType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("amount", DoubleType(), False),
    StructField("transaction_date", DateType(), False)
])

sales_data = generate_sales_data(num_initial_records)
df_sales = spark.createDataFrame(sales_data, sales_schema)

df_sales.writeTo(full_partitioned_table).append()

print(f"\n✅ Inserted {df_sales.count()} sales transactions")
print(f"\n📊 Sample data:")
spark.table(full_partitioned_table).show(5, truncate=False)

# Show partition spec
print(f"\n📋 Current Partition Spec:")
spark.sql(f"DESCRIBE EXTENDED {full_partitioned_table}").filter(
    col("col_name").contains("Part")
).show(truncate=False)
```

## Step 9: Evolve Partition Strategy (Without Data Rewrite)

Change from daily to monthly partitioning without rewriting existing data.


```python
# Evolve partition spec to monthly partitioning
spark.sql(f"""
    ALTER TABLE {full_partitioned_table}
    ADD PARTITION FIELD months(transaction_date)
""")

print(f"✅ Partition evolution: Added MONTHS(transaction_date) partition")
print(f"   Old data: Still partitioned by DAYS")
print(f"   New data: Will be partitioned by MONTHS")
print(f"\n💡 No data rewrite needed! Iceberg tracks partition evolution.")

# Insert new data (will use monthly partitioning)
new_sales = generate_sales_data(num_evolved_records)
df_new_sales = spark.createDataFrame(new_sales, sales_schema)
df_new_sales.writeTo(full_partitioned_table).append()

total_count = spark.table(full_partitioned_table).count()
print(f"\n✅ Inserted {num_evolved_records} more transactions")
print(f"   Total transactions: {total_count}")
print(f"\n📊 Partition evolution in action:")
print(f"   - First {num_initial_records} records: Partitioned by DAYS")
print(f"   - Last {num_evolved_records} records: Partitioned by MONTHS")
```

## Step 10: Schema History & Compatibility Testing


```python
# View table history (snapshots)
print(f"📜 Table History (Snapshots):")
spark.sql(f"SELECT * FROM {full_table}.history").show(truncate=False)

# View table snapshots
print(f"\n📸 Table Snapshots:")
spark.sql(f"SELECT * FROM {full_table}.snapshots").select(
    "committed_at", "snapshot_id", "operation", "summary"
).show(truncate=False)

print(f"\n💡 Schema Evolution Summary:")
print(f"   V1: Initial schema (5 columns)")
print(f"   V2: Added 3 columns (backward compatible)")
print(f"   V3: Renamed 2 columns (metadata-only)")
print(f"   V4: Type evolution INT → LONG (safe promotion)")
print(f"   V5: Dropped 1 column (forward compatible)")
```

## Step 11: Validation & Testing


```python
if enable_validation:
    print("🧪 Running validation tests...\n")
    
    # Test 1: Verify column additions (V2 schema has 8 columns, V5 has 7)
    current_columns = spark.table(full_table).columns
    expected_columns = ["product_id", "name", "category", "price", "created_at", 
                       "inventory_count", "last_updated"]
    assert set(current_columns) == set(expected_columns), \
        f"Column mismatch: {current_columns} != {expected_columns}"
    print(f"✅ Test 1 PASSED: Schema has {len(current_columns)} expected columns")
    
    # Test 2: Verify old data has NULL for new columns
    old_data_nulls = spark.table(full_table).filter(
        (col("product_id") <= num_initial_records) & 
        (col("inventory_count").isNull())
    ).count()
    assert old_data_nulls > 0, "Old data should have NULL for new columns"
    print(f"✅ Test 2 PASSED: {old_data_nulls} old records have NULL for evolved columns")
    
    # Test 3: Verify new data has values for all columns
    new_data_complete = spark.table(full_table).filter(
        (col("product_id") >= 1001) & 
        (col("inventory_count").isNotNull())
    ).count()
    assert new_data_complete >= 3, "New data should have all columns populated"
    print(f"✅ Test 3 PASSED: {new_data_complete} new records have complete data")
    
    # Test 4: Verify column rename (old name should not exist)
    try:
        spark.table(full_table).select("product_name").count()
        assert False, "Old column name 'product_name' should not exist"
    except Exception as e:
        if "cannot resolve" in str(e).lower():
            print(f"✅ Test 4 PASSED: Old column name 'product_name' no longer exists")
        else:
            raise e
    
    # Test 5: Verify type evolution (product_id should be BIGINT)
    schema_info = spark.sql(f"DESCRIBE {full_table}").collect()
    product_id_type = [row[1] for row in schema_info if row[0] == "product_id"][0]
    assert "bigint" in product_id_type.lower(), f"product_id should be BIGINT, got {product_id_type}"
    print(f"✅ Test 5 PASSED: product_id type evolved to {product_id_type}")
    
    # Test 6: Verify dropped column is inaccessible
    try:
        spark.table(full_table).select("supplier_id").count()
        assert False, "Dropped column 'supplier_id' should not be accessible"
    except Exception as e:
        if "cannot resolve" in str(e).lower():
            print(f"✅ Test 6 PASSED: Dropped column 'supplier_id' is inaccessible")
        else:
            raise e
    
    # Test 7: Verify partition evolution
    partition_count = spark.table(full_partitioned_table).count()
    expected_count = num_initial_records + num_evolved_records
    assert partition_count == expected_count, \
        f"Partition evolution failed: {partition_count} != {expected_count}"
    print(f"✅ Test 7 PASSED: Partition evolution preserved all {partition_count} records")
    
    test_passed = True
    print("\n✅ All validation tests PASSED!")
else:
    test_passed = True
    print("⏭️  Validation skipped")
```

## Step 12: Production Best Practices


```python
best_practices = '''
# ============================================================================
# SCHEMA EVOLUTION BEST PRACTICES
# ============================================================================

## 1. BACKWARD COMPATIBILITY (ADD COLUMN)

✅ DO:
- Add optional columns (allow NULL)
- Add columns with default values
- Document new column meanings

❌ DON'T:
- Add required (NOT NULL) columns to existing tables
- Change column meanings without versioning

Example:
  ALTER TABLE customers ADD COLUMN phone STRING;  -- ✅ Optional
  ALTER TABLE customers ADD COLUMN email STRING NOT NULL;  -- ❌ Breaks old data

## 2. FORWARD COMPATIBILITY (DROP COLUMN)

✅ DO:
- Deprecate columns before dropping (set to NULL first)
- Coordinate with downstream consumers
- Test with downstream systems before dropping

❌ DON'T:
- Drop columns still in use by downstream jobs
- Drop columns without migration period

## 3. TYPE EVOLUTION

✅ SAFE PROMOTIONS:
  INT → LONG
  FLOAT → DOUBLE
  DECIMAL(10,2) → DECIMAL(20,2)  # Increase precision

❌ UNSAFE CHANGES:
  LONG → INT  # Data loss
  DOUBLE → FLOAT  # Precision loss
  STRING → INT  # Type incompatibility

## 4. PARTITION EVOLUTION

Use Cases:
- Change granularity: DAYS → MONTHS (reduce partition count)
- Add partition columns: Add REGION to existing DATE partition
- Remove partition columns: Deprecate old partition schemes

Example:
  # Evolve from daily to monthly partitioning
  ALTER TABLE sales ADD PARTITION FIELD months(sale_date);
  
  # New data uses monthly partitions, old data keeps daily partitions
  # No data rewrite needed!

## 5. TESTING STRATEGY

Before production deployment:

1. **Shadow Testing**: Clone table, evolve schema, test queries
2. **Backward Compatibility**: Old readers should still work
3. **Forward Compatibility**: New readers handle old data
4. **Performance Testing**: Ensure schema changes don't degrade performance
5. **Rollback Plan**: Can revert schema changes if needed

## 6. NESSIE INTEGRATION (VERSION CONTROL FOR SCHEMAS)

Use Nessie branches for safe schema evolution:

  # Create dev branch for schema testing
  nessie branch create dev_schema_v2
  
  # Test schema changes on dev branch
  USE REFERENCE dev_schema_v2;
  ALTER TABLE products ADD COLUMN new_field STRING;
  
  # Test, validate, then merge to main
  nessie merge dev_schema_v2 INTO main

## 7. MIGRATION CHECKLIST

Before schema evolution:
  □ Document schema change rationale
  □ Identify downstream consumers (Spark, Trino, Flink, BI tools)
  □ Test schema change on dev/staging environment
  □ Notify downstream teams (data engineers, analysts)
  □ Plan migration timeline (add columns, migrate data, drop old columns)
  □ Verify backward/forward compatibility
  □ Execute schema change during low-traffic window
  □ Monitor for errors/performance issues
  □ Document new schema in data catalog (OpenMetadata)

## 8. COMMON SCHEMA EVOLUTION PATTERNS

### Pattern A: Column Replacement
  # Migrate from old_email to new_email field
  ALTER TABLE customers ADD COLUMN new_email STRING;
  UPDATE customers SET new_email = old_email;  # Backfill
  # Wait for downstream migration
  ALTER TABLE customers DROP COLUMN old_email;

### Pattern B: Type Migration
  # Migrate STRING customer_id to BIGINT
  ALTER TABLE customers ADD COLUMN customer_id_v2 BIGINT;
  UPDATE customers SET customer_id_v2 = CAST(customer_id AS BIGINT);
  ALTER TABLE customers DROP COLUMN customer_id;
  ALTER TABLE customers RENAME COLUMN customer_id_v2 TO customer_id;

### Pattern C: Denormalization
  # Add computed columns for performance
  ALTER TABLE orders ADD COLUMN total_amount DOUBLE;
  UPDATE orders SET total_amount = quantity * unit_price;
  # Query performance improves (no runtime calculation needed)

# ============================================================================
'''

print("📋 Schema Evolution Best Practices:")
print("="*80)
print(best_practices)
print("="*80)
```

## Summary

### ✅ Pattern 7.2: Schema Evolution Complete!

This notebook demonstrated:

1. **Additive Changes**: ADD COLUMN (backward compatible)
2. **Column Renaming**: RENAME COLUMN (metadata-only, no data rewrite)
3. **Type Evolution**: INT → LONG (safe type promotion)
4. **Column Removal**: DROP COLUMN (forward compatible)
5. **Partition Evolution**: DAYS → MONTHS (without data rewrite)

### Iceberg Advantages:

- **No Table Rewrites**: Schema changes are metadata-only
- **ACID Guarantees**: Schema evolution is transactional
- **Multi-Version Support**: Old and new data coexist seamlessly
- **Multi-Engine Compatibility**: Spark, Trino, Flink all see consistent schema

### Key Takeaways:

1. **Backward Compatibility**: Add columns with NULL defaults
2. **Forward Compatibility**: Drop columns after downstream migration
3. **Safe Type Evolution**: Only promote types (INT→LONG), never demote
4. **Partition Evolution**: Change partitioning strategy without rewriting data
5. **Testing Strategy**: Always test schema changes on dev/staging first

### Next Steps:

- **Pattern 7.3**: Data Lineage Tracking
- **Pattern 7.4**: Data Cataloging & Discovery


```python
# Cleanup (optional)
if enable_cleanup:
    spark.sql(f"DROP TABLE IF EXISTS {full_table}")
    spark.sql(f"DROP TABLE IF EXISTS {full_partitioned_table}")
    spark.sql(f"DROP DATABASE IF EXISTS {full_database}")
    print("🧹 Cleanup completed")
else:
    print("⏭️  Cleanup skipped (enable_cleanup=False)")
    print(f"\n📊 Tables created:")
    print(f"   - {full_table} (Schema V5 - 7 columns)")
    print(f"   - {full_partitioned_table} (Partition evolved: DAYS + MONTHS)")
```
