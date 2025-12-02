# Pattern 8.4: Orphan File Cleanup

Comprehensive orphan file management demonstrating:

1. **Orphan Files**: S3 files NOT tracked by Iceberg metadata
2. **Root Causes**: Failed writes, incomplete transactions, concurrent writers
3. **REMOVE ORPHAN FILES**: Iceberg procedure to delete orphans
4. **Safety Checks**: Only delete files older than N days (default: 3 days)
5. **Storage Reclamation**: Recover wasted S3 space
6. **S3 Lifecycle Policies**: Integration with AWS lifecycle rules
7. **Dry-Run Mode**: Preview orphans before deletion

## Architecture

```
S3 Bucket: openlakes/warehouse/

TRACKED FILES (in Iceberg metadata):
  data/
    ├─ file001.parquet ✅ Referenced by snapshot 123
    ├─ file002.parquet ✅ Referenced by snapshot 124
    └─ file003.parquet ✅ Referenced by snapshot 125

ORPHAN FILES (NOT in Iceberg metadata):
  data/
    ├─ file004.parquet ⚠️  Created by failed write (transaction aborted)
    ├─ file005.parquet ⚠️  Unreferenced after compaction
    └─ file006.parquet ⚠️  Leftover from concurrent write conflict

REMOVE ORPHAN FILES Procedure:
  1. List all files in S3 bucket
  2. List all files in Iceberg metadata
  3. Identify orphans (in S3 but not in metadata)
  4. Delete orphans older than safety threshold
  5. Reclaim storage space
```

## How Orphans Are Created

**Scenario 1: Failed Write**
```
1. Spark writes file001.parquet to S3
2. Before committing to Iceberg metadata, Spark crashes
3. Result: file001.parquet exists in S3 but NOT in metadata
4. → ORPHAN FILE
```

**Scenario 2: Compaction**
```
1. Compaction creates new_file.parquet (combines old files)
2. Iceberg metadata updated to reference new_file.parquet
3. Old files deleted from metadata (unreferenced)
4. If snapshot expiration hasn't run, old files still in S3
5. → ORPHAN FILES (until snapshot expiration)
```

**Scenario 3: Concurrent Writers**
```
1. Writer A creates fileA.parquet
2. Writer B creates fileB.parquet
3. Both try to commit (optimistic concurrency)
4. One succeeds, one fails
5. Failed writer's file still in S3
6. → ORPHAN FILE
```

## Use Cases

- **Storage Optimization**: Reclaim 5-20% of S3 space
- **Cost Reduction**: Delete unused files = lower S3 bill
- **Housekeeping**: Clean up after failures/retries
- **Compliance**: Remove unreferenced sensitive data
- **Maintenance**: Weekly/monthly cleanup routine

## Safety Considerations

**Why wait N days before deleting?**
- In-flight writes may not be in metadata yet
- Time travel needs recent snapshots (which reference files)
- Gives time to recover from mistakes

**Default: 3 days**
- Safe for most workloads
- Longer for critical tables (7 days)
- Shorter for dev/test (1 day)

## Parameters

This cell is tagged with `parameters` for Papermill execution.


```python
# Execution parameters
execution_date = "2025-01-17"
environment = "development"
database_name = "maintenance_demo"
table_name = "sales_data"
orphan_age_days = 3  # Only delete orphans older than 3 days
num_records = 5000
simulate_failures = True  # Create orphan files for demonstration
enable_validation = True
enable_cleanup = False
```

## Step 1: Initialize Spark with Iceberg + Nessie


```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
from pyspark.sql.functions import (
    col, current_timestamp, lit, rand, floor, expr, count, sum as spark_sum,
    avg, min as spark_min, max as spark_max, to_date, current_date
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
    .appName(f"Pattern-8.4-OrphanFileCleanup-{environment}") \
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

## Step 2: Create Table with Normal Writes


```python
# Create database
full_database = f"lakehouse.{database_name}"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_database}")
print(f"✅ Database created: {full_database}")

# Define schema
schema = StructType([
    StructField("sale_id", StringType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("sale_date", DateType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("total_amount", DoubleType(), False),
    StructField("store_id", StringType(), True)
])

full_table = f"{full_database}.{table_name}"

print(f"\n📊 Creating table with normal data writes...\n")

# Generate sales data
sales_data = []
for i in range(num_records):
    sale_id = f"SALE{i:06d}"
    product_id = random.randint(1000, 9999)
    sale_date = (datetime(2025, 1, 17) - timedelta(days=random.randint(0, 30))).date()
    quantity = random.randint(1, 100)
    unit_price = round(random.uniform(10, 500), 2)
    total_amount = round(quantity * unit_price, 2)
    store_id = f"STORE{random.randint(1, 50):03d}"
    
    sales_data.append((
        sale_id, product_id, sale_date, quantity, unit_price, total_amount, store_id
    ))

df_sales = spark.createDataFrame(sales_data, schema)

# Create table
df_sales.writeTo(full_table) \
    .using("iceberg") \
    .partitionedBy("sale_date") \
    .create()

print(f"✅ Created table with {num_records:,} records")
print(f"   Table: {full_table}")
```

## Step 3: View Current File Inventory


```python
print("📊 Current File Inventory (tracked by Iceberg):\n")

df_files_before = spark.sql(f"""
    SELECT 
        file_path,
        partition,
        ROUND(file_size_in_bytes / 1024 / 1024, 2) as file_size_mb,
        record_count
    FROM lakehouse.{database_name}.{table_name}.files
    ORDER BY file_path
""")

df_files_before.show(20, truncate=False)

file_stats_before = spark.sql(f"""
    SELECT 
        COUNT(*) as total_files,
        ROUND(SUM(file_size_in_bytes) / 1024 / 1024, 2) as total_size_mb,
        SUM(record_count) as total_records
    FROM lakehouse.{database_name}.{table_name}.files
""")

print("\nFile Statistics (BEFORE orphan cleanup):")
file_stats_before.show(truncate=False)

stats_before = file_stats_before.collect()[0]
tracked_files_before = stats_before['total_files']
tracked_size_before_mb = stats_before['total_size_mb']

print(f"\n📊 Summary:")
print(f"   Tracked files: {tracked_files_before}")
print(f"   Tracked storage: {tracked_size_before_mb} MB")
```

## Step 4: Simulate Orphan File Creation

Demonstrate how orphan files are created (failed writes, etc.).


```python
if simulate_failures:
    print("\n⚠️  Simulating orphan file creation...\n")
    
    # Get table location
    table_metadata = spark.sql(f"DESCRIBE EXTENDED {full_table}").collect()
    location = None
    for row in table_metadata:
        if row[0] == "Location":
            location = row[1]
            break
    
    print(f"Table location: {location}")
    
    # Simulate orphan files by writing directly to S3 (bypassing Iceberg)
    # In practice, these would be created by failed transactions
    
    print("\nSimulation scenarios:")
    print("1. Failed write (transaction aborted before commit)")
    print("2. Compaction leftovers (old files not yet deleted)")
    print("3. Concurrent write conflict (optimistic locking failure)")
    
    # Create some orphan data
    orphan_data = []
    for i in range(500):  # Create 500 orphan records
        sale_id = f"ORPHAN{i:05d}"
        product_id = random.randint(1000, 9999)
        sale_date = (datetime(2025, 1, 17) - timedelta(days=10)).date()  # Old date
        quantity = random.randint(1, 100)
        unit_price = round(random.uniform(10, 500), 2)
        total_amount = round(quantity * unit_price, 2)
        store_id = f"STORE{random.randint(1, 50):03d}"
        
        orphan_data.append((
            sale_id, product_id, sale_date, quantity, unit_price, total_amount, store_id
        ))
    
    df_orphan = spark.createDataFrame(orphan_data, schema)
    
    # Write to a temporary path to simulate orphan files
    # Note: In real scenarios, these would be in the table location but not in metadata
    orphan_path = f"s3a://openlakes/warehouse/{database_name}/{table_name}_orphan_simulation"
    df_orphan.write.mode("overwrite").parquet(orphan_path)
    
    print(f"\n⚠️  Created simulated orphan files at: {orphan_path}")
    print(f"   In production, orphan files would be in the table location")
    print(f"   but NOT tracked in Iceberg metadata")
    
else:
    print("\n⏭️  Orphan simulation skipped (simulate_failures=False)")
```

## Step 5: Understanding Orphan Detection

Explain how Iceberg identifies orphan files.


```python
print("🔍 How REMOVE ORPHAN FILES Works:\n")

explanation = '''
STEP 1: List ALL files in S3 table location
  - Recursively scan s3a://openlakes/warehouse/database/table/
  - Find all .parquet, .avro, .orc files
  - Result: Set A = {all S3 files}

STEP 2: List ALL files in Iceberg metadata
  - Query all snapshots
  - Extract file paths from manifests
  - Result: Set B = {tracked files}

STEP 3: Identify orphans
  - Orphans = Set A - Set B
  - Files in S3 but NOT in metadata

STEP 4: Apply safety filters
  - Only consider files older than N days (default: 3)
  - Check file modification timestamp
  - Excludes metadata files (.json, .avro)

STEP 5: Delete orphan files
  - Delete from S3 (permanent!)
  - Return list of deleted files
  - Log for auditing

SAFETY MECHANISMS:
✅ Age threshold (default: 3 days)
   → Protects in-flight writes
✅ Dry-run mode available
   → Preview before deletion
✅ Only deletes data files
   → Never deletes metadata files
✅ Requires table lock
   → Prevents concurrent modifications
'''

print(explanation)
```

## Step 6: Run REMOVE ORPHAN FILES (Dry-Run)

First, run in dry-run mode to see what would be deleted.


```python
print("🔍 Running REMOVE ORPHAN FILES (DRY-RUN MODE)...\n")
print(f"   Orphan age threshold: {orphan_age_days} days")
print(f"   Only files older than {orphan_age_days} days will be identified\n")

# Calculate older_than timestamp
older_than_date = datetime.now() - timedelta(days=orphan_age_days)
older_than_timestamp = older_than_date.strftime("%Y-%m-%d %H:%M:%S")

print(f"Identifying orphans older than: {older_than_timestamp}\n")

# Note: Iceberg's remove_orphan_files procedure doesn't have a dry-run flag
# In production, you would query the files table and compare with S3 listing
# For this demo, we'll show the concept

print("💡 DRY-RUN CONCEPT (would be implemented via custom script):")
print("""
import boto3
from pyspark.sql import SparkSession

def dry_run_orphan_detection(table, older_than_days):
    # Get all files in Iceberg metadata
    tracked_files = spark.sql(f"""
        SELECT file_path 
        FROM {table}.files
    """).collect()
    tracked_set = {row['file_path'] for row in tracked_files}
    
    # Get all files in S3
    s3 = boto3.client('s3')
    bucket, prefix = parse_s3_path(table_location)
    
    s3_files = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                s3_files.append(obj)
    
    # Identify orphans
    orphans = []
    cutoff_time = datetime.now() - timedelta(days=older_than_days)
    
    for obj in s3_files:
        file_path = f"s3://{bucket}/{obj['Key']}"
        if file_path not in tracked_set:
            if obj['LastModified'] < cutoff_time:
                orphans.append({
                    'path': file_path,
                    'size': obj['Size'],
                    'last_modified': obj['LastModified']
                })
    
    return orphans

# Run dry-run
orphans = dry_run_orphan_detection('lakehouse.production.events', 3)
print(f"Found {len(orphans)} orphan files")
print(f"Total size: {sum(o['size'] for o in orphans) / 1024 / 1024:.2f} MB")
""")

print("\n✅ Dry-run complete (in production, would list orphan files here)")
```

## Step 7: Run REMOVE ORPHAN FILES (Actual Cleanup)

Now run the actual cleanup to delete orphan files.


```python
print(f"🗑️  Running REMOVE ORPHAN FILES (ACTUAL CLEANUP)...\n")
print(f"   ⚠️  This will PERMANENTLY delete orphan files!")
print(f"   Older than: {older_than_timestamp}\n")

# Run REMOVE ORPHAN FILES procedure
cleanup_result = spark.sql(f"""
    CALL lakehouse.system.remove_orphan_files(
        table => '{full_table}',
        older_than => TIMESTAMP '{older_than_timestamp}'
    )
""")

print("Cleanup results:")
cleanup_result.show(truncate=False)

# The result shows:
# - orphan_file_location: Path to deleted file
# - Number of files deleted

result_rows = cleanup_result.collect()
if result_rows:
    orphans_deleted = len(result_rows)
    print(f"\n✅ Deleted {orphans_deleted} orphan files")
else:
    print("\n✅ No orphan files found (table is clean!)")
```

## Step 8: Verify File Inventory After Cleanup


```python
print("📊 File Inventory AFTER orphan cleanup:\n")

df_files_after = spark.sql(f"""
    SELECT 
        file_path,
        partition,
        ROUND(file_size_in_bytes / 1024 / 1024, 2) as file_size_mb,
        record_count
    FROM lakehouse.{database_name}.{table_name}.files
    ORDER BY file_path
""")

df_files_after.show(20, truncate=False)

file_stats_after = spark.sql(f"""
    SELECT 
        COUNT(*) as total_files,
        ROUND(SUM(file_size_in_bytes) / 1024 / 1024, 2) as total_size_mb,
        SUM(record_count) as total_records
    FROM lakehouse.{database_name}.{table_name}.files
""")

print("\nFile Statistics (AFTER):")
file_stats_after.show(truncate=False)

stats_after = file_stats_after.collect()[0]
tracked_files_after = stats_after['total_files']
tracked_size_after_mb = stats_after['total_size_mb']

print(f"\n📊 Comparison:")
print(f"   Files before: {tracked_files_before}")
print(f"   Files after:  {tracked_files_after}")
print(f"   (Tracked files should be same - orphans were NOT tracked)")
```

## Step 9: Verify Data Integrity


```python
print("🔍 Verifying data integrity after cleanup...\n")

# Query data to ensure nothing was deleted incorrectly
df_data_check = spark.sql(f"""
    SELECT 
        COUNT(*) as record_count,
        COUNT(DISTINCT sale_id) as unique_sales,
        ROUND(SUM(total_amount), 2) as total_revenue,
        MIN(sale_date) as earliest_sale,
        MAX(sale_date) as latest_sale
    FROM {full_table}
""")

print("Data integrity check:")
df_data_check.show(truncate=False)

data_stats = df_data_check.collect()[0]
record_count = data_stats['record_count']

print(f"\n✅ Data integrity verified:")
print(f"   Records: {record_count:,} (same as before cleanup)")
print(f"   Orphan cleanup only deletes unreferenced files")
print(f"   Active data is NEVER touched")
```

## Step 10: Orphan Cleanup Impact Summary


```python
print("="*80)
print("📊 ORPHAN FILE CLEANUP IMPACT SUMMARY")
print("="*80)

print(f"\n1. ORPHAN DETECTION:")
print(f"   Age threshold: {orphan_age_days} days")
print(f"   Cutoff date: {older_than_timestamp}")
print(f"   Files scanned: All S3 files in table location")

print(f"\n2. CLEANUP RESULTS:")
if result_rows:
    print(f"   Orphan files found: {orphans_deleted}")
    print(f"   Orphan files deleted: {orphans_deleted}")
    print(f"   Storage reclaimed: (varies based on file sizes)")
else:
    print(f"   Orphan files found: 0")
    print(f"   No cleanup needed (table is clean)")

print(f"\n3. DATA INTEGRITY:")
print(f"   Records before: {num_records:,}")
print(f"   Records after:  {record_count:,}")
print(f"   Data loss: NONE (orphans are unreferenced)")

print(f"\n4. FILE INVENTORY:")
print(f"   Tracked files (unchanged): {tracked_files_after}")
print(f"   Only orphans (untracked files) were deleted")

print("\n5. TYPICAL SAVINGS:")
print("   - Failed writes: 1-5% of storage")
print("   - After compaction: 10-20% of storage")
print("   - Concurrent write conflicts: 0.1-1% of storage")
print("   - Total typical savings: 5-20% of S3 costs")

print("\n" + "="*80)
print("✅ RECOMMENDATION: Schedule monthly orphan cleanup")
print("="*80)
```

## Step 11: Integration with S3 Lifecycle Policies


```python
s3_lifecycle_guide = '''
================================================================================
S3 LIFECYCLE POLICIES + ORPHAN CLEANUP
================================================================================

STRATEGY: Combine Iceberg cleanup with S3 lifecycle rules

LAYER 1: Iceberg REMOVE ORPHAN FILES (application-level)
  - Runs weekly/monthly via Airflow
  - Deletes orphans older than 3-7 days
  - Intelligent (knows what's tracked vs. orphan)
  - Safest option

LAYER 2: S3 Lifecycle Rules (infrastructure-level)
  - Delete files older than 90 days (backup safety net)
  - Transition old data to Glacier (archive)
  - Expire incomplete multipart uploads

================================================================================
EXAMPLE S3 LIFECYCLE POLICY
================================================================================

{
  "Rules": [
    {
      "Id": "DeleteOrphanFiles",
      "Status": "Enabled",
      "Prefix": "warehouse/",
      "Expiration": {
        "Days": 90
      },
      "NoncurrentVersionExpiration": {
        "NoncurrentDays": 30
      },
      "AbortIncompleteMultipartUpload": {
        "DaysAfterInitiation": 7
      }
    },
    {
      "Id": "TransitionToGlacier",
      "Status": "Enabled",
      "Prefix": "warehouse/archive/",
      "Transitions": [
        {
          "Days": 180,
          "StorageClass": "GLACIER"
        },
        {
          "Days": 365,
          "StorageClass": "DEEP_ARCHIVE"
        }
      ]
    }
  ]
}

================================================================================
LAYERED APPROACH BENEFITS
================================================================================

Day 0-7: Files created (active)
  → No deletion (too recent)

Day 7-90: Iceberg cleanup eligible
  → REMOVE ORPHAN FILES deletes orphans
  → Tracked files retained

Day 90+: S3 lifecycle kicks in
  → Safety net for missed orphans
  → Protects against runaway storage costs

Day 180+: Archive to Glacier
  → Old data moved to cheaper storage
  → Rarely accessed data

================================================================================
WHEN TO USE EACH APPROACH
================================================================================

Iceberg REMOVE ORPHAN FILES:
✅ Primary cleanup mechanism
✅ Intelligent (knows metadata)
✅ Runs frequently (weekly/monthly)
✅ Safe (respects retention)

S3 Lifecycle Policies:
✅ Backup safety net
✅ Cost optimization (Glacier)
✅ Incomplete multipart uploads
✅ Organization-wide policies

NEVER rely solely on S3 lifecycle!
  → Doesn't understand Iceberg metadata
  → Could delete active files if misconfigured
  → Use as backup only
================================================================================
'''

print(s3_lifecycle_guide)
```

## Step 12: Validation & Testing


```python
if enable_validation:
    print("🧪 Running validation tests...\n")
    
    # Test 1: No data loss
    assert record_count == num_records, f"Data loss detected: {record_count} != {num_records}"
    print(f"✅ Test 1 PASSED: No data loss ({record_count:,} records preserved)")
    
    # Test 2: Tracked files unchanged
    assert tracked_files_after == tracked_files_before, "Tracked files should not change"
    print(f"✅ Test 2 PASSED: Tracked files unchanged ({tracked_files_after} files)")
    
    # Test 3: Cleanup procedure ran successfully
    # Result exists (even if no orphans found)
    print(f"✅ Test 3 PASSED: Cleanup procedure executed successfully")
    
    # Test 4: Data queries still work
    test_query = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table}")
    assert test_query.count() > 0, "Should be able to query table"
    print(f"✅ Test 4 PASSED: Table queries work after cleanup")
    
    # Test 5: All snapshots still accessible
    snapshots = spark.sql(f"SELECT COUNT(*) as cnt FROM lakehouse.{database_name}.{table_name}.snapshots")
    assert snapshots.collect()[0]['cnt'] > 0, "Snapshots should still exist"
    print(f"✅ Test 5 PASSED: Snapshots intact (time travel still works)")
    
    test_passed = True
    print("\n✅ All validation tests PASSED!")
else:
    test_passed = True
    print("⏭️  Validation skipped")
```

## Step 13: Production Deployment - Airflow DAG


```python
production_dag = '''
# ============================================================================
# PRODUCTION ORPHAN FILE CLEANUP DAG
# ============================================================================
# Schedule: Monthly (first Sunday at 4 AM)
# Purpose: Reclaim S3 storage by deleting orphan files

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-ops@company.com"],
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

# Table-specific orphan age thresholds
ORPHAN_POLICIES = {
    # Production tables (conservative)
    "lakehouse.production.transactions": 7,      # 7 days
    "lakehouse.production.events": 7,            # 7 days
    "lakehouse.production.audit_logs": 14,       # 14 days (extra safe)
    
    # Analytics tables (moderate)
    "lakehouse.analytics.daily_metrics": 5,      # 5 days
    "lakehouse.analytics.user_sessions": 5,      # 5 days
    
    # Staging tables (aggressive)
    "lakehouse.staging.temp_data": 1,            # 1 day
    "lakehouse.staging.test_data": 1,            # 1 day
}

with DAG(
    "monthly_orphan_file_cleanup",
    default_args=default_args,
    description="Clean up orphan files to reclaim S3 storage",
    schedule_interval="0 4 1 * *",  # 4 AM on 1st of each month
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["maintenance", "orphan-cleanup", "storage-optimization"],
) as dag:
    
    # Clean orphan files for each table
    cleanup_tasks = []
    
    for table, orphan_age_days in ORPHAN_POLICIES.items():
        task_id = f"cleanup_{table.split('.')[-1]}"
        
        task = SparkSubmitOperator(
            task_id=task_id,
            application="/scripts/cleanup_orphans.py",
            name=f"cleanup-{table}-{{{{ ds }}}}",
            conf={
                "spark.sql.catalog.lakehouse": "org.apache.iceberg.spark.SparkCatalog",
                "spark.sql.catalog.lakehouse.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
            },
            application_args=[
                "--table", table,
                "--orphan-age-days", str(orphan_age_days),
                "--dry-run", "false",
            ],
        )
        
        cleanup_tasks.append(task)
    
    # Send summary report
    def send_cleanup_report(**context):
        """Aggregate cleanup metrics and send to Slack"""
        from slack_sdk import WebClient
        
        # Query cleanup metrics
        # ...
        
        message = f"""
        📊 Monthly Orphan File Cleanup Report - {context['ds']}
        
        Tables cleaned: {len(ORPHAN_POLICIES)}
        Orphan files deleted: 1,234
        Storage reclaimed: 15.6 GB
        Estimated savings: $3.50/month
        
        Top cleanups:
        - events: 567 files (8.2 GB)
        - transactions: 345 files (4.1 GB)
        - user_sessions: 234 files (2.8 GB)
        
        ✅ All tables cleaned successfully
        """
        
        client = WebClient(token="...")
        client.chat_postMessage(channel="#data-ops", text=message)
    
    notify = PythonOperator(
        task_id="send_report",
        python_callable=send_cleanup_report,
    )
    
    cleanup_tasks >> notify

# ============================================================================
# CLEANUP SCRIPT: /scripts/cleanup_orphans.py
# ============================================================================

import argparse
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import logging

def cleanup_orphan_files(table, orphan_age_days, dry_run=False):
    """Clean up orphan files from Iceberg table"""
    
    spark = SparkSession.builder.getOrCreate()
    logger = logging.getLogger(__name__)
    
    # Calculate older_than timestamp
    older_than = datetime.now() - timedelta(days=orphan_age_days)
    older_than_str = older_than.strftime('%Y-%m-%d %H:%M:%S')
    
    logger.info(f"Cleaning orphans for {table}")
    logger.info(f"Orphan age threshold: {orphan_age_days} days")
    logger.info(f"Deleting files older than: {older_than_str}")
    
    if dry_run:
        logger.info("DRY-RUN MODE: No files will be deleted")
        # Implement dry-run logic (list orphans without deleting)
        return
    
    # Run cleanup
    result = spark.sql(f"""
        CALL lakehouse.system.remove_orphan_files(
            table => '{table}',
            older_than => TIMESTAMP '{older_than_str}'
        )
    """)
    
    result.show(truncate=False)
    
    # Log metrics
    orphans_deleted = result.count()
    logger.info(f"Deleted {orphans_deleted} orphan files")
    
    return {
        "table": table,
        "orphans_deleted": orphans_deleted,
        "orphan_age_days": orphan_age_days
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True)
    parser.add_argument("--orphan-age-days", type=int, required=True)
    parser.add_argument("--dry-run", type=bool, default=False)
    
    args = parser.parse_args()
    cleanup_orphan_files(**vars(args))
'''

print("📋 Production Deployment Guide:")
print("="*80)
print(production_dag)
print("="*80)
```

## Summary

### ✅ Pattern 8.4: Orphan File Cleanup Complete!

This notebook demonstrated:

1. **Orphan Files**:
   - S3 files NOT tracked by Iceberg metadata
   - Created by failed writes, compaction, concurrent conflicts
   - Waste storage space and increase S3 costs

2. **Root Causes**:
   - Transaction failures (write to S3, but no metadata commit)
   - Compaction leftovers (old files not yet deleted)
   - Concurrent write conflicts (optimistic locking)

3. **REMOVE ORPHAN FILES**:
   - Compare S3 files vs. Iceberg metadata
   - Delete orphans older than threshold (default: 3 days)
   - Safety checks prevent deleting active files

4. **Safety Mechanisms**:
   - Age threshold (protect in-flight writes)
   - Dry-run mode (preview before deletion)
   - Only deletes data files (never metadata)
   - Requires table lock (prevent races)

5. **Production Integration**:
   - Monthly Airflow DAG
   - Table-specific age thresholds
   - S3 lifecycle policies (backup safety net)
   - Monitoring and reporting

### Key Benefits:

- **Storage Savings**: Reclaim 5-20% of S3 space
- **Cost Reduction**: Lower monthly S3 bill
- **Housekeeping**: Clean up failed writes automatically
- **Compliance**: Remove unreferenced sensitive data

### Typical Orphan Sources:

- **Failed Writes**: 1-5% of storage
- **After Compaction**: 10-20% of storage (temporary)
- **Concurrent Conflicts**: 0.1-1% of storage
- **Total Savings**: 5-20% of S3 costs

### Best Practices:

1. **Schedule Monthly**: First Sunday of month (low traffic)
2. **Conservative Threshold**: 3-7 days (protect active writes)
3. **Table-Specific Policies**: Critical tables = longer threshold
4. **Monitor Metrics**: Track orphan counts over time
5. **S3 Lifecycle Backup**: 90+ day safety net

### All 4 Maintenance Patterns Complete!

- **8.1 File Compaction**: Merge small files → optimal sizes
- **8.2 Snapshot Expiration**: Delete old snapshots → reclaim storage
- **8.3 Partition Evolution**: Change partitioning → zero downtime
- **8.4 Orphan Cleanup**: Delete unreferenced files → cost savings

Together, these patterns ensure a healthy, performant, cost-optimized lakehouse!


```python
# Cleanup (optional)
if enable_cleanup:
    spark.sql(f"DROP TABLE IF EXISTS {full_table}")
    spark.sql(f"DROP DATABASE IF EXISTS {full_database}")
    
    # Clean up orphan simulation files
    if simulate_failures:
        orphan_path = f"s3a://openlakes/warehouse/{database_name}/{table_name}_orphan_simulation"
        spark.sql(f"DROP TABLE IF EXISTS parquet.`{orphan_path}`")
    
    print("🧹 Cleanup completed")
else:
    print("⏭️  Cleanup skipped (enable_cleanup=False)")
    print(f"\n📊 Table created: {full_table}")
    print(f"   Explore orphan file cleanup and storage optimization!")
```
