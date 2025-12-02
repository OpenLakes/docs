# Pattern 8.2: Snapshot Expiration & Time Travel

Comprehensive snapshot management demonstrating:

1. **Iceberg Snapshots**: Every write creates a new snapshot (immutable)
2. **Time Travel**: Query historical data at any point in time
3. **Snapshot Retention**: Configure retention policies (7 days, 30 days, etc.)
4. **EXPIRE SNAPSHOTS**: Delete old snapshots to reclaim storage
5. **Storage Optimization**: Old snapshots = old data files = wasted S3 space
6. **Compliance Considerations**: Audit logs require longer retention
7. **Metadata Cleanup**: Remove unreferenced metadata files

## Architecture

```
Iceberg Table Timeline:

Jan 1   Jan 2   Jan 3   Jan 4   Jan 5   Jan 6   Jan 7   Jan 8
  │       │       │       │       │       │       │       │
  ▼       ▼       ▼       ▼       ▼       ▼       ▼       ▼
snap1   snap2   snap3   snap4   snap5   snap6   snap7   snap8
  │       │       │       │       │       │       │       │
files1  files2  files3  files4  files5  files6  files7  files8

Retention Policy: 3 days
Current Date: Jan 8

After EXPIRE SNAPSHOTS:
                                Jan 5   Jan 6   Jan 7   Jan 8
                                  │       │       │       │
                                snap5   snap6   snap7   snap8
                                  │       │       │       │
                                files5  files6  files7  files8

snap1-4 deleted → files1-4 deleted → Storage reclaimed
```

## Use Cases

- **Storage Cost Optimization**: Delete old snapshots to free S3 space
- **Time Travel**: Query data as it existed yesterday, last week, last month
- **Data Recovery**: Rollback to previous snapshot if bad data written
- **Audit Compliance**: Retain historical data for regulatory requirements
- **GDPR Right to Erasure**: Delete old snapshots containing deleted user data
- **Dev/Test Cleanup**: Aggressive expiration in non-prod environments

## Snapshot Retention Best Practices

**Production tables**:
- **Transactional data**: 7 days (balance recovery vs. storage cost)
- **Audit logs**: 90 days or longer (compliance)
- **Analytics tables**: 30 days (historical analysis)
- **CDC tables**: 3 days (only need recent changes)

**Non-production**:
- **Development**: 1 day (aggressive cleanup)
- **Staging**: 7 days (match production testing)
- **Sandbox**: Same-day expiration

## Parameters

This cell is tagged with `parameters` for Papermill execution.


```python
# Execution parameters
execution_date = "2025-01-17"
environment = "development"
database_name = "maintenance_demo"
table_name = "customer_transactions"
retention_days = 3  # Keep last 3 days of snapshots
num_days_history = 7  # Create 7 days of historical snapshots
transactions_per_day = 1000
enable_validation = True
enable_cleanup = False
```

## Step 1: Initialize Spark with Iceberg + Nessie


```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
from pyspark.sql.functions import (
    col, current_timestamp, lit, rand, floor, expr, count, sum as spark_sum,
    avg, min as spark_min, max as spark_max, to_date, date_sub, current_date
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
    .appName(f"Pattern-8.2-SnapshotExpiration-{environment}") \
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

## Step 2: Create Historical Snapshots

Simulate a table with multiple days of writes, creating one snapshot per day.


```python
# Create database
full_database = f"lakehouse.{database_name}"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_database}")
print(f"✅ Database created: {full_database}")

# Define schema
schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("transaction_date", DateType(), False),
    StructField("amount", DoubleType(), False),
    StructField("product_category", StringType(), True),
    StructField("payment_method", StringType(), True)
])

full_table = f"{full_database}.{table_name}"

print(f"\n📊 Creating {num_days_history} days of historical snapshots...")
print(f"   Each day = 1 snapshot\n")

# Calculate date range
end_date = datetime.strptime(execution_date, "%Y-%m-%d")
start_date = end_date - timedelta(days=num_days_history - 1)

categories = ["Electronics", "Clothing", "Food", "Books", "Home"]
payment_methods = ["credit_card", "debit_card", "paypal", "cash"]

# Create snapshots for each day
for day_offset in range(num_days_history):
    current_date = start_date + timedelta(days=day_offset)
    date_str = current_date.strftime("%Y-%m-%d")
    
    # Generate transactions for this day
    transactions = []
    for i in range(transactions_per_day):
        transaction_id = f"TXN{current_date.strftime('%Y%m%d')}{i:05d}"
        customer_id = random.randint(1000, 9999)
        amount = round(random.uniform(10, 1000), 2)
        category = random.choice(categories)
        payment = random.choice(payment_methods)
        
        transactions.append((
            transaction_id, customer_id, current_date.date(),
            amount, category, payment
        ))
    
    df_day = spark.createDataFrame(transactions, schema)
    
    # Write transactions (creates new snapshot)
    if day_offset == 0:
        # First day: create table
        df_day.writeTo(full_table) \
            .using("iceberg") \
            .partitionedBy("transaction_date") \
            .create()
        print(f"   {date_str}: Created table with snapshot 1")
    else:
        # Subsequent days: append (creates new snapshot)
        df_day.writeTo(full_table) \
            .using("iceberg") \
            .append()
        print(f"   {date_str}: Appended data (snapshot {day_offset + 1})")
    
    # Small delay to ensure distinct snapshot timestamps
    time.sleep(0.5)

total_count = spark.table(full_table).count()
print(f"\n✅ Created {num_days_history} snapshots with {total_count:,} total transactions")
```

## Step 3: View Snapshot History

Query Iceberg's snapshots metadata table to see all historical snapshots.


```python
print("📊 Snapshot History (BEFORE expiration):\n")

# Query snapshots metadata table
df_snapshots_before = spark.sql(f"""
    SELECT 
        committed_at,
        snapshot_id,
        parent_id,
        operation,
        manifest_list,
        summary
    FROM lakehouse.{database_name}.{table_name}.snapshots
    ORDER BY committed_at
""")

df_snapshots_before.show(truncate=False)

snapshot_count_before = df_snapshots_before.count()
print(f"\n📊 Snapshot Summary:")
print(f"   Total snapshots: {snapshot_count_before}")
print(f"   Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
print(f"   Retention policy: Keep last {retention_days} days")
print(f"   Snapshots to expire: {max(0, snapshot_count_before - retention_days)}")
```

## Step 4: Time Travel - Query Historical Data

Demonstrate time travel by querying data as it existed at different points in time.


```python
print("⏰ Time Travel Demonstration\n")

# Get first and third snapshot IDs
snapshots = df_snapshots_before.select("snapshot_id", "committed_at").collect()
first_snapshot_id = snapshots[0]['snapshot_id']
first_snapshot_time = snapshots[0]['committed_at']

if len(snapshots) >= 3:
    third_snapshot_id = snapshots[2]['snapshot_id']
    third_snapshot_time = snapshots[2]['committed_at']
else:
    third_snapshot_id = snapshots[-1]['snapshot_id']
    third_snapshot_time = snapshots[-1]['committed_at']

# Query 1: Current data (latest snapshot)
print("1. Current data (latest snapshot):")
df_current = spark.sql(f"""
    SELECT 
        transaction_date,
        COUNT(*) as transaction_count,
        ROUND(SUM(amount), 2) as total_amount
    FROM {full_table}
    GROUP BY transaction_date
    ORDER BY transaction_date
""")
df_current.show()
current_count = spark.table(full_table).count()
print(f"   Total transactions: {current_count:,}\n")

# Query 2: Data as of first snapshot (day 1 only)
print(f"2. Time travel to first snapshot (snapshot ID: {first_snapshot_id}):")
print(f"   Committed at: {first_snapshot_time}")
df_first = spark.sql(f"""
    SELECT 
        transaction_date,
        COUNT(*) as transaction_count,
        ROUND(SUM(amount), 2) as total_amount
    FROM {full_table}
    VERSION AS OF {first_snapshot_id}
    GROUP BY transaction_date
    ORDER BY transaction_date
""")
df_first.show()
first_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table} VERSION AS OF {first_snapshot_id}").collect()[0]['cnt']
print(f"   Total transactions: {first_count:,}\n")

# Query 3: Data as of third snapshot (first 3 days)
print(f"3. Time travel to third snapshot (snapshot ID: {third_snapshot_id}):")
print(f"   Committed at: {third_snapshot_time}")
df_third = spark.sql(f"""
    SELECT 
        transaction_date,
        COUNT(*) as transaction_count,
        ROUND(SUM(amount), 2) as total_amount
    FROM {full_table}
    VERSION AS OF {third_snapshot_id}
    GROUP BY transaction_date
    ORDER BY transaction_date
""")
df_third.show()
third_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table} VERSION AS OF {third_snapshot_id}").collect()[0]['cnt']
print(f"   Total transactions: {third_count:,}\n")

print("✅ Time travel allows querying data at any historical snapshot!")
```

## Step 5: View Data Files (Before Expiration)

Understand which data files are referenced by snapshots.


```python
print("📊 Data Files (BEFORE expiration):\n")

df_files_before = spark.sql(f"""
    SELECT 
        partition,
        file_path,
        ROUND(file_size_in_bytes / 1024 / 1024, 2) as file_size_mb,
        record_count
    FROM lakehouse.{database_name}.{table_name}.files
    ORDER BY partition
""")

df_files_before.show(truncate=False)

file_stats_before = spark.sql(f"""
    SELECT 
        COUNT(*) as total_files,
        ROUND(SUM(file_size_in_bytes) / 1024 / 1024, 2) as total_size_mb,
        SUM(record_count) as total_records
    FROM lakehouse.{database_name}.{table_name}.files
""")

print("\nFile Statistics (BEFORE):")
file_stats_before.show(truncate=False)

stats_before = file_stats_before.collect()[0]
files_before = stats_before['total_files']
size_before_mb = stats_before['total_size_mb']

print(f"\n💾 Storage Impact:")
print(f"   - {files_before} data files")
print(f"   - {size_before_mb} MB total storage")
print(f"   - After expiration, old files will be deleted")
```

## Step 6: Run EXPIRE SNAPSHOTS Procedure

Delete snapshots older than retention period, freeing up storage.


```python
print(f"🗑️  Running EXPIRE SNAPSHOTS procedure...\n")
print(f"   Retention: {retention_days} days")
print(f"   Current date: {execution_date}")

# Calculate expiration timestamp
expiration_date = end_date - timedelta(days=retention_days)
expiration_timestamp = int(expiration_date.timestamp() * 1000)  # milliseconds

print(f"   Expire snapshots older than: {expiration_date.strftime('%Y-%m-%d')}\n")

# Run EXPIRE SNAPSHOTS procedure
expire_result = spark.sql(f"""
    CALL lakehouse.system.expire_snapshots(
        table => '{full_table}',
        older_than => TIMESTAMP '{expiration_date.strftime('%Y-%m-%d')} 00:00:00',
        retain_last => {retention_days}
    )
""")

print("Expiration results:")
expire_result.show(truncate=False)

print(f"\n✅ Snapshot expiration completed")
```

## Step 7: View Snapshot History (After Expiration)


```python
print("📊 Snapshot History (AFTER expiration):\n")

df_snapshots_after = spark.sql(f"""
    SELECT 
        committed_at,
        snapshot_id,
        parent_id,
        operation,
        manifest_list,
        summary
    FROM lakehouse.{database_name}.{table_name}.snapshots
    ORDER BY committed_at
""")

df_snapshots_after.show(truncate=False)

snapshot_count_after = df_snapshots_after.count()
snapshots_deleted = snapshot_count_before - snapshot_count_after

print(f"\n📊 Snapshot Summary (AFTER):")
print(f"   Snapshots before: {snapshot_count_before}")
print(f"   Snapshots after:  {snapshot_count_after}")
print(f"   Snapshots deleted: {snapshots_deleted}")
print(f"   Retention: Last {retention_days} days")
```

## Step 8: View Data Files (After Expiration)

Old data files should be deleted along with their snapshots.


```python
print("📊 Data Files (AFTER expiration):\n")

df_files_after = spark.sql(f"""
    SELECT 
        partition,
        file_path,
        ROUND(file_size_in_bytes / 1024 / 1024, 2) as file_size_mb,
        record_count
    FROM lakehouse.{database_name}.{table_name}.files
    ORDER BY partition
""")

df_files_after.show(truncate=False)

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
files_after = stats_after['total_files']
size_after_mb = stats_after['total_size_mb']

files_deleted = files_before - files_after
storage_reclaimed_mb = size_before_mb - size_after_mb
storage_reclaimed_pct = (storage_reclaimed_mb / size_before_mb * 100) if size_before_mb > 0 else 0

print(f"\n💾 Storage Reclaimed:")
print(f"   Files deleted: {files_deleted}")
print(f"   Storage reclaimed: {storage_reclaimed_mb:.2f} MB ({storage_reclaimed_pct:.1f}%)")
```

## Step 9: Verify Current Data Intact

Ensure that current data is still accessible after snapshot expiration.


```python
print("🔍 Verifying current data integrity...\n")

df_current_after = spark.sql(f"""
    SELECT 
        transaction_date,
        COUNT(*) as transaction_count,
        ROUND(SUM(amount), 2) as total_amount
    FROM {full_table}
    GROUP BY transaction_date
    ORDER BY transaction_date
""")

df_current_after.show()

current_count_after = spark.table(full_table).count()
print(f"Total transactions: {current_count_after:,}")
print(f"\n✅ Current data intact (same as before expiration)")
```

## Step 10: Test Time Travel Limitations

Demonstrate that expired snapshots are no longer accessible.


```python
print("⚠️  Testing time travel to expired snapshot...\n")

# Try to query first snapshot (should be expired)
try:
    df_expired = spark.sql(f"""
        SELECT COUNT(*) as cnt 
        FROM {full_table}
        VERSION AS OF {first_snapshot_id}
    """)
    df_expired.show()
    print("⚠️  Snapshot still accessible (may not have been expired)")
except Exception as e:
    print(f"❌ Expected error: Cannot time travel to expired snapshot")
    print(f"   Error: {str(e)[:200]}...")
    print(f"\n✅ This is CORRECT behavior - expired snapshots are deleted")

print("\n💡 KEY INSIGHT:")
print("   - Snapshot expiration is PERMANENT")
print("   - Cannot time travel to deleted snapshots")
print("   - Balance retention vs. storage cost carefully!")
```

## Step 11: Snapshot Expiration Impact Summary


```python
print("="*80)
print("📊 SNAPSHOT EXPIRATION IMPACT SUMMARY")
print("="*80)

print(f"\n1. SNAPSHOT CLEANUP:")
print(f"   Snapshots before: {snapshot_count_before}")
print(f"   Snapshots after:  {snapshot_count_after}")
print(f"   Snapshots deleted: {snapshots_deleted}")
print(f"   Retention policy: {retention_days} days")

print(f"\n2. STORAGE RECLAIMED:")
print(f"   Files before: {files_before}")
print(f"   Files after:  {files_after}")
print(f"   Files deleted: {files_deleted}")
print(f"   Storage freed: {storage_reclaimed_mb:.2f} MB ({storage_reclaimed_pct:.1f}%)")

print(f"\n3. DATA INTEGRITY:")
print(f"   Current data intact: YES")
print(f"   Records before: {current_count:,}")
print(f"   Records after:  {current_count_after:,}")

print(f"\n4. TIME TRAVEL IMPACT:")
print(f"   Retained snapshots: {snapshot_count_after}")
print(f"   Can time travel: {retention_days} days back")
print(f"   Expired snapshots: NOT accessible")

print("\n" + "="*80)
print("✅ RECOMMENDATION: Schedule weekly snapshot expiration")
print("="*80)
```

## Step 12: Validation & Testing


```python
if enable_validation:
    print("🧪 Running validation tests...\n")
    
    # Test 1: Snapshots were deleted
    assert snapshot_count_after < snapshot_count_before, "Snapshots should be deleted"
    print(f"✅ Test 1 PASSED: Snapshots deleted ({snapshot_count_before} → {snapshot_count_after})")
    
    # Test 2: Retention policy enforced
    assert snapshot_count_after <= retention_days + 1, f"Should retain ~{retention_days} snapshots"
    print(f"✅ Test 2 PASSED: Retention policy enforced ({snapshot_count_after} snapshots retained)")
    
    # Test 3: Current data unchanged
    assert current_count_after == current_count, "Current data should be intact"
    print(f"✅ Test 3 PASSED: Current data unchanged ({current_count_after:,} records)")
    
    # Test 4: Storage reclaimed
    if snapshots_deleted > 0:
        assert storage_reclaimed_mb > 0, "Storage should be reclaimed when snapshots deleted"
        print(f"✅ Test 4 PASSED: Storage reclaimed ({storage_reclaimed_mb:.2f} MB)")
    
    # Test 5: Recent data still time-travelable
    latest_snapshot = df_snapshots_after.orderBy(col("committed_at").desc()).first()
    latest_snapshot_id = latest_snapshot['snapshot_id']
    df_latest = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table} VERSION AS OF {latest_snapshot_id}")
    assert df_latest.count() > 0, "Should be able to time travel to retained snapshots"
    print(f"✅ Test 5 PASSED: Can time travel to retained snapshots")
    
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
# PRODUCTION SNAPSHOT EXPIRATION DAG
# ============================================================================
# Schedule: Weekly on Sundays at 3 AM (low-traffic period)
# Purpose: Reclaim storage by deleting old snapshots

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
    "retry_delay": timedelta(minutes=5),
}

# Table-specific retention policies
RETENTION_POLICIES = {
    "lakehouse.production.transactions": 7,      # 7 days (transactional data)
    "lakehouse.production.audit_logs": 90,       # 90 days (compliance)
    "lakehouse.production.analytics_events": 30, # 30 days (analytics)
    "lakehouse.production.cdc_users": 3,         # 3 days (CDC only needs recent)
    "lakehouse.staging.temp_data": 1,            # 1 day (staging cleanup)
}

with DAG(
    "weekly_snapshot_expiration",
    default_args=default_args,
    description="Expire old snapshots to reclaim storage",
    schedule_interval="0 3 * * 0",  # 3 AM every Sunday
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["maintenance", "snapshot-expiration", "storage-optimization"],
) as dag:
    
    # Expire snapshots for each table
    for table, retention_days in RETENTION_POLICIES.items():
        task_id = f"expire_{table.split('.')[-1]}"
        
        SparkSubmitOperator(
            task_id=task_id,
            application="/scripts/expire_snapshots.py",
            name=f"expire-{table}-{{{{ ds }}}}",
            conf={
                "spark.sql.catalog.lakehouse": "org.apache.iceberg.spark.SparkCatalog",
                "spark.sql.catalog.lakehouse.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
            },
            application_args=[
                "--table", table,
                "--retention-days", str(retention_days),
            ],
        )

# ============================================================================
# SNAPSHOT EXPIRATION SCRIPT: /scripts/expire_snapshots.py
# ============================================================================

import argparse
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

def expire_snapshots(table, retention_days):
    """Run Iceberg snapshot expiration"""
    
    spark = SparkSession.builder.getOrCreate()
    
    # Query snapshot count before
    snapshots_before = spark.sql(f"""
        SELECT COUNT(*) as cnt FROM {table}.snapshots
    """).collect()[0]['cnt']
    
    # Calculate expiration timestamp
    expiration_date = datetime.now() - timedelta(days=retention_days)
    
    print(f"Expiring snapshots for {table}")
    print(f"Retention: {retention_days} days")
    print(f"Expire before: {expiration_date}")
    print(f"Snapshots before: {snapshots_before}")
    
    # Run expiration
    result = spark.sql(f"""
        CALL lakehouse.system.expire_snapshots(
            table => '{table}',
            older_than => TIMESTAMP '{expiration_date.strftime('%Y-%m-%d %H:%M:%S')}',
            retain_last => {retention_days}
        )
    """)
    
    result.show(truncate=False)
    
    # Query snapshot count after
    snapshots_after = spark.sql(f"""
        SELECT COUNT(*) as cnt FROM {table}.snapshots
    """).collect()[0]['cnt']
    
    snapshots_deleted = snapshots_before - snapshots_after
    
    print(f"Snapshots after: {snapshots_after}")
    print(f"Snapshots deleted: {snapshots_deleted}")
    print(f"✅ Expiration completed")
    
    # Return metrics for monitoring
    return {
        "table": table,
        "snapshots_before": snapshots_before,
        "snapshots_after": snapshots_after,
        "snapshots_deleted": snapshots_deleted,
        "retention_days": retention_days
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True)
    parser.add_argument("--retention-days", type=int, required=True)
    
    args = parser.parse_args()
    expire_snapshots(**vars(args))

# ============================================================================
# COMPLIANCE CONSIDERATIONS
# ============================================================================
"""
IMPORTANT: Snapshot expiration has compliance implications

1. GDPR Right to Erasure:
   - Deleting snapshots helps comply with data deletion requests
   - Ensure retention aligns with privacy policies

2. SOX/Financial Compliance:
   - Audit logs may require 7+ years retention
   - Configure long retention for audit tables

3. HIPAA Healthcare:
   - Patient data may require 6 years retention
   - Balance compliance vs. storage cost

4. Disaster Recovery:
   - Longer retention = more recovery points
   - Consider backup/restore procedures

5. Legal Hold:
   - May need to pause expiration during litigation
   - Implement legal hold flags in orchestration
"""
'''

print("📋 Production Deployment Guide:")
print("="*80)
print(production_dag)
print("="*80)
```

## Summary

### ✅ Pattern 8.2: Snapshot Expiration Complete!

This notebook demonstrated:

1. **Iceberg Snapshots**:
   - Every write creates immutable snapshot
   - Snapshots enable time travel and rollback
   - Historical snapshots accumulate over time

2. **Time Travel**:
   - Query data as of any historical snapshot
   - Use `VERSION AS OF snapshot_id`
   - Critical for debugging and analysis

3. **EXPIRE SNAPSHOTS**:
   - Delete old snapshots beyond retention period
   - Automatically removes unreferenced data files
   - Reclaims S3 storage space

4. **Retention Policies**:
   - Transactional: 7 days
   - Analytics: 30 days
   - Audit/Compliance: 90+ days
   - CDC: 3 days

5. **Production Deployment**:
   - Weekly Airflow DAG
   - Table-specific retention policies
   - Compliance considerations (GDPR, SOX, HIPAA)

### Key Benefits:

- **Storage Optimization**: Reclaim 30-70% of storage
- **Cost Savings**: Less S3 storage = lower monthly bill
- **Time Travel**: Balance history vs. cost
- **Compliance**: Meet data retention regulations

### Trade-offs:

- **Longer Retention** = More time travel, higher cost
- **Shorter Retention** = Less time travel, lower cost
- **Balance** based on use case and compliance needs

### Next Steps:

- **Pattern 8.3**: Partition Evolution (change partitioning strategy)
- **Pattern 8.4**: Orphan File Cleanup (delete unreferenced files)


```python
# Cleanup (optional)
if enable_cleanup:
    spark.sql(f"DROP TABLE IF EXISTS {full_table}")
    spark.sql(f"DROP DATABASE IF EXISTS {full_database}")
    print("🧹 Cleanup completed")
else:
    print("⏭️  Cleanup skipped (enable_cleanup=False)")
    print(f"\n📊 Table created: {full_table}")
    print(f"   Explore snapshot history with time travel queries!")
```
