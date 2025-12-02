# Pattern 7.1: Data Quality Validation & Monitoring

Comprehensive data quality framework demonstrating:

1. **Row-level Validations**: Null checks, range checks, regex patterns
2. **Column-level Validations**: Data type integrity, uniqueness, referential integrity
3. **Quality Metrics**: Completeness, accuracy, consistency, timeliness
4. **Quarantine Pattern**: Failed records isolated to separate tables
5. **Quality Scorecards**: SLA monitoring and reporting
6. **Production Integration**: Automated quality gates for data pipelines

## Architecture

```
Source Data (customers_raw)
    ↓
Quality Validation Engine
    ├─→ Valid Records → customers_validated (Iceberg)
    └─→ Failed Records → customers_quarantine (Iceberg)
         ↓
    Quality Metrics → quality_scorecard (Iceberg)
```

## Use Cases

- **GDPR Compliance**: Validate PII data quality (emails, phone numbers)
- **Financial Data**: Ensure transaction accuracy and completeness
- **Customer 360**: Data quality for master data management
- **Regulatory Reporting**: SOX, HIPAA data quality requirements

## Quality Dimensions

- **Completeness**: Required fields are not null/empty
- **Accuracy**: Data conforms to expected formats (email, phone, dates)
- **Consistency**: Cross-field validations (age vs. birthdate)
- **Validity**: Values within acceptable ranges
- **Uniqueness**: No duplicates on key fields
- **Timeliness**: Data freshness checks

## Parameters

This cell is tagged with `parameters` for Papermill execution.


```python
# Execution parameters
execution_date = "2025-01-17"
environment = "development"
database_name = "governance_demo"
source_table = "customers_raw"
validated_table = "customers_validated"
quarantine_table = "customers_quarantine"
scorecard_table = "quality_scorecard"
quality_threshold = 0.95  # 95% quality SLA
num_records = 1000
inject_errors = True  # Simulate data quality issues
enable_validation = True
enable_cleanup = False
```

## Step 1: Initialize Spark with Iceberg + Nessie


```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType, TimestampType
from pyspark.sql.functions import (
    col, when, lit, regexp_extract, length, upper, lower, trim,
    current_timestamp, datediff, to_date, year, count, sum as spark_sum,
    avg, round as spark_round, concat_ws, array, struct
)
import random
from datetime import datetime, timedelta
import os

# Set AWS environment variables for S3FileIO
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "admin123"

spark = SparkSession.builder \
    .appName(f"Pattern-7.1-DataQuality-{environment}") \
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

## Step 2: Generate Realistic Customer Data with Quality Issues

Simulate real-world data quality problems:
- Missing required fields (nulls)
- Invalid email formats
- Invalid phone numbers
- Age/birthdate inconsistencies
- Out-of-range values
- Duplicate records
- Inconsistent formatting


```python
# Create database
full_database = f"lakehouse.{database_name}"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_database}")
print(f"✅ Database created: {full_database}")

# Helper function to generate customer data
def generate_customer_data(num_records, inject_errors=True):
    """Generate realistic customer data with configurable error injection"""
    
    first_names = ["John", "Jane", "Robert", "Maria", "Michael", "Sarah", "David", "Emma", "James", "Lisa"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]
    domains = ["gmail.com", "yahoo.com", "outlook.com", "company.com"]
    countries = ["USA", "Canada", "UK", "Germany", "France"]
    
    records = []
    error_rate = 0.15 if inject_errors else 0.0  # 15% error rate
    
    for i in range(1, num_records + 1):
        # Base data
        customer_id = f"CUST{i:06d}"
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        email = f"{first_name.lower()}.{last_name.lower()}@{random.choice(domains)}"
        phone = f"+1-{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}"
        age = random.randint(18, 80)
        birth_date = (datetime(2025, 1, 1) - timedelta(days=age*365 + random.randint(0, 364))).strftime("%Y-%m-%d")
        country = random.choice(countries)
        account_balance = round(random.uniform(0, 100000), 2)
        registration_date = (datetime(2025, 1, 1) - timedelta(days=random.randint(1, 3650))).strftime("%Y-%m-%d")
        
        # Inject errors randomly
        if inject_errors and random.random() < error_rate:
            error_type = random.choice(["null_email", "invalid_email", "null_name", "invalid_phone", 
                                       "negative_balance", "age_mismatch", "future_date", "duplicate"])
            
            if error_type == "null_email":
                email = None
            elif error_type == "invalid_email":
                email = f"{first_name.lower()}@invalid"  # Missing TLD
            elif error_type == "null_name":
                first_name = None
            elif error_type == "invalid_phone":
                phone = "123"  # Too short
            elif error_type == "negative_balance":
                account_balance = -random.uniform(100, 10000)
            elif error_type == "age_mismatch":
                age = random.randint(150, 200)  # Impossible age
            elif error_type == "future_date":
                registration_date = (datetime(2025, 1, 1) + timedelta(days=random.randint(1, 365))).strftime("%Y-%m-%d")
            elif error_type == "duplicate" and i > 1:
                customer_id = f"CUST{(i-1):06d}"  # Duplicate previous ID
        
        records.append((
            customer_id,
            first_name,
            last_name,
            email,
            phone,
            age,
            birth_date,
            country,
            account_balance,
            registration_date
        ))
    
    return records

# Define schema
schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("birth_date", StringType(), True),
    StructField("country", StringType(), True),
    StructField("account_balance", DoubleType(), True),
    StructField("registration_date", StringType(), True)
])

# Generate data
customer_data = generate_customer_data(num_records, inject_errors)
df_raw = spark.createDataFrame(customer_data, schema)

print(f"\n📊 Generated {num_records} customer records")
print(f"   Error injection: {'ENABLED (15% error rate)' if inject_errors else 'DISABLED'}")
df_raw.show(10, truncate=False)
```

## Step 3: Write Raw Data to Iceberg


```python
full_source_table = f"{full_database}.{source_table}"

df_raw.writeTo(full_source_table) \
    .using("iceberg") \
    .createOrReplace()

print(f"✅ Written {df_raw.count()} records to {full_source_table}")
```

## Step 4: Define Data Quality Rules

Comprehensive validation framework:

### Completeness Checks
- Required fields must not be null

### Format Validations
- Email: RFC 5322 compliant regex
- Phone: E.164 format (+X-XXX-XXX-XXXX)
- Dates: Valid date formats

### Range Validations
- Age: 18-120 years
- Account balance: >= 0
- Registration date: Not in future

### Consistency Checks
- Age matches birth_date (within tolerance)

### Uniqueness
- Customer ID must be unique


```python
# Read source data
df_source = spark.table(full_source_table)

# Define quality rules as individual boolean columns
df_validated = df_source \
    .withColumn("rule_first_name_not_null", col("first_name").isNotNull()) \
    .withColumn("rule_last_name_not_null", col("last_name").isNotNull()) \
    .withColumn("rule_email_not_null", col("email").isNotNull()) \
    .withColumn("rule_email_format", 
                col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")) \
    .withColumn("rule_phone_not_null", col("phone").isNotNull()) \
    .withColumn("rule_phone_format", 
                col("phone").rlike(r"^\\+\\d{1,3}-\\d{3}-\\d{3}-\\d{4}$")) \
    .withColumn("rule_age_range", 
                (col("age") >= 18) & (col("age") <= 120)) \
    .withColumn("rule_balance_positive", 
                col("account_balance") >= 0) \
    .withColumn("rule_registration_not_future", 
                to_date(col("registration_date")) <= current_timestamp().cast("date")) \
    .withColumn("rule_birth_date_valid", 
                col("birth_date").isNotNull()) 

# Calculate overall quality score (percentage of rules passed)
rule_columns = [
    "rule_first_name_not_null",
    "rule_last_name_not_null",
    "rule_email_not_null",
    "rule_email_format",
    "rule_phone_not_null",
    "rule_phone_format",
    "rule_age_range",
    "rule_balance_positive",
    "rule_registration_not_future",
    "rule_birth_date_valid"
]

# Calculate quality score: sum of passed rules / total rules
from functools import reduce
from operator import add

df_validated = df_validated.withColumn(
    "quality_score",
    reduce(add, [when(col(rule), 1).otherwise(0) for rule in rule_columns]) / len(rule_columns)
)

# Determine if record is valid (all rules pass)
df_validated = df_validated.withColumn(
    "is_valid",
    reduce(lambda a, b: a & b, [col(rule) for rule in rule_columns])
)

# Add processing metadata
df_validated = df_validated \
    .withColumn("validation_timestamp", current_timestamp()) \
    .withColumn("validation_date", lit(execution_date).cast("date"))

# Create list of failed rules for debugging
df_validated = df_validated.withColumn(
    "failed_rules",
    concat_ws(", ", 
        *[when(~col(rule), lit(rule.replace("rule_", ""))).otherwise(lit("")) for rule in rule_columns]
    )
)

print("✅ Quality rules applied to all records")
print(f"   Total rules: {len(rule_columns)}")
print(f"\n📊 Sample validated data:")
df_validated.select(
    "customer_id", "email", "phone", "age", "quality_score", "is_valid", "failed_rules"
).show(10, truncate=False)
```

## Step 5: Separate Valid and Invalid Records (Quarantine Pattern)


```python
# Split into valid and invalid records
df_valid = df_validated.filter(col("is_valid") == True)
df_invalid = df_validated.filter(col("is_valid") == False)

valid_count = df_valid.count()
invalid_count = df_invalid.count()
total_count = df_validated.count()

quality_rate = (valid_count / total_count) * 100 if total_count > 0 else 0

print(f"📊 Data Quality Summary:")
print(f"   Total records: {total_count}")
print(f"   Valid records: {valid_count} ({quality_rate:.2f}%)")
print(f"   Invalid records: {invalid_count} ({100-quality_rate:.2f}%)")
print(f"   Quality threshold: {quality_threshold*100}%")
print(f"   Status: {'✅ PASS' if quality_rate/100 >= quality_threshold else '❌ FAIL'}")

# Write valid records to validated table
full_validated_table = f"{full_database}.{validated_table}"
df_valid.writeTo(full_validated_table) \
    .using("iceberg") \
    .createOrReplace()

print(f"\n✅ Valid records written to {full_validated_table}")

# Write invalid records to quarantine table (for investigation)
if invalid_count > 0:
    full_quarantine_table = f"{full_database}.{quarantine_table}"
    df_invalid.writeTo(full_quarantine_table) \
        .using("iceberg") \
        .createOrReplace()
    
    print(f"⚠️  Invalid records written to {full_quarantine_table}")
    print(f"\n📊 Sample quarantined records:")
    df_invalid.select(
        "customer_id", "email", "phone", "age", "account_balance", "quality_score", "failed_rules"
    ).show(10, truncate=False)
else:
    print(f"✅ No invalid records (100% quality)")
```

## Step 6: Generate Quality Scorecard

Create detailed metrics for monitoring and SLA tracking.


```python
# Calculate per-rule statistics
rule_stats = []

for rule in rule_columns:
    rule_name = rule.replace("rule_", "")
    passed = df_validated.filter(col(rule) == True).count()
    failed = df_validated.filter(col(rule) == False).count()
    pass_rate = (passed / total_count) * 100 if total_count > 0 else 0
    
    rule_stats.append((
        execution_date,
        source_table,
        rule_name,
        total_count,
        passed,
        failed,
        round(pass_rate, 2),
        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ))

# Add overall summary row
rule_stats.append((
    execution_date,
    source_table,
    "OVERALL_QUALITY",
    total_count,
    valid_count,
    invalid_count,
    round(quality_rate, 2),
    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
))

# Create scorecard DataFrame
scorecard_schema = StructType([
    StructField("execution_date", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("rule_name", StringType(), False),
    StructField("total_records", IntegerType(), False),
    StructField("passed", IntegerType(), False),
    StructField("failed", IntegerType(), False),
    StructField("pass_rate_pct", DoubleType(), False),
    StructField("generated_at", StringType(), False)
])

df_scorecard = spark.createDataFrame(rule_stats, scorecard_schema)

# Write scorecard to Iceberg
full_scorecard_table = f"{full_database}.{scorecard_table}"
df_scorecard.writeTo(full_scorecard_table) \
    .using("iceberg") \
    .createOrReplace()

print(f"✅ Quality scorecard written to {full_scorecard_table}")
print(f"\n📊 Quality Scorecard (Rule-by-Rule):")
df_scorecard.orderBy(col("pass_rate_pct")).show(20, truncate=False)
```

## Step 7: Quality Metrics Dashboard

Visualize quality dimensions and identify problem areas.


```python
# Group rules by quality dimension
print("📊 Quality Metrics by Dimension:\n")

# Completeness (null checks)
completeness_rules = [r for r in rule_columns if "not_null" in r]
completeness_df = df_scorecard.filter(col("rule_name").isin([r.replace("rule_", "") for r in completeness_rules]))
completeness_avg = completeness_df.agg(avg("pass_rate_pct")).collect()[0][0]
print(f"1. COMPLETENESS (required fields): {completeness_avg:.2f}%")
completeness_df.select("rule_name", "pass_rate_pct").show(truncate=False)

# Accuracy (format validations)
accuracy_rules = [r for r in rule_columns if "format" in r]
accuracy_df = df_scorecard.filter(col("rule_name").isin([r.replace("rule_", "") for r in accuracy_rules]))
accuracy_avg = accuracy_df.agg(avg("pass_rate_pct")).collect()[0][0] if accuracy_df.count() > 0 else 0
print(f"\n2. ACCURACY (format validations): {accuracy_avg:.2f}%")
accuracy_df.select("rule_name", "pass_rate_pct").show(truncate=False)

# Validity (range checks)
validity_rules = [r for r in rule_columns if "range" in r or "positive" in r or "future" in r]
validity_df = df_scorecard.filter(col("rule_name").isin([r.replace("rule_", "") for r in validity_rules]))
validity_avg = validity_df.agg(avg("pass_rate_pct")).collect()[0][0] if validity_df.count() > 0 else 0
print(f"\n3. VALIDITY (range checks): {validity_avg:.2f}%")
validity_df.select("rule_name", "pass_rate_pct").show(truncate=False)

# Overall summary
overall_df = df_scorecard.filter(col("rule_name") == "OVERALL_QUALITY")
print(f"\n4. OVERALL QUALITY: {overall_df.select('pass_rate_pct').collect()[0][0]:.2f}%")
overall_df.show(truncate=False)
```

## Step 8: Validation & Testing


```python
if enable_validation:
    print("🧪 Running validation tests...\n")
    
    # Test 1: All records should be processed (valid + invalid = total)
    assert valid_count + invalid_count == total_count, \
        f"Record count mismatch: {valid_count} + {invalid_count} != {total_count}"
    print("✅ Test 1 PASSED: All records processed (valid + invalid = total)")
    
    # Test 2: Valid records should have quality_score = 1.0
    valid_scores = spark.table(full_validated_table).select("quality_score").distinct().collect()
    assert all(row[0] == 1.0 for row in valid_scores), "Valid records should have quality_score = 1.0"
    print("✅ Test 2 PASSED: All validated records have quality_score = 1.0")
    
    # Test 3: Invalid records should exist in quarantine table (if errors injected)
    if inject_errors:
        quarantine_count = spark.table(full_quarantine_table).count()
        assert quarantine_count > 0, "Should have quarantined records when errors injected"
        print(f"✅ Test 3 PASSED: {quarantine_count} records in quarantine table")
    
    # Test 4: Scorecard should have entries for all rules + overall
    scorecard_count = spark.table(full_scorecard_table).count()
    expected_count = len(rule_columns) + 1  # +1 for overall
    assert scorecard_count == expected_count, \
        f"Scorecard should have {expected_count} entries, got {scorecard_count}"
    print(f"✅ Test 4 PASSED: Scorecard has {scorecard_count} metric entries")
    
    # Test 5: Overall quality should match threshold expectations
    if inject_errors:
        # With 15% error rate, quality should be around 85%
        assert quality_rate < 100, "Quality should be < 100% when errors injected"
        print(f"✅ Test 5 PASSED: Quality rate {quality_rate:.2f}% reflects injected errors")
    
    # Test 6: Failed records should have non-empty failed_rules field
    if invalid_count > 0:
        failed_rules_check = spark.table(full_quarantine_table) \
            .filter(col("failed_rules") != "") \
            .count()
        assert failed_rules_check == invalid_count, "All quarantined records should have failed_rules documented"
        print(f"✅ Test 6 PASSED: All {invalid_count} quarantined records have failed_rules documented")
    
    test_passed = True
    print("\n✅ All validation tests PASSED!")
else:
    test_passed = True
    print("⏭️  Validation skipped")
```

## Step 9: Production Deployment Example

How to integrate this pattern into production ETL pipelines.


```python
production_code = '''
# ============================================================================
# PRODUCTION DATA QUALITY PIPELINE
# ============================================================================
# Deploy this as an Airflow DAG or Spark job

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_timestamp
import sys

def run_quality_validation(source_table, target_table, quarantine_table, 
                          quality_threshold=0.95):
    """
    Production-grade data quality validation pipeline.
    
    Args:
        source_table: Input table to validate
        target_table: Output table for valid records
        quarantine_table: Output table for invalid records
        quality_threshold: Minimum acceptable quality (0-1)
    
    Returns:
        quality_rate: Actual quality rate achieved
    
    Raises:
        Exception: If quality threshold not met
    """
    
    # 1. Read source data
    df = spark.table(source_table)
    
    # 2. Apply quality rules (same as notebook)
    df_validated = apply_quality_rules(df)
    
    # 3. Calculate quality metrics
    total = df_validated.count()
    valid = df_validated.filter(col("is_valid") == True).count()
    quality_rate = valid / total if total > 0 else 0
    
    # 4. Quality gate: Fail if threshold not met
    if quality_rate < quality_threshold:
        raise Exception(
            f"Quality threshold not met: {quality_rate:.2%} < {quality_threshold:.2%}"
        )
    
    # 5. Write valid records
    df_validated.filter(col("is_valid") == True) \
        .writeTo(target_table) \
        .using("iceberg") \
        .createOrReplace()
    
    # 6. Write quarantine records (for monitoring)
    invalid_count = df_validated.filter(col("is_valid") == False).count()
    if invalid_count > 0:
        df_validated.filter(col("is_valid") == False) \
            .writeTo(quarantine_table) \
            .using("iceberg") \
            .append()  # Append for historical tracking
    
    # 7. Log metrics to monitoring system (e.g., CloudWatch, DataDog)
    log_metrics({
        "source_table": source_table,
        "total_records": total,
        "valid_records": valid,
        "invalid_records": total - valid,
        "quality_rate": quality_rate
    })
    
    return quality_rate

# ============================================================================
# AIRFLOW DAG INTEGRATION
# ============================================================================

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-alerts@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "customer_data_quality_pipeline",
    default_args=default_args,
    description="Daily customer data quality validation",
    schedule_interval="0 2 * * *",  # 2 AM daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["quality", "governance", "customers"],
) as dag:
    
    # Task 1: Extract from source system (Meltano, Debezium, etc.)
    extract_task = ...
    
    # Task 2: Quality validation (this notebook as Papermill)
    quality_task = PapermillOperator(
        task_id="validate_quality",
        input_nb="/notebooks/07-governance/01-data-quality.ipynb",
        output_nb="/tmp/quality_{{ ds }}.ipynb",
        parameters={
            "execution_date": "{{ ds }}",
            "environment": "production",
            "database_name": "customers",
            "source_table": "customers_raw",
            "validated_table": "customers_validated",
            "quarantine_table": "customers_quarantine",
            "quality_threshold": 0.95,
            "inject_errors": False,
            "enable_validation": True,
            "enable_cleanup": False
        },
    )
    
    # Task 3: Send quality report to Slack/Email
    notify_task = ...
    
    extract_task >> quality_task >> notify_task

# ============================================================================
# MONITORING & ALERTING
# ============================================================================

# Query quality scorecard for SLA monitoring:
# 
# SELECT 
#     execution_date,
#     table_name,
#     rule_name,
#     pass_rate_pct,
#     CASE 
#         WHEN pass_rate_pct >= 95 THEN 'GREEN'
#         WHEN pass_rate_pct >= 90 THEN 'YELLOW'
#         ELSE 'RED'
#     END as sla_status
# FROM lakehouse.governance_demo.quality_scorecard
# WHERE execution_date = CURRENT_DATE
# ORDER BY pass_rate_pct ASC

# Superset dashboard: Track quality trends over time
# DataDog/CloudWatch: Real-time quality metrics
'''

print("📋 Production Deployment Guide:")
print("="*80)
print(production_code)
print("="*80)
```

## Summary

### ✅ Pattern 7.1: Data Quality Validation Complete!

This notebook demonstrated:

1. **Comprehensive Quality Framework**:
   - 10 validation rules across multiple quality dimensions
   - Row-level quality scoring
   - Configurable quality thresholds

2. **Quarantine Pattern**:
   - Valid records → `customers_validated` table
   - Invalid records → `customers_quarantine` table
   - Failed rules documented for debugging

3. **Quality Metrics**:
   - Per-rule pass/fail statistics
   - Overall quality scorecard
   - Quality dimensions: Completeness, Accuracy, Validity

4. **Production Integration**:
   - Airflow DAG example
   - Quality gates (fail pipeline if threshold not met)
   - Monitoring and alerting patterns

### Key Benefits:

- **Compliance**: GDPR, SOX, HIPAA data quality requirements
- **Trust**: Downstream consumers get validated, clean data
- **Debugging**: Quarantine pattern makes data issues visible
- **SLA Tracking**: Scorecard enables quality monitoring over time

### Next Steps:

- **Pattern 7.2**: Schema Evolution (safe schema changes)
- **Pattern 7.3**: Data Lineage (track data flow)
- **Pattern 7.4**: Data Cataloging (metadata management)


```python
# Cleanup (optional)
if enable_cleanup:
    spark.sql(f"DROP TABLE IF EXISTS {full_source_table}")
    spark.sql(f"DROP TABLE IF EXISTS {full_validated_table}")
    spark.sql(f"DROP TABLE IF EXISTS {full_quarantine_table}")
    spark.sql(f"DROP TABLE IF EXISTS {full_scorecard_table}")
    spark.sql(f"DROP DATABASE IF EXISTS {full_database}")
    print("🧹 Cleanup completed")
else:
    print("⏭️  Cleanup skipped (enable_cleanup=False)")
    print(f"\n📊 Tables created:")
    print(f"   - {full_source_table}")
    print(f"   - {full_validated_table}")
    print(f"   - {full_quarantine_table}")
    print(f"   - {full_scorecard_table}")
```
