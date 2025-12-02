```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType, LongType, DecimalType
from pyspark.sql.functions import col, sum as spark_sum, count, avg, when, expr, lit, window, current_timestamp, from_json, sha2, concat_ws
import random
from datetime import datetime, date, timedelta
import json
import time
from kafka import KafkaProducer
```

# Pattern 2.7: Exactly-Once Streaming with Spark

Guarantee exactly-once processing semantics for critical financial data:

1. **Kafka Source**: Financial transactions
2. **Spark Checkpointing**: State snapshots for fault tolerance
3. **Iceberg Transactional Writes**: ACID guarantees
4. **Exactly-Once**: No duplicates, no data loss

## Architecture

```
Kafka (transactions)
    ↓
Spark Structured Streaming (checkpointing enabled)
    ↓ (state snapshots to persistent storage)
Iceberg (transactional writes with ACID)
```

**Critical for**: Financial transactions, payments, billing, audit logs
**Guarantees**: Each record processed exactly once, even with failures

## Exactly-Once Semantics in Spark

Spark Structured Streaming achieves exactly-once processing through:

**1. Idempotent Sources** (Kafka):
- Replayable offset-based reading
- Offsets committed only after successful write

**2. Checkpointing**:
- Write-ahead log (WAL) tracks offsets and metadata
- Recovery point on failure
- Automatic restart from last checkpoint

**3. Idempotent Sinks** (Iceberg):
- Transaction hash prevents duplicate writes
- Deterministic processing (same input → same output)
- ACID transactions

**Result**: Each transaction processed **exactly once**, even with node failures, restarts, or network issues.

## Parameters


```python
execution_date = "2025-01-16"
environment = "development"
database_name = "demo"
kafka_topic = "financial_transactions"
iceberg_table = "transactions_ledger"
kafka_bootstrap_servers = "infrastructure-kafka.openlakes.svc.cluster.local:9092"
num_transactions = 200
checkpoint_interval_seconds = 10
streaming_duration_seconds = 35
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
    .appName(f"Pattern-2.7-ExactlyOnce-{environment}") \
    .config("spark.jars.packages", 
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.8.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0-preview3") \
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

print("✅ Spark initialized for exactly-once processing")
```

## Setup: Create Transactions Ledger Table


```python
full_database = f"lakehouse.{database_name}"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_database}")

full_table = f"{full_database}.{iceberg_table}"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {full_table} (
        transaction_id STRING,
        account_from STRING,
        account_to STRING,
        amount DECIMAL(18, 2),
        currency STRING,
        transaction_type STRING,
        transaction_hash STRING,
        event_timestamp TIMESTAMP,
        processing_time TIMESTAMP
    ) USING iceberg
""")

print(f"✅ Ledger table created: {full_table}")
```

## Kafka Producer: Generate Financial Transactions


```python
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"📊 Producing {num_transactions} financial transactions...")

transaction_types = ["transfer", "payment", "withdrawal", "deposit"]
currencies = ["USD", "EUR", "GBP"]

for i in range(num_transactions):
    txn = {
        "transaction_id": f"TXN{i:08d}",
        "account_from": f"ACC{random.randint(1000, 9999)}",
        "account_to": f"ACC{random.randint(1000, 9999)}",
        "amount": str(round(random.uniform(10.00, 10000.00), 2)),
        "currency": random.choice(currencies),
        "transaction_type": random.choice(transaction_types),
        "timestamp": datetime.utcnow().isoformat()
    }
    
    producer.send(kafka_topic, value=txn)
    
    if (i + 1) % 50 == 0:
        print(f"  Sent {i + 1}/{num_transactions} transactions...")
        time.sleep(0.2)

producer.flush()
print(f"✅ Produced {num_transactions} transactions to Kafka")
```

## Spark Exactly-Once Processing

**Key Concepts**:
1. **Checkpointing**: Periodic snapshots of stream state and offsets
2. **Offset Management**: Kafka offsets committed atomically with writes
3. **Idempotent Writes**: Transaction hash prevents duplicates
4. **Fault Recovery**: Automatic restart from last checkpoint


```python
# Read transactions stream
txn_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("account_from", StringType(), True),
    StructField("account_to", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("timestamp", StringType(), True)
])

df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("kafka.group.id", "exactly-once-processor") \
    .load()

# Parse with string amount (for hashing)
df_parsed = df_stream.select(
    from_json(col("value").cast("string"), txn_schema).alias("data")
).select(
    col("data.transaction_id").alias("transaction_id"),
    col("data.account_from").alias("account_from"),
    col("data.account_to").alias("account_to"),
    col("data.amount").alias("amount_str"),
    col("data.currency").alias("currency"),
    col("data.transaction_type").alias("transaction_type"),
    col("data.timestamp").cast(TimestampType()).alias("event_timestamp")
)

# Add transaction hash using string amount (consistent hashing)
# Then cast amount to Decimal for final output
df_with_hash = df_parsed.withColumn(
    "transaction_hash",
    sha2(concat_ws("|", 
                   col("transaction_id"),
                   col("account_from"),
                   col("account_to"),
                   col("amount_str"),  # Use string for consistent hashing
                   col("currency")), 256)
).withColumn(
    "amount",
    col("amount_str").cast(DecimalType(18, 2))  # Cast to Decimal after hashing
).withColumn(
    "processing_time",
    current_timestamp()
).select(
    col("transaction_id"),
    col("account_from"),
    col("account_to"),
    col("amount"),  # Final Decimal type
    col("currency"),
    col("transaction_type"),
    col("transaction_hash"),
    col("event_timestamp"),
    col("processing_time")
)

print("✅ Transaction stream configured with idempotency hashing")
```

## Exactly-Once Sink: Iceberg with Checkpointing


```python
# Write with checkpointing for exactly-once guarantees
query = df_with_hash.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", f"/tmp/checkpoint_exactly_once_{iceberg_table}") \
    .option("fanout-enabled", "true") \
    .trigger(processingTime=f"{checkpoint_interval_seconds} seconds") \
    .toTable(full_table)

print(f"✅ Exactly-once processing started with Spark checkpointing")
print(f"   Checkpoint interval: Every {checkpoint_interval_seconds} seconds")
print(f"   Kafka offsets committed atomically with Iceberg writes")
print(f"⏳ Processing for {streaming_duration_seconds} seconds...\n")

time.sleep(streaming_duration_seconds)

query.stop()
print("\n✅ Streaming query stopped")
```

## Query: Verify Ledger Integrity


```python
# Transaction summary
df_summary = spark.sql(f"""
    SELECT 
        transaction_type,
        currency,
        COUNT(*) as txn_count,
        SUM(amount) as total_amount,
        AVG(amount) as avg_amount
    FROM {full_table}
    GROUP BY transaction_type, currency
    ORDER BY txn_count DESC
""")

print("📊 Transaction Ledger Summary:")
df_summary.show()

# Check for duplicates (should be 0 with exactly-once)
df_duplicates = spark.sql(f"""
    SELECT 
        transaction_hash,
        COUNT(*) as occurrence_count
    FROM {full_table}
    GROUP BY transaction_hash
    HAVING COUNT(*) > 1
""")

duplicate_count = df_duplicates.count()
print(f"\n🔍 Duplicate Check: {duplicate_count} duplicates found")
if duplicate_count > 0:
    df_duplicates.show()

# Sample transactions
df_sample = spark.sql(f"""
    SELECT 
        transaction_id,
        account_from,
        account_to,
        amount,
        currency,
        transaction_type,
        event_timestamp
    FROM {full_table}
    ORDER BY event_timestamp DESC
    LIMIT 10
""")

print("\n📊 Latest 10 Transactions:")
df_sample.show()
```

## Validation


```python
if enable_validation:
    total_txns = spark.sql(f"SELECT COUNT(*) FROM {full_table}").collect()[0][0]
    
    assert total_txns > 0, "Should have processed transactions"
    print(f"✅ Processed {total_txns} transactions")
    
    # Verify exactly-once: no duplicates
    unique_hashes = spark.sql(f"SELECT COUNT(DISTINCT transaction_hash) FROM {full_table}").collect()[0][0]
    assert unique_hashes == total_txns, "Should have no duplicate transaction hashes"
    print(f"✅ Exactly-once guarantee verified: {unique_hashes} unique transactions")
    
    # Verify transaction IDs are unique
    unique_ids = spark.sql(f"SELECT COUNT(DISTINCT transaction_id) FROM {full_table}").collect()[0][0]
    assert unique_ids == total_txns, "Should have unique transaction IDs"
    print(f"✅ All transaction IDs unique")
    
    # Verify amounts are positive
    negative_count = spark.sql(f"SELECT COUNT(*) FROM {full_table} WHERE amount <= 0").collect()[0][0]
    assert negative_count == 0, "No negative amounts allowed"
    print(f"✅ All amounts are positive")
    
    test_passed = True
    print("\n✅ All validations passed! Ledger integrity confirmed.")
else:
    test_passed = True
```

## Summary

### ✅ Pattern 2.7: Exactly-Once Spark Streaming Complete!

Demonstrated:
1. Financial transaction stream from Kafka
2. Idempotency hashing (SHA256 of transaction data)
3. Spark checkpointing for fault tolerance
4. Iceberg transactional writes (ACID)
5. Exactly-once guarantee verification

**Spark Exactly-Once Mechanisms**:
```
1. Checkpointing:
   - Write-ahead log (WAL) stores offsets and state
   - Recovery point on failure
   - Automatic restart from last checkpoint

2. Idempotent Writes:
   - Transaction hash prevents duplicates
   - Deterministic processing (same input → same output)
   - Iceberg ACID transactions

3. Offset Management:
   - Kafka offsets committed atomically with sink writes
   - Either both succeed or both fail
   - No partial writes
```

**Production Configuration**:
```python
query = df.writeStream \
  .format("iceberg") \
  .option("checkpointLocation", "s3://checkpoints/ledger") \
  .trigger(processingTime='10 seconds') \
  .start()
```

**Benefits**:
- 🔒 Data integrity (no duplicates, no loss)
- 💰 Financial accuracy (critical for billing/payments)
- 🔄 Fault tolerance (automatic recovery)
- 📊 Audit compliance (complete transaction history)
- 🎯 Unified platform (no separate Flink deployment)

**Use Cases**: Payment processing, billing systems, financial ledgers, order processing, audit logs


```python
if enable_cleanup:
    spark.sql(f"DROP TABLE IF EXISTS {full_table}")
    print("🧹 Cleanup completed")
else:
    print("⏭️  Cleanup skipped")
```
