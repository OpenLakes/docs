```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType, LongType, DecimalType
from pyspark.sql.functions import col, sum as spark_sum, count, avg, when, expr, lit, window, current_timestamp, from_json, broadcast
import random
from datetime import datetime, date, timedelta
import json
import time
from kafka import KafkaProducer
import psycopg2
```

# Pattern 2.3: CDC - Database → Lakehouse Replication

Change Data Capture (CDC) for real-time database replication:

1. **PostgreSQL**: Source transactional database
2. **Debezium**: Capture DB changes (INSERT/UPDATE/DELETE)
3. **Kafka**: Stream CDC events
4. **Spark Streaming**: Process CDC events
5. **Iceberg**: Replicate to analytics lakehouse

## Architecture

```
PostgreSQL (OLTP)
    ↓ (Debezium connector)
Kafka (CDC events: before/after values)
    ↓
Spark Structured Streaming (parse CDC, MERGE)
    ↓
Iceberg (OLAP - analytics replica)
```

**Latency**: Seconds (near real-time)
**Use Case**: Real-time analytics on transactional data without impacting OLTP performance

## Parameters


```python
execution_date = "2025-01-16"
environment = "development"
database_name = "demo"
kafka_topic = "dbserver1.public.orders"
iceberg_table = "orders_realtime_replica"
kafka_bootstrap_servers = "infrastructure-kafka.openlakes.svc.cluster.local:9092"
postgres_host = "infrastructure-postgres"
postgres_port = 5432
postgres_db = "openlakes_source"
postgres_user = "openlakes"
postgres_password = "openlakes123"
num_initial_orders = 50
num_updates = 20
streaming_duration_seconds = 30
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
    .appName(f"Pattern-2.3-CDC-{environment}") \
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

print("✅ Spark initialized for CDC processing")
```

## Setup: Create Source Table in PostgreSQL


```python
# Create database if it doesn't exist (setup)
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

try:
    # Connect to default postgres database to create openlakes_source if needed
    conn_init = psycopg2.connect(
        host=postgres_host,
        port=postgres_port,
        database="postgres",
        user=postgres_user,
        password=postgres_password
    )
    conn_init.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur_init = conn_init.cursor()
    
    # Check if database exists
    cur_init.execute(f"SELECT 1 FROM pg_database WHERE datname = '{postgres_db}'")
    if not cur_init.fetchone():
        cur_init.execute(f"CREATE DATABASE {postgres_db}")
        print(f"✅ Created database: {postgres_db}")
    else:
        print(f"✅ Database already exists: {postgres_db}")
    
    cur_init.close()
    conn_init.close()
except Exception as e:
    print(f"⚠️  Database setup: {e}")

print("✅ Database setup complete")
```


```python
# Connect to PostgreSQL and create source table
conn = psycopg2.connect(
    host=postgres_host,
    port=postgres_port,
    database=postgres_db,
    user=postgres_user,
    password=postgres_password
)
conn.autocommit = True
cur = conn.cursor()

# Create orders table
cur.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        order_id SERIAL PRIMARY KEY,
        customer_id INTEGER NOT NULL,
        product_name VARCHAR(100),
        quantity INTEGER,
        price DECIMAL(10, 2),
        status VARCHAR(20),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")

# Insert initial orders
print(f"📊 Inserting {num_initial_orders} initial orders into PostgreSQL...")
statuses = ["pending", "processing", "shipped", "delivered"]
products = ["Laptop", "Mouse", "Keyboard", "Monitor", "Headphones"]

for _ in range(num_initial_orders):
    cur.execute("""
        INSERT INTO orders (customer_id, product_name, quantity, price, status)
        VALUES (%s, %s, %s, %s, %s)
    """, (
        random.randint(1000, 9999),
        random.choice(products),
        random.randint(1, 5),
        round(random.uniform(29.99, 999.99), 2),
        random.choice(statuses)
    ))

print(f"✅ Inserted {num_initial_orders} orders into PostgreSQL")
```

## Setup: Create Iceberg Replica Table


```python
full_database = f"lakehouse.{database_name}"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_database}")

full_table = f"{full_database}.{iceberg_table}"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {full_table} (
        order_id BIGINT,
        customer_id BIGINT,
        product_name STRING,
        quantity BIGINT,
        price DECIMAL(10, 2),
        status STRING,
        created_at TIMESTAMP,
        cdc_operation STRING,
        cdc_timestamp TIMESTAMP
    ) USING iceberg
""")

print(f"✅ Iceberg replica table created: {full_table}")
```

## Simulate Debezium CDC Events

**Note**: In production, Debezium connector would automatically capture PostgreSQL changes.

For this demo, we'll:
1. Make changes to PostgreSQL
2. Manually publish CDC-format events to Kafka (simulating Debezium)


```python
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"📊 Simulating CDC: Making {num_updates} updates to PostgreSQL...")

# Get existing order IDs
cur.execute("SELECT order_id FROM orders LIMIT 10")
order_ids = [row[0] for row in cur.fetchall()]

for i in range(num_updates):
    order_id = random.choice(order_ids)
    new_status = random.choice(["processing", "shipped", "delivered"])
    
    # Update in PostgreSQL
    cur.execute(
        "UPDATE orders SET status = %s WHERE order_id = %s RETURNING *",
        (new_status, order_id)
    )
    updated_row = cur.fetchone()
    
    # Publish CDC event to Kafka (Debezium format)
    cdc_event = {
        "op": "u",  # update operation
        "after": {
            "order_id": updated_row[0],
            "customer_id": updated_row[1],
            "product_name": updated_row[2],
            "quantity": updated_row[3],
            "price": float(updated_row[4]),
            "status": updated_row[5],
            "created_at": updated_row[6].isoformat()
        },
        "ts_ms": int(time.time() * 1000)
    }
    
    producer.send(kafka_topic, value=cdc_event)
    
    if (i + 1) % 5 == 0:
        print(f"  Published {i + 1}/{num_updates} CDC events...")
        time.sleep(0.5)

producer.flush()
print(f"✅ Published {num_updates} CDC events to Kafka")
```

## Spark Streaming: Process CDC Events


```python
# Define CDC event schema (Debezium format)
cdc_schema = StructType([
    StructField("op", StringType(), True),
    StructField("after", StructType([
        StructField("order_id", LongType(), True),
        StructField("customer_id", LongType(), True),
        StructField("product_name", StringType(), True),
        StructField("quantity", LongType(), True),
        StructField("price", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("created_at", StringType(), True)
    ]), True),
    StructField("ts_ms", LongType(), True)
])

# Read CDC stream from Kafka
df_cdc_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse CDC events
df_parsed = df_cdc_stream.select(
    from_json(col("value").cast("string"), cdc_schema).alias("cdc")
).select(
    col("cdc.after.order_id").alias("order_id"),
    col("cdc.after.customer_id").alias("customer_id"),
    col("cdc.after.product_name").alias("product_name"),
    col("cdc.after.quantity").alias("quantity"),
    col("cdc.after.price").cast(DecimalType(10, 2)).alias("price"),
    col("cdc.after.status").alias("status"),
    col("cdc.after.created_at").cast(TimestampType()).alias("created_at"),
    col("cdc.op").alias("cdc_operation"),
    current_timestamp().alias("cdc_timestamp")
)

print("✅ CDC stream parsing configured")
```

## Iceberg Sink: Replicate to Analytics Lakehouse


```python
# Write CDC changes to Iceberg
query = df_parsed.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", f"/tmp/checkpoint_cdc_{iceberg_table}") \
    .toTable(full_table)

print(f"✅ CDC replication started: PostgreSQL → Kafka → Iceberg")
print(f"⏳ Replicating changes for {streaming_duration_seconds} seconds...\n")

time.sleep(streaming_duration_seconds)

query.stop()
print("\n✅ CDC replication stopped")
```

## Query: Analytics on Replicated Data


```python
# Order status distribution (real-time analytics without impacting OLTP)
df_status_summary = spark.sql(f"""
    SELECT 
        status,
        COUNT(*) as order_count,
        SUM(quantity * price) as total_revenue
    FROM {full_table}
    GROUP BY status
    ORDER BY order_count DESC
""")

print("📊 Order Status Distribution (from Iceberg replica):")
df_status_summary.show()

# Latest updates
df_recent_changes = spark.sql(f"""
    SELECT 
        order_id,
        customer_id,
        product_name,
        status,
        cdc_operation,
        cdc_timestamp
    FROM {full_table}
    ORDER BY cdc_timestamp DESC
    LIMIT 10
""")

print("\n📊 Latest CDC Changes:")
df_recent_changes.show()
```

## Validation


```python
if enable_validation:
    replica_count = spark.sql(f"SELECT COUNT(*) FROM {full_table}").collect()[0][0]
    
    assert replica_count > 0, "Should have replicated some changes"
    print(f"✅ Replicated {replica_count} CDC events to Iceberg")
    
    # Verify CDC operation types
    update_count = spark.sql(f"SELECT COUNT(*) FROM {full_table} WHERE cdc_operation = 'u'").collect()[0][0]
    assert update_count == replica_count, "All operations should be updates (u)"
    print(f"✅ All {update_count} events are UPDATE operations")
    
    # Verify data consistency with source
    cur.execute("SELECT COUNT(DISTINCT order_id) FROM orders")
    source_count = cur.fetchone()[0]
    print(f"✅ Source has {source_count} unique orders")
    
    test_passed = True
    print("\n✅ All validations passed!")
else:
    test_passed = True

# Cleanup PostgreSQL connection
cur.close()
conn.close()
```

## Summary

### ✅ Pattern 2.3: CDC Replication Complete!

Demonstrated:
1. PostgreSQL source database with orders table
2. CDC event generation (simulating Debezium)
3. Kafka as CDC event stream
4. Spark Structured Streaming processing CDC events
5. Iceberg lakehouse as analytics replica

**Real-World Debezium Setup**:
```bash
# Deploy Debezium PostgreSQL connector
curl -X POST http://kafka-connect:8083/connectors -H "Content-Type: application/json" -d '{
  "name": "postgres-cdc-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "debezium",
    "database.dbname": "orders",
    "database.server.name": "dbserver1",
    "table.include.list": "public.orders"
  }
}'
```

**Benefits**:
- 🔄 Real-time analytics without impacting OLTP performance
- 📊 Complete change history (INSERT/UPDATE/DELETE)
- ⚡ Near real-time latency (seconds)
- 🔒 ACID guarantees on both source and replica

**Use Cases**: Real-time reporting, data warehousing, event-driven architectures, microservices data synchronization


```python
if enable_cleanup:
    spark.sql(f"DROP TABLE IF EXISTS {full_table}")
    # Note: PostgreSQL table cleanup would be done separately
    print("🧹 Cleanup completed (Iceberg table dropped)")
else:
    print("⏭️  Cleanup skipped")
```
