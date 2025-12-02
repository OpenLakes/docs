```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType, LongType
from pyspark.sql.functions import col, sum as spark_sum, count, avg, when, expr, lit, window, current_timestamp, from_json
import random
from datetime import datetime, date, timedelta
import psycopg2

```

# Pattern 4.1: Operational + Analytical Federation

Use Trino to join operational data (PostgreSQL) with analytical data (Iceberg) in a single query:

1. **Operational Data**: Live orders in PostgreSQL
2. **Analytical Data**: Historical sales trends in Iceberg
3. **Federated Query**: Trino joins both sources

## Architecture

```
PostgreSQL (operational DB)
    ↓ (current orders, low latency)
Trino Federated Query
    ↓ (join)
Iceberg Lakehouse (analytical)
    ↓ (historical sales trends)
Unified Result Set
```

**Use Case**: Join live order status (PostgreSQL) with historical customer purchase patterns (Iceberg) to provide sales reps with complete customer context

**Benefits**:
- No ETL needed for ad-hoc joins
- Real-time operational data
- Historical analytical data
- Single SQL query

**When to Use**:
- Ad-hoc joins across operational and analytical systems
- Real-time dashboards combining current + historical
- Customer 360 views
- Sales analytics

## Parameters


```python
import os

from pyspark.sql import SparkSession
# Set AWS environment variables for S3FileIO
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "admin123"

spark = SparkSession.builder \
    .appName(f"Pattern-4.1-FederatedQuery-{environment}") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.8.0,org.postgresql:postgresql:42.7.1") \
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

print("✅ Spark initialized for federated query pattern")
```


```python
import os

from pyspark.sql import SparkSession
# Set AWS environment variables for S3FileIO
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "admin123"

spark = SparkSession.builder \
    .appName(f"Pattern-4.1-FederatedQuery-{environment}") \
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

print("✅ Spark initialized for federated query pattern")
```

## Setup: Create PostgreSQL Table (Operational)


```python
# Connect to PostgreSQL
conn = psycopg2.connect(
    host=postgres_host,
    port=postgres_port,
    database=postgres_database,
    user=postgres_user,
    password=postgres_password
)
cursor = conn.cursor()

# Create current orders table
cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {postgres_table} (
        order_id VARCHAR(50) PRIMARY KEY,
        customer_id VARCHAR(50),
        order_value DECIMAL(10, 2),
        order_status VARCHAR(20),
        order_timestamp TIMESTAMP
    )
""")

# Truncate if exists
cursor.execute(f"TRUNCATE TABLE {postgres_table}")

# Insert current orders
print(f"📊 Inserting {num_current_orders} current orders into PostgreSQL...")
for i in range(num_current_orders):
    order_id = f"order_{i+1:05d}"
    customer_id = f"customer_{random.randint(1, 20)}"
    order_value = round(random.uniform(50.0, 1000.0), 2)
    order_status = random.choice(["pending", "processing", "shipped"])
    order_timestamp = datetime.now() - timedelta(hours=random.randint(0, 48))
    
    cursor.execute(f"""
        INSERT INTO {postgres_table} (order_id, customer_id, order_value, order_status, order_timestamp)
        VALUES (%s, %s, %s, %s, %s)
    """, (order_id, customer_id, order_value, order_status, order_timestamp))

conn.commit()
print(f"✅ Created PostgreSQL table: {postgres_table} ({num_current_orders} orders)")
```

## Setup: Create Iceberg Table (Analytical)


```python
full_database = f"lakehouse.{database_name}"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_database}")

full_iceberg_table = f"{full_database}.{iceberg_table}"

# Create historical sales table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {full_iceberg_table} (
        customer_id STRING,
        total_lifetime_orders BIGINT,
        total_lifetime_value DOUBLE,
        avg_order_value DOUBLE,
        first_order_date DATE,
        last_order_date DATE,
        customer_tier STRING
    ) USING iceberg
""")

# Generate historical customer sales data
historical_data = []
for i in range(1, 21):  # 20 customers
    customer_id = f"customer_{i}"
    total_orders = random.randint(5, 50)
    total_value = round(random.uniform(500.0, 10000.0), 2)
    avg_value = round(total_value / total_orders, 2)
    first_date = (datetime.now() - timedelta(days=random.randint(365, 730))).date()
    last_date = (datetime.now() - timedelta(days=random.randint(1, 30))).date()
    tier = "Platinum" if total_value > 7000 else ("Gold" if total_value > 3000 else "Silver")
    
    historical_data.append((
        customer_id, total_orders, total_value, avg_value,
        first_date, last_date, tier
    ))

historical_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("total_lifetime_orders", LongType(), False),
    StructField("total_lifetime_value", DoubleType(), False),
    StructField("avg_order_value", DoubleType(), False),
    StructField("first_order_date", DateType(), False),
    StructField("last_order_date", DateType(), False),
    StructField("customer_tier", StringType(), False)
])

df_historical = spark.createDataFrame(historical_data, schema=historical_schema)
df_historical.writeTo(full_iceberg_table).using("iceberg").overwritePartitions()

print(f"✅ Created Iceberg table: {full_iceberg_table} (20 customers)")
```

## Federated Query 1: Spark (Joining Both Sources)

Spark can read from both PostgreSQL and Iceberg, then join them.


```python
# Read current orders from PostgreSQL
df_postgres = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_database}") \
    .option("dbtable", postgres_table) \
    .option("user", postgres_user) \
    .option("password", postgres_password) \
    .option("driver", "org.postgresql.Driver") \
    .load()

print("📊 Current Orders (PostgreSQL - Operational):")
df_postgres.select("order_id", "customer_id", "order_value", "order_status").show(10)

# Read historical sales from Iceberg
df_iceberg = spark.table(full_iceberg_table)

print("\n📊 Historical Sales (Iceberg - Analytical):")
df_iceberg.show(10)

# Federated join: Current orders + Historical customer context
df_federated = df_postgres.join(
    df_iceberg,
    on="customer_id",
    how="left"
).select(
    col("order_id"),
    col("customer_id"),
    col("order_value"),
    col("order_status"),
    col("customer_tier"),
    col("total_lifetime_value"),
    col("total_lifetime_orders"),
    col("avg_order_value").alias("historical_avg_order")
)

print("\n📊 Federated Query Result (Operational + Analytical):")
print("   (Current orders enriched with historical customer context)\n")
df_federated.show(20, truncate=False)

# Business insight: High-value customers with pending orders
df_high_value_pending = df_federated \
    .filter((col("customer_tier") == "Platinum") & (col("order_status") == "pending")) \
    .orderBy(col("order_value").desc())

print("\n💎 High-Value Customers (Platinum) with Pending Orders:")
df_high_value_pending.show(truncate=False)
```

## Federated Query 2: Trino SQL (Alternative)

Trino provides SQL-based federation without Spark code.

**Note**: This notebook demonstrates the pattern via Spark. In production, you would use Trino directly via:

```sql
-- Trino federated query
SELECT 
    o.order_id,
    o.customer_id,
    o.order_value,
    o.order_status,
    h.customer_tier,
    h.total_lifetime_value
FROM postgresql.public.current_orders o
LEFT JOIN iceberg.demo.historical_sales h
    ON o.customer_id = h.customer_id
WHERE h.customer_tier = 'Platinum'
```

**Trino Benefits**:
- No Spark job needed
- SQL-only (accessible to analysts)
- Multiple catalogs (PostgreSQL, Iceberg, MySQL, etc.)
- Sub-second query performance


```python
print("""\n💡 Trino Federated Query Pattern:

In production, use Trino for SQL-based federation:

```bash
# Connect to Trino
trino --server http://analytics-trino:8080 --catalog iceberg --schema demo

# Run federated query
SELECT 
    o.order_id,
    o.customer_id,
    o.order_value,
    h.customer_tier,
    h.total_lifetime_value,
    ROUND(o.order_value / h.avg_order_value * 100, 2) as pct_of_avg
FROM postgresql.public.current_orders o
LEFT JOIN iceberg.demo.historical_sales h
    ON o.customer_id = h.customer_id
WHERE o.order_status IN ('pending', 'processing')
ORDER BY h.total_lifetime_value DESC
LIMIT 20;
```

**Catalogs**:
- `postgresql`: Operational database (live orders)
- `iceberg`: Analytical lakehouse (historical trends)

**Use Cases**:
- Sales dashboards (Superset → Trino → PostgreSQL + Iceberg)
- Customer 360 views (operational + analytical context)
- Ad-hoc analyst queries (no ETL required)
""")
```

## Validation


```python
if enable_validation:
    federated_count = df_federated.count()
    postgres_count = df_postgres.count()
    
    assert federated_count == postgres_count, "Federated result should match PostgreSQL row count (left join)"
    print(f"✅ Federated query returned {federated_count} rows (matches PostgreSQL)")
    
    # Verify customer tier enrichment
    enriched_count = df_federated.filter(col("customer_tier").isNotNull()).count()
    assert enriched_count > 0, "Should have enriched rows with customer tier"
    print(f"✅ Enriched {enriched_count} orders with historical customer context")
    
    # Verify high-value customers exist
    platinum_count = df_federated.filter(col("customer_tier") == "Platinum").count()
    print(f"✅ Found {platinum_count} orders from Platinum customers")
    
    test_passed = True
    print("\n✅ All validations passed!")
else:
    test_passed = True
```

## Summary

### ✅ Pattern 4.1: Operational + Analytical Federation Complete!

Demonstrated:
1. **Operational Data**: Current orders in PostgreSQL (transactional DB)
2. **Analytical Data**: Historical sales in Iceberg (lakehouse)
3. **Federated Join**: Spark/Trino joins both sources in single query
4. **Business Value**: Enrich live orders with historical customer context

**Federated Query Benefits**:
- 🚀 No ETL required for ad-hoc joins
- ⚡ Real-time operational data (PostgreSQL)
- 📊 Historical analytical data (Iceberg)
- 🔍 Single SQL query across both systems
- 👥 Accessible to SQL analysts (via Trino)

**Spark vs. Trino**:

| Aspect | Spark | Trino |
|--------|-------|-------|
| **Interface** | Code (Python/Scala) | SQL only |
| **Use Case** | ETL pipelines | Ad-hoc queries |
| **Latency** | Seconds-minutes | Sub-second |
| **Audience** | Data engineers | Analysts |
| **Scalability** | Batch jobs | Interactive queries |

**When to Use Federation**:
- ✅ Ad-hoc queries across operational and analytical systems
- ✅ Real-time dashboards (Superset → Trino)
- ✅ Customer 360 views (operational DB + lakehouse)
- ✅ Sales analytics (live orders + historical trends)
- ✅ Exploratory analysis (no predefined ETL)

**When to Use ETL Instead**:
- ❌ Repeated queries (materialized view better)
- ❌ Complex transformations (deduplicate first)
- ❌ High query volume (pre-join to lakehouse)
- ❌ Operational DB performance concerns (offload to lakehouse)

**Production Setup**:
```yaml
# Trino catalogs (values.yaml)
catalogs:
  postgresql:
    connector.name: postgresql
    connection-url: jdbc:postgresql://infrastructure-postgresql:5432/openlakes
  
  iceberg:
    connector.name: iceberg
    iceberg.catalog.type: hadoop
    hive.metastore.uri: thrift://infrastructure-hive-metastore:9083
```


```python
if enable_cleanup:
    spark.sql(f"DROP TABLE IF EXISTS {full_iceberg_table}")
    cursor.execute(f"DROP TABLE IF EXISTS {postgres_table}")
    conn.commit()
    cursor.close()
    conn.close()
    print("🧹 Cleanup completed")
else:
    cursor.close()
    conn.close()
    print("⏭️  Cleanup skipped")
```
