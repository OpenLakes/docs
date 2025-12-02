```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType, LongType
from pyspark.sql.functions import col, sum as spark_sum, count, avg, when, expr, lit, window, current_timestamp, from_json
import random
from datetime import datetime, date, timedelta
import psycopg2

```

# Pattern 4.3: Cross-Database Federation

Use Trino to join data across multiple databases and catalogs:

1. **PostgreSQL**: Customer master data (operational)
2. **Iceberg Table 1**: Transaction history (analytical)
3. **Iceberg Table 2**: Product catalog (analytical)
4. **Federated Join**: Single query combining all three sources

## Architecture

```
┌─────────────────────────┐
│ PostgreSQL              │
│ customers (master data) │
└────────────┬────────────┘
             │
    ┌────────▼────────┐
    │ Trino Federated │
    │   Query Engine  │
    └────────┬────────┘
             │
   ┌─────────┴──────────┐
   │                    │
┌──▼──────────────┐  ┌──▼────────────┐
│ Iceberg         │  │ Iceberg       │
│ transactions    │  │ products      │
└─────────────────┘  └───────────────┘
```

**Use Case**: Customer 360 view combining:
- Master customer data (PostgreSQL): Name, email, region
- Transaction history (Iceberg): Purchase patterns
- Product catalog (Iceberg): Product details

**Benefits**:
- No ETL for ad-hoc cross-database joins
- Unified SQL query across systems
- Real-time operational data
- Historical analytical data
- Accessible to SQL analysts

## Parameters


```python
import os

from pyspark.sql import SparkSession
# Set AWS environment variables for S3FileIO
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "admin123"

spark = SparkSession.builder \
    .appName(f"Pattern-4.3-CrossDatabaseFederation-{environment}") \
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

print("✅ Spark initialized for cross-database federation")
```


```python
import os

from pyspark.sql import SparkSession
# Set AWS environment variables for S3FileIO
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "admin123"

spark = SparkSession.builder \
    .appName(f"Pattern-4.3-CrossDatabaseFederation-{environment}") \
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

print("✅ Spark initialized for cross-database federation")
```

## Setup: PostgreSQL Customer Master


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

# Create customers table
cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {postgres_customers_table} (
        customer_id VARCHAR(50) PRIMARY KEY,
        customer_name VARCHAR(100),
        email VARCHAR(100),
        region VARCHAR(50),
        signup_date DATE
    )
""")

cursor.execute(f"TRUNCATE TABLE {postgres_customers_table}")

# Insert customers
regions = ["North America", "Europe", "Asia", "South America", "Australia"]
for i in range(num_customers):
    customer_id = f"cust_{i+1:05d}"
    customer_name = f"Customer {i+1}"
    email = f"customer{i+1}@example.com"
    region = random.choice(regions)
    signup_date = (datetime.now() - timedelta(days=random.randint(30, 730))).date()
    
    cursor.execute(f"""
        INSERT INTO {postgres_customers_table} (customer_id, customer_name, email, region, signup_date)
        VALUES (%s, %s, %s, %s, %s)
    """, (customer_id, customer_name, email, region, signup_date))

conn.commit()
print(f"✅ PostgreSQL: Created {num_customers} customers")
```

## Setup: Iceberg Product Catalog


```python
full_database = f"lakehouse.{database_name}"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_database}")

full_products_table = f"{full_database}.{iceberg_products_table}"

# Create products table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {full_products_table} (
        product_id STRING,
        product_name STRING,
        category STRING,
        unit_price DOUBLE
    ) USING iceberg
""")

# Generate products
categories = ["Electronics", "Clothing", "Books", "Home", "Sports"]
products_data = []

for i in range(num_products):
    product_id = f"prod_{i+1:05d}"
    category = random.choice(categories)
    product_name = f"{category} Item {i+1}"
    unit_price = round(random.uniform(10.0, 500.0), 2)
    
    products_data.append((product_id, product_name, category, unit_price))

products_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("unit_price", DoubleType(), False)
])

df_products = spark.createDataFrame(products_data, schema=products_schema)
df_products.writeTo(full_products_table).using("iceberg").overwritePartitions()

print(f"✅ Iceberg: Created {num_products} products")
```

## Setup: Iceberg Transaction History


```python
full_transactions_table = f"{full_database}.{iceberg_transactions_table}"

# Create transactions table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {full_transactions_table} (
        transaction_id STRING,
        customer_id STRING,
        product_id STRING,
        quantity BIGINT,
        transaction_date DATE,
        total_amount DOUBLE
    ) USING iceberg
    PARTITIONED BY (days(transaction_date))
""")

# Generate transactions
transactions_data = []

for i in range(num_transactions):
    transaction_id = f"txn_{i+1:06d}"
    customer_id = f"cust_{random.randint(1, num_customers):05d}"
    product_id = f"prod_{random.randint(1, num_products):05d}"
    quantity = random.randint(1, 5)
    transaction_date = (datetime.now() - timedelta(days=random.randint(0, 90))).date()
    # Will be updated after joining with products
    total_amount = round(random.uniform(50.0, 1000.0), 2)
    
    transactions_data.append((
        transaction_id, customer_id, product_id, quantity,
        transaction_date, total_amount
    ))

transactions_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity", LongType(), False),
    StructField("transaction_date", DateType(), False),
    StructField("total_amount", DoubleType(), False)
])

df_transactions = spark.createDataFrame(transactions_data, schema=transactions_schema)
df_transactions.writeTo(full_transactions_table).using("iceberg").overwritePartitions()

print(f"✅ Iceberg: Created {num_transactions} transactions")
```

## Cross-Database Federation: 3-Way Join


```python
# Read from PostgreSQL
df_customers = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_database}") \
    .option("dbtable", postgres_customers_table) \
    .option("user", postgres_user) \
    .option("password", postgres_password) \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Read from Iceberg
df_transactions_iceberg = spark.table(full_transactions_table)
df_products_iceberg = spark.table(full_products_table)

# Three-way federated join
df_customer_360 = df_transactions_iceberg \
    .join(df_customers, on="customer_id", how="left") \
    .join(df_products_iceberg, on="product_id", how="left") \
    .select(
        col("transaction_id"),
        col("customer_name"),
        col("email"),
        col("region"),
        col("product_name"),
        col("category"),
        col("quantity"),
        col("transaction_date"),
        (col("quantity") * col("unit_price")).alias("calculated_amount")
    )

print("📊 Customer 360 View (PostgreSQL + Iceberg Transactions + Iceberg Products):")
df_customer_360.orderBy(desc("transaction_date")).show(20, truncate=False)
```

## Business Analytics: Regional Performance


```python
# Regional sales analysis
df_regional = df_customer_360.groupBy("region").agg(
    count("*").alias("total_transactions"),
    _sum("calculated_amount").alias("total_revenue"),
    avg("calculated_amount").alias("avg_transaction_value"),
    count("customer_name").alias("unique_customers")
).orderBy(desc("total_revenue"))

print("📊 Regional Performance (Cross-Database Analytics):")
df_regional.show(truncate=False)

# Category popularity
df_category = df_customer_360.groupBy("category").agg(
    count("*").alias("total_transactions"),
    _sum("quantity").alias("total_units_sold"),
    _sum("calculated_amount").alias("total_revenue")
).orderBy(desc("total_revenue"))

print("\n📊 Category Performance:")
df_category.show(truncate=False)
```

## Trino SQL Pattern (Production)

In production, use Trino for SQL-based cross-database federation:


```python
print("""\n💡 Trino Cross-Database Federation Pattern:

```sql
-- Customer 360 query spanning 3 data sources
SELECT 
    t.transaction_id,
    c.customer_name,
    c.email,
    c.region,
    p.product_name,
    p.category,
    t.quantity,
    t.transaction_date,
    t.quantity * p.unit_price AS calculated_amount
FROM iceberg.demo.transactions t
LEFT JOIN postgresql.public.customers c
    ON t.customer_id = c.customer_id
LEFT JOIN iceberg.demo.products p
    ON t.product_id = p.product_id
ORDER BY t.transaction_date DESC
LIMIT 100;

-- Regional performance analytics
SELECT 
    c.region,
    COUNT(*) as total_transactions,
    SUM(t.quantity * p.unit_price) as total_revenue,
    AVG(t.quantity * p.unit_price) as avg_transaction,
    COUNT(DISTINCT c.customer_id) as unique_customers
FROM iceberg.demo.transactions t
JOIN postgresql.public.customers c ON t.customer_id = c.customer_id
JOIN iceberg.demo.products p ON t.product_id = p.product_id
WHERE t.transaction_date >= CURRENT_DATE - INTERVAL '30' DAY
GROUP BY c.region
ORDER BY total_revenue DESC;
```

**Catalogs**:
- `postgresql`: Operational database (customer master)
- `iceberg`: Lakehouse (transactions + products)

**Benefits**:
- Single SQL query across 3 sources
- No ETL required for ad-hoc joins
- Real-time operational data (PostgreSQL)
- Historical analytical data (Iceberg)
- Accessible to SQL analysts via Superset/BI tools
""")
```

## Validation


```python
if enable_validation:
    customer_360_count = df_customer_360.count()
    transactions_count = df_transactions_iceberg.count()
    
    assert customer_360_count == transactions_count, "Customer 360 should match transaction count (left join)"
    print(f"✅ Customer 360 view: {customer_360_count} rows")
    
    # Verify all three sources joined
    enriched_count = df_customer_360.filter(
        col("customer_name").isNotNull() & col("product_name").isNotNull()
    ).count()
    assert enriched_count > 0, "Should have enriched rows from all sources"
    print(f"✅ Enriched rows (all 3 sources): {enriched_count}")
    
    # Verify regional analytics
    regional_count = df_regional.count()
    assert regional_count > 0, "Should have regional analytics"
    print(f"✅ Regional breakdown: {regional_count} regions")
    
    test_passed = True
    print("\n✅ All validations passed!")
else:
    test_passed = True
```

## Summary

### ✅ Pattern 4.3: Cross-Database Federation Complete!

Demonstrated:
1. **PostgreSQL**: Customer master data (operational)
2. **Iceberg Table 1**: Transaction history (analytical)
3. **Iceberg Table 2**: Product catalog (analytical)
4. **3-Way Join**: Single query combining all sources
5. **Business Analytics**: Regional and category performance

**Cross-Database Federation Benefits**:
- 🔗 Single SQL query across multiple sources
- 🚀 No ETL for ad-hoc cross-database joins
- ⚡ Real-time operational data (PostgreSQL)
- 📊 Historical analytical data (Iceberg)
- 👥 Accessible to SQL analysts (Trino + Superset)
- 🎯 Engine specialization (operational vs analytical)

**When to Use Federation**:
- ✅ Ad-hoc customer 360 views
- ✅ Cross-system analytics
- ✅ Exploratory data analysis
- ✅ One-time reports
- ✅ Real-time dashboards (Superset → Trino)

**When to Use ETL Instead**:
- ❌ Repeated queries (materialize to lakehouse)
- ❌ Complex transformations (pre-process)
- ❌ High query volume (pre-join)
- ❌ Operational DB performance impact (offload)

**Architecture Comparison**:

| Approach | Federated Query | ETL Pipeline |
|----------|----------------|---------------|
| **Latency** | Real-time | Hours/days |
| **Flexibility** | Ad-hoc queries | Predefined schema |
| **Performance** | Query-time join | Pre-joined |
| **Use Case** | Exploratory | Production reports |
| **Audience** | Analysts | Everyone |

**Production Setup**:
```yaml
# Trino catalogs (values.yaml)
additionalCatalogs:
  postgresql:
    connector.name: postgresql
    connection-url: jdbc:postgresql://infrastructure-postgresql:5432/openlakes
    connection-user: admin
    connection-password: admin123
  
  iceberg:
    connector.name: iceberg
    iceberg.catalog.type: hadoop
    hive.metastore.uri: thrift://infrastructure-hive-metastore:9083
```

**Superset Integration**:
```python
# Connect Superset to Trino
SQLAlchemy URI: trino://admin@analytics-trino:8080/iceberg/demo

# Create dataset from federated query
SELECT 
    c.region,
    p.category,
    SUM(t.quantity * p.unit_price) as revenue
FROM iceberg.demo.transactions t
JOIN postgresql.public.customers c ON t.customer_id = c.customer_id
JOIN iceberg.demo.products p ON t.product_id = p.product_id
GROUP BY c.region, p.category
```


```python
if enable_cleanup:
    spark.sql(f"DROP TABLE IF EXISTS {full_transactions_table}")
    spark.sql(f"DROP TABLE IF EXISTS {full_products_table}")
    cursor.execute(f"DROP TABLE IF EXISTS {postgres_customers_table}")
    conn.commit()
    cursor.close()
    conn.close()
    print("🧹 Cleanup completed")
else:
    cursor.close()
    conn.close()
    print("⏭️  Cleanup skipped")
```
