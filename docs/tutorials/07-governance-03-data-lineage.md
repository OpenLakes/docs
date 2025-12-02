# Pattern 7.3: Data Lineage Tracking

Comprehensive data lineage tracking using OpenLineage and OpenMetadata:

1. **Table-Level Lineage**: Track data flow between tables
2. **Column-Level Lineage**: Which source columns create target columns
3. **Transformation Lineage**: Capture SQL transformation logic
4. **Impact Analysis**: Identify downstream dependencies
5. **Data Quality Lineage**: Link quality issues to source data
6. **Audit Trail**: Track who, what, when, where, why

## Architecture

```
Source Tables (customers, orders, products)
    ↓
Transformation 1: Join customers + orders
    ↓ (OpenLineage events emitted)
Staging Table (customer_orders)
    ↓
Transformation 2: Aggregate + enrich with products
    ↓ (OpenLineage events emitted)
Target Table (customer_revenue_summary)
    ↓
OpenMetadata (Lineage visualization + API)
```

## Components

- **OpenLineage**: Standard for lineage events (SERDE for lineage metadata)
- **OpenMetadata**: Lineage storage, visualization, and API
- **Spark Integration**: Automatic lineage capture via OpenLineage Spark agent
- **Iceberg Integration**: Table-level lineage via table properties

## Use Cases

- **Debugging**: Trace bad data to its source
- **Impact Analysis**: What breaks if I change this table?
- **Compliance**: Audit data transformations for regulations
- **Data Discovery**: Find where data comes from
- **Root Cause Analysis**: Track data quality issues upstream

## Parameters


```python
# Execution parameters
execution_date = "2025-01-17"
environment = "development"
database_name = "governance_demo"
customers_table = "customers_source"
orders_table = "orders_source"
products_table = "products_source"
staging_table = "customer_orders_staging"
target_table = "customer_revenue_summary"
lineage_events_table = "lineage_events"
num_customers = 100
num_orders = 500
num_products = 50
enable_validation = True
enable_cleanup = False
```

## Step 1: Initialize Spark with Iceberg + Nessie


```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType, ArrayType
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_json, struct, array, when,
    sum as spark_sum, count, avg, max as spark_max, min as spark_min,
    round as spark_round, concat, concat_ws, explode
)
import random
from datetime import datetime, timedelta
import json
import os

# Set AWS environment variables for S3FileIO
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "admin123"

spark = SparkSession.builder \
    .appName(f"Pattern-7.3-DataLineage-{environment}") \
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
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileIO") \
    .getOrCreate()

print(f"✅ Spark session created")
print(f"   App Name: {spark.sparkContext.appName}")
print(f"   Catalog: Nessie (Git-like versioning)")
print(f"   Warehouse: s3a://openlakes/warehouse/")
print(f"   Environment: {environment}")
print(f"\n💡 Note: OpenLineage integration requires openlineage-spark JAR")
print(f"   For full lineage capture, add to spark.jars.packages:")
print(f"   io.openlineage:openlineage-spark_2.13:1.2.0")
```

## Step 2: Create Source Tables with Lineage Metadata

Create realistic source data for a retail scenario.


```python
# Create database
full_database = f"lakehouse.{database_name}"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_database}")
print(f"✅ Database created: {full_database}")

# Generate customers data
def generate_customers(num_customers):
    first_names = ["John", "Jane", "Robert", "Maria", "Michael", "Sarah", "David", "Emma", "James", "Lisa"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"]
    states = ["CA", "NY", "TX", "FL", "IL", "WA", "CO", "MA"]
    
    customers = []
    for i in range(1, num_customers + 1):
        customers.append((
            i,
            random.choice(first_names),
            random.choice(last_names),
            f"customer{i}@example.com",
            random.choice(states),
            (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1460))).date()
        ))
    
    return customers

# Generate products data
def generate_products(num_products):
    categories = ["Electronics", "Clothing", "Home", "Sports", "Books"]
    products = []
    
    for i in range(1, num_products + 1):
        category = random.choice(categories)
        products.append((
            i,
            f"Product {i}",
            category,
            round(random.uniform(10.0, 500.0), 2),
            round(random.uniform(5.0, 250.0), 2)  # cost
        ))
    
    return products

# Generate orders data
def generate_orders(num_orders, num_customers, num_products):
    orders = []
    base_date = datetime(2024, 1, 1)
    
    for i in range(1, num_orders + 1):
        customer_id = random.randint(1, num_customers)
        product_id = random.randint(1, num_products)
        quantity = random.randint(1, 10)
        order_date = (base_date + timedelta(days=random.randint(0, 365))).date()
        
        orders.append((
            i,
            customer_id,
            product_id,
            quantity,
            order_date
        ))
    
    return orders

# Create customers table
customers_data = generate_customers(num_customers)
customers_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("state", StringType(), False),
    StructField("signup_date", DateType(), False)
])
df_customers = spark.createDataFrame(customers_data, customers_schema)

full_customers_table = f"{full_database}.{customers_table}"
df_customers.writeTo(full_customers_table).using("iceberg").createOrReplace()

# Add table metadata for lineage
spark.sql(f"""
    ALTER TABLE {full_customers_table} 
    SET TBLPROPERTIES (
        'description' = 'Customer master data from CRM system',
        'source_system' = 'Salesforce CRM',
        'owner' = 'data-engineering-team',
        'pii_data' = 'true',
        'created_by' = 'Pattern-7.3-DataLineage'
    )
""")

print(f"✅ Created {full_customers_table} ({df_customers.count()} records)")

# Create products table
products_data = generate_products(num_products)
products_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("cost", DoubleType(), False)
])
df_products = spark.createDataFrame(products_data, products_schema)

full_products_table = f"{full_database}.{products_table}"
df_products.writeTo(full_products_table).using("iceberg").createOrReplace()

spark.sql(f"""
    ALTER TABLE {full_products_table}
    SET TBLPROPERTIES (
        'description' = 'Product catalog from inventory system',
        'source_system' = 'Oracle ERP',
        'owner' = 'product-team',
        'created_by' = 'Pattern-7.3-DataLineage'
    )
""")

print(f"✅ Created {full_products_table} ({df_products.count()} records)")

# Create orders table
orders_data = generate_orders(num_orders, num_customers, num_products)
orders_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("order_date", DateType(), False)
])
df_orders = spark.createDataFrame(orders_data, orders_schema)

full_orders_table = f"{full_database}.{orders_table}"
df_orders.writeTo(full_orders_table).using("iceberg").createOrReplace()

spark.sql(f"""
    ALTER TABLE {full_orders_table}
    SET TBLPROPERTIES (
        'description' = 'Transaction data from e-commerce platform',
        'source_system' = 'Shopify',
        'owner' = 'sales-team',
        'created_by' = 'Pattern-7.3-DataLineage'
    )
""")

print(f"✅ Created {full_orders_table} ({df_orders.count()} records)")
print(f"\n📊 Source tables ready for lineage tracking")
```

## Step 3: Transformation 1 - Join Customers + Orders (Table-Level Lineage)

Track lineage from customers + orders → customer_orders_staging


```python
# Capture lineage metadata before transformation
lineage_event_1 = {
    "eventType": "START",
    "eventTime": datetime.now().isoformat(),
    "job": {
        "namespace": "openlakes",
        "name": "customer_orders_etl",
        "description": "Join customers and orders for staging"
    },
    "inputs": [
        {"namespace": full_database, "name": customers_table},
        {"namespace": full_database, "name": orders_table}
    ],
    "outputs": [
        {"namespace": full_database, "name": staging_table}
    ],
    "run": {
        "runId": f"run_{execution_date}_001"
    }
}

print("📊 Starting Transformation 1: customers + orders → customer_orders_staging")
print(f"\n📋 Lineage Event:")
print(json.dumps(lineage_event_1, indent=2))

# Execute transformation with column-level lineage tracking
df_customer_orders = spark.sql(f"""
    SELECT 
        o.order_id,
        o.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.state,
        o.product_id,
        o.quantity,
        o.order_date,
        CURRENT_TIMESTAMP() as processing_timestamp
    FROM {full_orders_table} o
    INNER JOIN {full_customers_table} c
        ON o.customer_id = c.customer_id
""")

full_staging_table = f"{full_database}.{staging_table}"
df_customer_orders.writeTo(full_staging_table).using("iceberg").createOrReplace()

# Add lineage metadata to table properties
spark.sql(f"""
    ALTER TABLE {full_staging_table}
    SET TBLPROPERTIES (
        'description' = 'Staging table: customer orders joined data',
        'upstream_tables' = '{full_customers_table}, {full_orders_table}',
        'transformation' = 'INNER JOIN on customer_id',
        'owner' = 'data-engineering-team',
        'created_by' = 'Pattern-7.3-DataLineage',
        'lineage_job' = 'customer_orders_etl',
        'lineage_run_id' = 'run_{execution_date}_001'
    )
""")

staging_count = df_customer_orders.count()
print(f"\n✅ Transformation 1 completed: {staging_count} records")
print(f"   Source: {full_customers_table} + {full_orders_table}")
print(f"   Target: {full_staging_table}")
print(f"\n📊 Sample staging data:")
df_customer_orders.show(5, truncate=False)

# Capture completion lineage event
lineage_event_1["eventType"] = "COMPLETE"
lineage_event_1["eventTime"] = datetime.now().isoformat()
lineage_event_1["outputs"][0]["outputStatistics"] = {
    "rowCount": staging_count,
    "size": df_customer_orders.rdd.map(lambda x: len(str(x))).reduce(lambda a, b: a + b)
}

print(f"\n✅ Lineage event captured (COMPLETE)")
```

## Step 4: Transformation 2 - Aggregate + Enrich (Column-Level Lineage)

Track column-level lineage:
- customer_id → customer_id
- first_name + last_name → customer_name
- quantity * price → total_revenue
- quantity * cost → total_cost


```python
# Capture lineage metadata
lineage_event_2 = {
    "eventType": "START",
    "eventTime": datetime.now().isoformat(),
    "job": {
        "namespace": "openlakes",
        "name": "customer_revenue_aggregation",
        "description": "Aggregate customer revenue with product data"
    },
    "inputs": [
        {"namespace": full_database, "name": staging_table},
        {"namespace": full_database, "name": products_table}
    ],
    "outputs": [
        {"namespace": full_database, "name": target_table}
    ],
    "run": {
        "runId": f"run_{execution_date}_002"
    },
    "columnLineage": [
        {
            "targetColumn": "customer_id",
            "sourceColumns": [{"table": staging_table, "column": "customer_id"}],
            "transformation": "DIRECT"
        },
        {
            "targetColumn": "customer_name",
            "sourceColumns": [
                {"table": staging_table, "column": "first_name"},
                {"table": staging_table, "column": "last_name"}
            ],
            "transformation": "CONCAT(first_name, ' ', last_name)"
        },
        {
            "targetColumn": "total_revenue",
            "sourceColumns": [
                {"table": staging_table, "column": "quantity"},
                {"table": products_table, "column": "price"}
            ],
            "transformation": "SUM(quantity * price)"
        },
        {
            "targetColumn": "total_cost",
            "sourceColumns": [
                {"table": staging_table, "column": "quantity"},
                {"table": products_table, "column": "cost"}
            ],
            "transformation": "SUM(quantity * cost)"
        },
        {
            "targetColumn": "profit_margin",
            "sourceColumns": [
                {"table": staging_table, "column": "quantity"},
                {"table": products_table, "column": "price"},
                {"table": products_table, "column": "cost"}
            ],
            "transformation": "(total_revenue - total_cost) / total_revenue"
        }
    ]
}

print("📊 Starting Transformation 2: staging + products → customer_revenue_summary")
print(f"\n📋 Column-Level Lineage:")
for col_lineage in lineage_event_2["columnLineage"]:
    print(f"   {col_lineage['targetColumn']} ← {col_lineage['transformation']}")

# Execute transformation
df_revenue_summary = spark.sql(f"""
    SELECT 
        co.customer_id,
        CONCAT(co.first_name, ' ', co.last_name) as customer_name,
        co.email,
        co.state,
        COUNT(DISTINCT co.order_id) as total_orders,
        SUM(co.quantity) as total_items,
        ROUND(SUM(co.quantity * p.price), 2) as total_revenue,
        ROUND(SUM(co.quantity * p.cost), 2) as total_cost,
        ROUND(SUM(co.quantity * p.price) - SUM(co.quantity * p.cost), 2) as total_profit,
        ROUND(
            (SUM(co.quantity * p.price) - SUM(co.quantity * p.cost)) / 
            NULLIF(SUM(co.quantity * p.price), 0) * 100,
            2
        ) as profit_margin_pct,
        MAX(co.order_date) as last_order_date,
        CURRENT_TIMESTAMP() as generated_at
    FROM {full_staging_table} co
    INNER JOIN {full_products_table} p
        ON co.product_id = p.product_id
    GROUP BY co.customer_id, co.first_name, co.last_name, co.email, co.state
""")

full_target_table = f"{full_database}.{target_table}"
df_revenue_summary.writeTo(full_target_table).using("iceberg").createOrReplace()

# Add comprehensive lineage metadata
spark.sql(f"""
    ALTER TABLE {full_target_table}
    SET TBLPROPERTIES (
        'description' = 'Customer revenue summary with profit metrics',
        'upstream_tables' = '{full_staging_table}, {full_products_table}',
        'upstream_original' = '{full_customers_table}, {full_orders_table}, {full_products_table}',
        'transformation' = 'INNER JOIN + GROUP BY + Aggregations',
        'business_logic' = 'Calculate customer lifetime value and profitability',
        'owner' = 'analytics-team',
        'created_by' = 'Pattern-7.3-DataLineage',
        'lineage_job' = 'customer_revenue_aggregation',
        'lineage_run_id' = 'run_{execution_date}_002',
        'quality_checks' = 'total_revenue > 0, profit_margin < 100',
        'refresh_frequency' = 'daily'
    )
""")

target_count = df_revenue_summary.count()
print(f"\n✅ Transformation 2 completed: {target_count} customer summaries")
print(f"   Source: {full_staging_table} + {full_products_table}")
print(f"   Target: {full_target_table}")
print(f"\n📊 Customer Revenue Summary:")
df_revenue_summary.orderBy(col("total_revenue").desc()).show(10, truncate=False)

# Capture completion event
lineage_event_2["eventType"] = "COMPLETE"
lineage_event_2["eventTime"] = datetime.now().isoformat()
lineage_event_2["outputs"][0]["outputStatistics"] = {
    "rowCount": target_count,
    "totalRevenue": df_revenue_summary.agg(spark_sum("total_revenue")).collect()[0][0],
    "totalProfit": df_revenue_summary.agg(spark_sum("total_profit")).collect()[0][0],
    "avgProfitMargin": df_revenue_summary.agg(avg("profit_margin_pct")).collect()[0][0]
}

print(f"\n✅ Lineage event captured (COMPLETE)")
```

## Step 5: Store Lineage Events for Audit Trail

Store all lineage events in Iceberg table for querying.


```python
# Combine all lineage events
lineage_events = [lineage_event_1, lineage_event_2]

# Create lineage events DataFrame
lineage_records = []
for event in lineage_events:
    lineage_records.append((
        event["run"]["runId"],
        event["job"]["name"],
        event["eventType"],
        event["eventTime"],
        json.dumps(event["inputs"]),
        json.dumps(event["outputs"]),
        json.dumps(event.get("columnLineage", [])),
        event["job"].get("description", ""),
        execution_date
    ))

lineage_schema = StructType([
    StructField("run_id", StringType(), False),
    StructField("job_name", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("event_time", StringType(), False),
    StructField("input_tables", StringType(), False),
    StructField("output_tables", StringType(), False),
    StructField("column_lineage", StringType(), False),
    StructField("description", StringType(), False),
    StructField("execution_date", StringType(), False)
])

df_lineage = spark.createDataFrame(lineage_records, lineage_schema)

full_lineage_table = f"{full_database}.{lineage_events_table}"
df_lineage.writeTo(full_lineage_table).using("iceberg").createOrReplace()

print(f"✅ Lineage events stored in {full_lineage_table}")
print(f"\n📊 Lineage Events Audit Trail:")
df_lineage.show(truncate=False)

spark.sql(f"""
    ALTER TABLE {full_lineage_table}
    SET TBLPROPERTIES (
        'description' = 'Audit trail of all data lineage events',
        'owner' = 'governance-team',
        'retention_days' = '2555'  -- 7 years for compliance
    )
""")

print(f"\n💡 Lineage events can be queried for:")
print(f"   - Debugging: Trace data flow for specific run_id")
print(f"   - Audit: Compliance reporting (who, what, when)")
print(f"   - Impact Analysis: Find downstream dependencies")
```

## Step 6: Impact Analysis - Find Downstream Dependencies

Given a source table, find all downstream tables that depend on it.


```python
def find_downstream_tables(source_table_name):
    """
    Find all tables that depend on the given source table.
    
    This is critical for impact analysis:
    - If I change customers table, what breaks?
    - If data quality issue in orders, what's affected?
    """
    
    # Query lineage events
    downstream = spark.sql(f"""
        SELECT DISTINCT
            job_name,
            output_tables,
            description,
            event_time
        FROM {full_lineage_table}
        WHERE input_tables LIKE '%{source_table_name}%'
        ORDER BY event_time DESC
    """)
    
    return downstream

# Example: Impact analysis for customers table
print(f"📊 Impact Analysis: What depends on '{customers_table}'?\n")
downstream_customers = find_downstream_tables(customers_table)
downstream_customers.show(truncate=False)

# Example: Impact analysis for orders table
print(f"\n📊 Impact Analysis: What depends on '{orders_table}'?\n")
downstream_orders = find_downstream_tables(orders_table)
downstream_orders.show(truncate=False)

# Build complete lineage graph
print(f"\n📊 Complete Data Lineage Graph:\n")
print(f"Source Tables (External Systems):")
print(f"  ├─ {customers_table} (Salesforce CRM)")
print(f"  ├─ {orders_table} (Shopify)")
print(f"  └─ {products_table} (Oracle ERP)")
print(f"       ↓")
print(f"Transformation 1 (customer_orders_etl):")
print(f"  └─ {staging_table}")
print(f"       ↓")
print(f"Transformation 2 (customer_revenue_aggregation):")
print(f"  └─ {target_table}")
print(f"       ↓")
print(f"Downstream Consumers:")
print(f"  ├─ Superset Dashboard (Customer Analytics)")
print(f"  ├─ ML Model (Customer Lifetime Value Prediction)")
print(f"  └─ Data Export (Weekly CEO Report)")

print(f"\n💡 Impact Analysis Use Cases:")
print(f"   1. Schema Change: If customers.email changes type, what breaks?")
print(f"   2. Data Quality: If orders has duplicates, which reports affected?")
print(f"   3. Deprecation: Can we safely drop products.cost column?")
print(f"   4. Compliance: Show auditors how PII flows through system")
```

## Step 7: Root Cause Analysis - Trace Data Quality Issues

Given a data quality issue in target table, trace back to source.


```python
# Simulate data quality issue: Find customers with negative profit
df_quality_issue = spark.sql(f"""
    SELECT 
        customer_id,
        customer_name,
        total_revenue,
        total_cost,
        total_profit,
        profit_margin_pct
    FROM {full_target_table}
    WHERE total_profit < 0
    ORDER BY total_profit ASC
    LIMIT 5
""")

issue_count = df_quality_issue.count()

if issue_count > 0:
    print(f"⚠️  Data Quality Issue Detected: {issue_count} customers with negative profit\n")
    df_quality_issue.show(truncate=False)
    
    # Trace back to source data
    problem_customer_id = df_quality_issue.first()["customer_id"]
    
    print(f"\n🔍 Root Cause Analysis for customer_id = {problem_customer_id}:\n")
    
    # Step 1: Check staging table
    print(f"Step 1: Check staging table ({staging_table})")
    df_staging_trace = spark.sql(f"""
        SELECT * FROM {full_staging_table}
        WHERE customer_id = {problem_customer_id}
    """)
    df_staging_trace.show(truncate=False)
    
    # Step 2: Check source orders
    print(f"\nStep 2: Check source orders ({orders_table})")
    df_orders_trace = spark.sql(f"""
        SELECT * FROM {full_orders_table}
        WHERE customer_id = {problem_customer_id}
    """)
    df_orders_trace.show(truncate=False)
    
    # Step 3: Check product pricing
    product_ids = [row["product_id"] for row in df_orders_trace.select("product_id").distinct().collect()]
    print(f"\nStep 3: Check product pricing ({products_table})")
    df_products_trace = spark.sql(f"""
        SELECT * FROM {full_products_table}
        WHERE product_id IN ({','.join(map(str, product_ids))})
    """)
    df_products_trace.show(truncate=False)
    
    print(f"\n🎯 Root Cause: Check if:")
    print(f"   1. Product cost > price (pricing error in {products_table})")
    print(f"   2. Quantity is negative (data error in {orders_table})")
    print(f"   3. Join produced duplicates (logic error in {staging_table})")
    
    print(f"\n📋 Lineage Path:")
    print(f"   {products_table} (Oracle ERP) → cost/price data")
    print(f"   {orders_table} (Shopify) → quantity data")
    print(f"   {staging_table} (JOIN) → combined data")
    print(f"   {target_table} (AGGREGATE) → profit calculation")
    print(f"   ↓")
    print(f"   Issue detected: negative profit")
else:
    print(f"✅ No data quality issues detected (all customers profitable)")
```

## Step 8: Validation & Testing


```python
if enable_validation:
    print("🧪 Running validation tests...\n")
    
    # Test 1: All source tables should have lineage metadata
    for table in [full_customers_table, full_orders_table, full_products_table]:
        props = spark.sql(f"SHOW TBLPROPERTIES {table}").collect()
        prop_dict = {row["key"]: row["value"] for row in props}
        assert "source_system" in prop_dict, f"{table} missing source_system metadata"
        assert "owner" in prop_dict, f"{table} missing owner metadata"
    print("✅ Test 1 PASSED: All source tables have lineage metadata")
    
    # Test 2: Staging table should reference upstream tables
    staging_props = spark.sql(f"SHOW TBLPROPERTIES {full_staging_table}").collect()
    staging_dict = {row["key"]: row["value"] for row in staging_props}
    assert "upstream_tables" in staging_dict, "Staging table missing upstream_tables"
    assert customers_table in staging_dict["upstream_tables"], "Missing customers reference"
    assert orders_table in staging_dict["upstream_tables"], "Missing orders reference"
    print("✅ Test 2 PASSED: Staging table references upstream tables")
    
    # Test 3: Target table should reference all original sources
    target_props = spark.sql(f"SHOW TBLPROPERTIES {full_target_table}").collect()
    target_dict = {row["key"]: row["value"] for row in target_props}
    assert "upstream_original" in target_dict, "Target table missing upstream_original"
    for table in [customers_table, orders_table, products_table]:
        assert table in target_dict["upstream_original"], f"Missing {table} in lineage"
    print("✅ Test 3 PASSED: Target table tracks all original sources")
    
    # Test 4: Lineage events table should have all jobs
    lineage_count = spark.table(full_lineage_table).count()
    assert lineage_count >= 2, f"Should have at least 2 lineage events, got {lineage_count}"
    print(f"✅ Test 4 PASSED: Lineage events table has {lineage_count} events")
    
    # Test 5: Impact analysis should find dependencies
    downstream = find_downstream_tables(customers_table)
    assert downstream.count() > 0, "Should find downstream dependencies for customers"
    print(f"✅ Test 5 PASSED: Impact analysis found {downstream.count()} downstream dependencies")
    
    # Test 6: Data integrity check (no orphaned records)
    # All customer_ids in target should exist in staging
    orphaned = spark.sql(f"""
        SELECT t.customer_id
        FROM {full_target_table} t
        LEFT JOIN {full_staging_table} s ON t.customer_id = s.customer_id
        WHERE s.customer_id IS NULL
    """).count()
    assert orphaned == 0, f"Found {orphaned} orphaned records in target table"
    print(f"✅ Test 6 PASSED: No orphaned records (data integrity maintained)")
    
    test_passed = True
    print("\n✅ All validation tests PASSED!")
else:
    test_passed = True
    print("⏭️  Validation skipped")
```

## Step 9: Production Integration Examples


```python
production_code = '''
# ============================================================================
# PRODUCTION DATA LINEAGE INTEGRATION
# ============================================================================

## 1. OPENLINEAGE SPARK INTEGRATION

# Add to spark-submit or spark-defaults.conf:
spark-submit \
  --packages io.openlineage:openlineage-spark_2.13:1.2.0 \
  --conf spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener \
  --conf spark.openlineage.transport.type=http \
  --conf spark.openlineage.transport.url=http://openmetadata:8585/api/v1/lineage \
  --conf spark.openlineage.namespace=openlakes \
  your_job.py

# Lineage events are automatically emitted to OpenMetadata!

## 2. OPENMETADATA API INTEGRATION

from metadata.generated.schema.entity.data.table import Table
from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.ingestion.ometa.ometa_api import OpenMetadata

# Initialize OpenMetadata client
metadata = OpenMetadata(
    config=OpenMetadataConnection(
        hostPort="http://openmetadata:8585/api"
    )
)

# Add lineage manually
lineage = AddLineageRequest(
    edge=EntitiesEdge(
        fromEntity=EntityReference(
            id=source_table_id,
            type="table"
        ),
        toEntity=EntityReference(
            id=target_table_id,
            type="table"
        )
    ),
    description="ETL pipeline: customer_orders_etl"
)

metadata.add_lineage(lineage)

## 3. QUERY LINEAGE FROM OPENMETADATA

# Get upstream lineage (sources)
upstream = metadata.get_lineage_by_id(
    entity_id=table_id,
    upstream_depth=3,  # 3 levels up
    downstream_depth=0
)

# Get downstream lineage (consumers)
downstream = metadata.get_lineage_by_id(
    entity_id=table_id,
    upstream_depth=0,
    downstream_depth=3  # 3 levels down
)

## 4. AIRFLOW + LINEAGE INTEGRATION

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.lineage import AUTO
from airflow.lineage.entities import File

def emit_lineage(**context):
    """Emit lineage metadata to OpenMetadata"""
    # Custom lineage emission logic
    pass

with DAG(
    "customer_revenue_pipeline",
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    task = PythonOperator(
        task_id="process_revenue",
        python_callable=emit_lineage,
        # Airflow automatically tracks lineage between tasks
        inlets=[File("s3://lakehouse/customers"), File("s3://lakehouse/orders")],
        outlets=[File("s3://lakehouse/customer_revenue_summary")],
    )

## 5. DBT + LINEAGE (FOR SQL-BASED TRANSFORMATIONS)

# dbt automatically generates lineage DAG from SQL models
# models/customer_revenue.sql

-- upstream dependencies documented in dbt_project.yml
SELECT 
    c.customer_id,
    c.name,
    SUM(o.amount) as total_revenue
FROM {{ ref('customers') }} c  -- dbt tracks this dependency
JOIN {{ ref('orders') }} o ON c.id = o.customer_id
GROUP BY c.customer_id, c.name

# dbt docs generate --lineage-graph
# dbt can push lineage to OpenMetadata via metadata plugin

## 6. IMPACT ANALYSIS QUERIES

# Find all tables affected by customers table change:
WITH RECURSIVE lineage_tree AS (
    -- Base case: direct children
    SELECT 
        output_tables,
        1 as depth
    FROM lineage_events
    WHERE input_tables LIKE '%customers%'
    
    UNION ALL
    
    -- Recursive case: grandchildren
    SELECT 
        le.output_tables,
        lt.depth + 1
    FROM lineage_events le
    JOIN lineage_tree lt 
        ON le.input_tables LIKE CONCAT('%', lt.output_tables, '%')
    WHERE lt.depth < 5  -- prevent infinite recursion
)
SELECT DISTINCT output_tables, MAX(depth) as max_depth
FROM lineage_tree
GROUP BY output_tables
ORDER BY max_depth, output_tables;

## 7. DATA QUALITY + LINEAGE INTEGRATION

# Link quality issues to lineage
CREATE TABLE data_quality_issues AS
SELECT 
    'customer_revenue_summary' as affected_table,
    'negative_profit' as issue_type,
    customer_id as affected_record,
    'products_source' as root_cause_table,
    'cost > price' as root_cause,
    CURRENT_TIMESTAMP as detected_at
FROM customer_revenue_summary
WHERE total_profit < 0;

# Alert downstream consumers:
# "Warning: Data quality issue in customer_revenue_summary"
# "Root cause: products_source table has pricing errors"
# "Affected downstream: CEO_dashboard, ML_model_training"

## 8. COMPLIANCE & AUDIT REPORTING

# GDPR Article 30: Record of processing activities
SELECT 
    job_name,
    description,
    input_tables,
    output_tables,
    event_time,
    CASE 
        WHEN input_tables LIKE '%customers%' THEN 'PII Processing'
        ELSE 'Non-PII'
    END as data_classification
FROM lineage_events
WHERE execution_date >= '2024-01-01'
ORDER BY event_time DESC;

# Generate audit report:
# "All transformations involving customer PII are tracked"
# "Lineage shows: CRM → staging → analytics → BI dashboard"
# "Data retention: 7 years per GDPR Article 5(e)"

# ============================================================================
'''

print("📋 Production Data Lineage Integration:")
print("="*80)
print(production_code)
print("="*80)
```

## Summary

### ✅ Pattern 7.3: Data Lineage Tracking Complete!

This notebook demonstrated:

1. **Table-Level Lineage**: Track data flow between tables
   - customers + orders → customer_orders_staging
   - staging + products → customer_revenue_summary

2. **Column-Level Lineage**: Track transformations
   - first_name + last_name → customer_name (CONCAT)
   - quantity * price → total_revenue (SUM)
   - quantity * cost → total_cost (SUM)
   - (revenue - cost) / revenue → profit_margin (CALCULATION)

3. **Lineage Metadata Storage**: Audit trail in Iceberg table
   - Job name, run ID, input/output tables
   - Column-level transformations
   - Timestamps for compliance

4. **Impact Analysis**: Find downstream dependencies
   - "What breaks if I change customers table?"
   - "Which reports use orders data?"

5. **Root Cause Analysis**: Trace data quality issues
   - Negative profit → bad product pricing
   - Lineage path: products → staging → summary

### Key Benefits:

- **Debugging**: Trace bad data to source system
- **Compliance**: Audit data transformations for GDPR, SOX, HIPAA
- **Data Discovery**: Understand data flow without tribal knowledge
- **Impact Analysis**: Safe schema changes and deprecations
- **Quality**: Link quality issues to root causes

### Production Integration:

- **OpenLineage**: Automatic lineage capture from Spark
- **OpenMetadata**: Lineage visualization and API
- **Airflow**: DAG-level lineage tracking
- **dbt**: SQL-based lineage for analytics models

### Next Steps:

- **Pattern 7.4**: Data Cataloging & Discovery
- **OpenMetadata Integration**: Deploy and visualize lineage
- **Automated Lineage**: Configure OpenLineage Spark agent


```python
# Cleanup (optional)
if enable_cleanup:
    for table in [full_customers_table, full_orders_table, full_products_table, 
                  full_staging_table, full_target_table, full_lineage_table]:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(f"DROP DATABASE IF EXISTS {full_database}")
    print("🧹 Cleanup completed")
else:
    print("⏭️  Cleanup skipped (enable_cleanup=False)")
    print(f"\n📊 Tables created:")
    print(f"   Source Tables:")
    print(f"     - {full_customers_table}")
    print(f"     - {full_orders_table}")
    print(f"     - {full_products_table}")
    print(f"   Transformation Tables:")
    print(f"     - {full_staging_table}")
    print(f"     - {full_target_table}")
    print(f"   Lineage Metadata:")
    print(f"     - {full_lineage_table}")
```
