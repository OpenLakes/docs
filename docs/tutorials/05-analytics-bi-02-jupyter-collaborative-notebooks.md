# Pattern 5.2: Collaborative Data Science with JupyterHub

Enable data scientists to collaborate on analytics using shared Jupyter notebooks:

1. **Shared Environment**: JupyterHub with consistent Python/Spark setup
2. **Lakehouse Access**: Direct access to Iceberg tables via Spark
3. **Version Control**: Git integration for notebook collaboration
4. **Resource Management**: Per-user resource limits and scheduling

## Architecture

```
┌─────────────────────────────────────────────┐
│  Data Science Team                          │
│  - Data Scientists                          │
│  - ML Engineers                             │
│  - Analytics Engineers                      │
└────────────────┬────────────────────────────┘
                 │ (Browser: localhost:8888)
        ┌────────▼────────┐
        │   JupyterHub    │
        │  (Multi-User)   │
        └────────┬────────┘
                 │
        ┌────────▼────────┐
        │  Jupyter Server │
        │  (per user pod) │
        │  PySpark + libs │
        └────────┬────────┘
                 │
        ┌────────▼────────┐
        │  Iceberg Tables │
        │  (via Nessie)   │
        └─────────────────┘
```

**Use Case**: Data science team collaborates on exploratory data analysis, feature engineering, and ML model development using shared notebooks with version control.

**Benefits**:
- Shared computing environment (no "works on my machine")
- Git integration for version control
- Resource isolation per user
- Access to full lakehouse data

## Parameters


```python
execution_date = "2025-01-16"
environment = "development"
database_name = "demo"
customer_events_table = "customer_events"
features_table = "customer_features"
num_customers = 1000
num_events = 5000
enable_validation = True
enable_cleanup = False
```

## Step 1: Initialize Spark Session


```python
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

# Set AWS environment variables for S3FileIO
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "admin123"

spark = SparkSession.builder \
    .appName(f"Pattern-5.2-JupyterCollaboration-{environment}") \
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

print("✅ Spark initialized for collaborative data science")
```

## Step 2: Setup - Create Tables


```python
full_database = f"lakehouse.{database_name}"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_database}")

full_events_table = f"{full_database}.{customer_events_table}"
full_features_table = f"{full_database}.{features_table}"

# Customer events (raw behavioral data)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {full_events_table} (
        customer_id STRING,
        event_type STRING,
        event_timestamp TIMESTAMP,
        product_id STRING,
        amount DOUBLE
    ) USING iceberg
    PARTITIONED BY (days(event_timestamp))
""")

print(f"✅ Created tables for collaborative analytics")
```

## Step 3: Generate Sample Customer Events


```python
# Generate customer behavioral events
event_types = ["page_view", "add_to_cart", "purchase", "review", "search"]
products = [f"prod_{i:03d}" for i in range(1, 51)]
events_data = []

for i in range(num_events):
    customer_id = f"customer_{random.randint(1, num_customers):05d}"
    event_type = random.choice(event_types)
    event_timestamp = datetime.now() - timedelta(
        days=random.randint(0, 90),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )
    product_id = random.choice(products)
    
    # Purchase events have amounts
    amount = round(random.uniform(10.0, 500.0), 2) if event_type == "purchase" else 0.0
    
    events_data.append((customer_id, event_type, event_timestamp, product_id, amount))

events_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("event_timestamp", TimestampType(), False),
    StructField("product_id", StringType(), False),
    StructField("amount", DoubleType(), False)
])

df_events = spark.createDataFrame(events_data, schema=events_schema)
df_events.writeTo(full_events_table).using("iceberg").overwritePartitions()

print(f"✅ Generated {num_events} customer events")
df_events.show(10, truncate=False)
```

## Step 4: Exploratory Data Analysis (EDA)

Data scientists explore the data to understand patterns and prepare features.


```python
# EDA 1: Event type distribution
df_event_dist = spark.sql(f"""
    SELECT 
        event_type,
        COUNT(*) as event_count,
        COUNT(DISTINCT customer_id) as unique_customers
    FROM {full_events_table}
    GROUP BY event_type
    ORDER BY event_count DESC
""")

print("📊 Event Type Distribution:")
df_event_dist.show()
```


```python
# EDA 2: Customer activity patterns
df_customer_activity = spark.sql(f"""
    SELECT 
        customer_id,
        COUNT(*) as total_events,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases,
        SUM(CASE WHEN event_type = 'purchase' THEN amount ELSE 0 END) as total_spent,
        MIN(event_timestamp) as first_event,
        MAX(event_timestamp) as last_event
    FROM {full_events_table}
    GROUP BY customer_id
    ORDER BY total_spent DESC
    LIMIT 20
""")

print("\n📊 Top 20 Customers by Spending:")
df_customer_activity.show(truncate=False)
```

## Step 5: Feature Engineering

Data scientists create features for ML models.


```python
# Create customer features for ML
df_features = spark.sql(f"""
    WITH customer_stats AS (
        SELECT 
            customer_id,
            COUNT(*) as total_events,
            COUNT(DISTINCT DATE(event_timestamp)) as active_days,
            SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) as page_views,
            SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) as cart_adds,
            SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases,
            SUM(CASE WHEN event_type = 'review' THEN 1 ELSE 0 END) as reviews,
            SUM(CASE WHEN event_type = 'search' THEN 1 ELSE 0 END) as searches,
            SUM(CASE WHEN event_type = 'purchase' THEN amount ELSE 0 END) as total_spent,
            AVG(CASE WHEN event_type = 'purchase' THEN amount ELSE NULL END) as avg_purchase_value,
            DATEDIFF(CURRENT_DATE, MAX(DATE(event_timestamp))) as days_since_last_activity,
            DATEDIFF(MAX(DATE(event_timestamp)), MIN(DATE(event_timestamp))) as customer_lifetime_days
        FROM {full_events_table}
        GROUP BY customer_id
    )
    SELECT 
        customer_id,
        total_events,
        active_days,
        page_views,
        cart_adds,
        purchases,
        reviews,
        searches,
        ROUND(total_spent, 2) as total_spent,
        ROUND(COALESCE(avg_purchase_value, 0), 2) as avg_purchase_value,
        -- Derived features
        ROUND(CASE WHEN cart_adds > 0 THEN purchases / cart_adds ELSE 0 END, 3) as conversion_rate,
        ROUND(CASE WHEN customer_lifetime_days > 0 THEN total_events / customer_lifetime_days ELSE 0 END, 2) as events_per_day,
        days_since_last_activity,
        customer_lifetime_days,
        -- Customer segment
        CASE 
            WHEN total_spent >= 1000 THEN 'high_value'
            WHEN total_spent >= 500 THEN 'medium_value'
            WHEN total_spent > 0 THEN 'low_value'
            ELSE 'no_purchase'
        END as customer_segment
    FROM customer_stats
""")

# Save features to Iceberg table
df_features.writeTo(full_features_table).using("iceberg").createOrReplace()

print("✅ Created customer features for ML")
df_features.show(20, truncate=False)
```

## Step 6: Statistical Analysis


```python
# Summary statistics
df_features.select(
    "total_spent",
    "purchases",
    "conversion_rate",
    "events_per_day"
).summary("count", "mean", "stddev", "min", "25%", "50%", "75%", "max").show()

print("\n📊 Customer Segment Distribution:")
df_features.groupBy("customer_segment").count().orderBy(desc("count")).show()
```

## Collaboration Best Practices

This notebook demonstrates collaborative data science patterns:


```python
print("""
💡 JupyterHub Collaboration Best Practices:

## 1. Version Control with Git

**Save notebooks to Git**:
```bash
cd ~/notebooks
git add customer-segmentation.ipynb
git commit -m "Add customer segmentation features"
git push origin feature/customer-features
```

## 2. Shared Notebook Standards

**Notebook Structure**:
- Cell 1: Parameters (execution_date, environment, etc.)
- Cell 2: Imports and Spark initialization
- Cell 3+: Analysis steps with markdown explanations
- Last cell: Cleanup (optional)

**Naming Convention**:
- Descriptive names: `customer-churn-analysis.ipynb`
- Date prefixes: `2025-01-16-sales-forecasting.ipynb`

## 3. Resource Management

**Per-User Limits** (configured in JupyterHub):
```yaml
singleuser:
  memory:
    limit: 4G
    guarantee: 2G
  cpu:
    limit: 2
    guarantee: 0.5
```

**Stop Spark when done**:
```python
spark.stop()  # Free up resources
```

## 4. Data Cataloging

**Document feature tables in OpenMetadata**:
- Table: `lakehouse.demo.customer_features`
- Owner: Data Science Team
- Description: ML features for customer segmentation
- Tags: `ml-features`, `customer-analytics`

## 5. Collaboration Workflow

**Team Workflow**:
1. **Explore** (individual): EDA in personal branch
2. **Share** (team review): Push notebook to Git
3. **Refine** (collaborative): Code review + feedback
4. **Productionize** (engineering): Convert to Airflow DAG

**Example**:
- Data Scientist A: EDA on customer churn patterns
- Data Scientist B: Feature engineering for ML model
- ML Engineer: Review features, suggest improvements
- Analytics Engineer: Productionize as scheduled DAG

## 6. Shared Libraries

**Common functions in shared package**:
```python
# ~/shared/openlakes_utils.py
def get_spark_session(app_name):
    # Standard Spark config for all notebooks
    ...

def load_customer_features(spark, date):
    # Reusable feature loading
    ...
```

**Use in notebooks**:
```python
from openlakes_utils import get_spark_session, load_customer_features
spark = get_spark_session("my-analysis")
df = load_customer_features(spark, "2025-01-16")
```
""")
```

## Validation


```python
if enable_validation:
    events_count = spark.sql(f"SELECT COUNT(*) FROM {full_events_table}").collect()[0][0]
    features_count = spark.sql(f"SELECT COUNT(*) FROM {full_features_table}").collect()[0][0]
    
    assert events_count > 0, "Should have events data"
    print(f"✅ Events: {events_count}")
    
    assert features_count > 0, "Should have features"
    print(f"✅ Customer features: {features_count}")
    
    # Verify feature columns
    feature_columns = df_features.columns
    required_features = ["conversion_rate", "events_per_day", "customer_segment"]
    for feat in required_features:
        assert feat in feature_columns, f"Missing feature: {feat}"
    print(f"✅ All required features present")
    
    test_passed = True
    print("\n✅ All validations passed!")
else:
    test_passed = True
```

## Summary

### ✅ Pattern 5.2: Collaborative Data Science Complete!

Demonstrated:
1. **Shared Environment**: JupyterHub with consistent Spark setup
2. **EDA**: Exploratory analysis of customer events
3. **Feature Engineering**: ML features for customer segmentation
4. **Collaboration**: Git-based workflow for team collaboration

**Collaboration Benefits**:
- 🤝 **Team Collaboration**: Git version control for notebooks
- 🔒 **Resource Isolation**: Per-user resource limits
- 📚 **Shared Libraries**: Common utilities for the team
- 📊 **Consistent Environment**: No "works on my machine" issues
- 🔄 **Workflow**: Explore → Share → Refine → Productionize

**Technology Stack**:

| Layer | Technology | Purpose |
|-------|-----------|----------|
| **Notebook** | JupyterHub | Multi-user collaboration |
| **Compute** | PySpark | Distributed data processing |
| **Storage** | Iceberg | Lakehouse tables |
| **Catalog** | Nessie | Git-like data versioning |
| **Version Control** | Git | Notebook versioning |

**When to Use Jupyter**:
- ✅ Exploratory data analysis (EDA)
- ✅ Feature engineering for ML
- ✅ Ad-hoc analysis and prototyping
- ✅ Team collaboration on analysis

**When to Productionize**:
- ❌ Scheduled production jobs → Convert to Airflow DAG
- ❌ Real-time inference → Deploy to serving layer
- ❌ Complex pipelines → Use orchestration (Airflow)

**Productionization Path**:
```
Jupyter Notebook (exploration)
    ↓
Git Review (team feedback)
    ↓
Python Module (refactor code)
    ↓
Airflow DAG (scheduled execution)
    ↓
Production Pipeline (monitored, tested)
```


```python
if enable_cleanup:
    spark.sql(f"DROP TABLE IF EXISTS {full_events_table}")
    spark.sql(f"DROP TABLE IF EXISTS {full_features_table}")
    print("🧹 Cleanup completed")
else:
    print("⏭️  Cleanup skipped")
```
