```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType, LongType
from pyspark.sql.functions import col, sum as spark_sum, count, avg, when, expr, lit, window, current_timestamp, from_json
import random
from datetime import datetime, date, timedelta

```

# Pattern 5.1: Self-Service SQL Analytics with Superset

Enable business users to create SQL-based dashboards using Superset connected to Trino/Iceberg:

1. **Data Source**: Iceberg lakehouse tables
2. **Query Engine**: Trino for fast SQL queries
3. **Visualization**: Superset for dashboards, charts, alerts
4. **Self-Service**: Business users create their own analytics

## Architecture

```
┌─────────────────────────────────────────────┐
│  Business Users (Self-Service)              │
│  - Marketing analysts                       │
│  - Sales managers                           │
│  - Product owners                           │
└────────────────┬────────────────────────────┘
                 │ (SQL + Drag-Drop)
        ┌────────▼────────┐
        │    Superset     │
        │   (BI Layer)    │
        └────────┬────────┘
                 │ (SQL queries)
        ┌────────▼────────┐
        │      Trino      │
        │  (Query Engine) │
        └────────┬────────┘
                 │
        ┌────────▼────────┐
        │  Iceberg Tables │
        │   (Lakehouse)   │
        └─────────────────┘
```

**Use Case**: Marketing team creates dashboards tracking campaign performance, sales team monitors regional trends, product team analyzes feature adoption—all without data engineering support.

**Benefits**:
- Self-service analytics (no tickets to data team)
- SQL + drag-drop interface
- Scheduled refresh (hourly/daily)
- Alerts on metric thresholds
- Role-based access control

## Parameters


```python
execution_date = "2025-01-16"
environment = "development"
database_name = "demo"
sales_table = "sales_data"
campaigns_table = "marketing_campaigns"
num_sales_records = 500
num_campaigns = 10
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
    .appName(f"Pattern-5.1-SupersetSelfService-{environment}") \
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

print("✅ Spark initialized for self-service analytics pattern")
```

## Setup: Create Analytics Tables


```python
full_database = f"lakehouse.{database_name}"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_database}")

full_sales_table = f"{full_database}.{sales_table}"
full_campaigns_table = f"{full_database}.{campaigns_table}"

# Sales data table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {full_sales_table} (
        sale_id STRING,
        sale_date DATE,
        region STRING,
        product_category STRING,
        revenue DOUBLE,
        units_sold BIGINT,
        campaign_id STRING
    ) USING iceberg
    PARTITIONED BY (days(sale_date))
""")

# Marketing campaigns table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {full_campaigns_table} (
        campaign_id STRING,
        campaign_name STRING,
        channel STRING,
        budget DOUBLE,
        start_date DATE,
        end_date DATE
    ) USING iceberg
""")

print(f"✅ Created analytics tables for self-service BI")
```

## Generate Sample Data: Marketing Campaigns


```python
# Generate marketing campaigns
channels = ["Email", "Social Media", "Search Ads", "Display Ads", "TV"]
campaigns_data = []

for i in range(num_campaigns):
    campaign_id = f"campaign_{i+1:03d}"
    channel = random.choice(channels)
    campaign_name = f"{channel} Campaign Q{random.randint(1,4)}"
    budget = round(random.uniform(10000, 100000), 2)
    start_date = (datetime.now() - timedelta(days=random.randint(60, 180))).date()
    end_date = start_date + timedelta(days=random.randint(30, 90))
    
    campaigns_data.append((
        campaign_id, campaign_name, channel, budget, start_date, end_date
    ))

campaigns_schema = StructType([
    StructField("campaign_id", StringType(), False),
    StructField("campaign_name", StringType(), False),
    StructField("channel", StringType(), False),
    StructField("budget", DoubleType(), False),
    StructField("start_date", DateType(), False),
    StructField("end_date", DateType(), False)
])

df_campaigns = spark.createDataFrame(campaigns_data, schema=campaigns_schema)
df_campaigns.writeTo(full_campaigns_table).using("iceberg").overwritePartitions()

print(f"✅ Generated {num_campaigns} marketing campaigns")
df_campaigns.show(10, truncate=False)
```

## Generate Sample Data: Sales Records


```python
# Generate sales data
regions = ["North America", "Europe", "Asia", "South America", "Australia"]
categories = ["Electronics", "Clothing", "Books", "Home", "Sports"]
sales_data = []

for i in range(num_sales_records):
    sale_id = f"sale_{i+1:06d}"
    sale_date = (datetime.now() - timedelta(days=random.randint(0, 90))).date()
    region = random.choice(regions)
    category = random.choice(categories)
    revenue = round(random.uniform(50.0, 5000.0), 2)
    units_sold = random.randint(1, 20)
    campaign_id = f"campaign_{random.randint(1, num_campaigns):03d}"
    
    sales_data.append((
        sale_id, sale_date, region, category, revenue, units_sold, campaign_id
    ))

sales_schema = StructType([
    StructField("sale_id", StringType(), False),
    StructField("sale_date", DateType(), False),
    StructField("region", StringType(), False),
    StructField("product_category", StringType(), False),
    StructField("revenue", DoubleType(), False),
    StructField("units_sold", LongType(), False),
    StructField("campaign_id", StringType(), False)
])

df_sales = spark.createDataFrame(sales_data, schema=sales_schema)
df_sales.writeTo(full_sales_table).using("iceberg").overwritePartitions()

print(f"✅ Generated {num_sales_records} sales records")
df_sales.show(10, truncate=False)
```

## Example Analytics Queries (Self-Service BI)

These are examples of queries business users would create in Superset.


```python
# Query 1: Regional Sales Performance
df_regional_sales = spark.sql(f"""
    SELECT 
        region,
        COUNT(*) as total_sales,
        SUM(revenue) as total_revenue,
        AVG(revenue) as avg_sale_value,
        SUM(units_sold) as total_units
    FROM {full_sales_table}
    WHERE sale_date >= CURRENT_DATE - INTERVAL 30 DAYS
    GROUP BY region
    ORDER BY total_revenue DESC
""")

print("📊 Dashboard 1: Regional Sales Performance (Last 30 Days)")
df_regional_sales.show(truncate=False)
```


```python
# Query 2: Campaign ROI Analysis
df_campaign_roi = spark.sql(f"""
    SELECT 
        c.campaign_name,
        c.channel,
        c.budget,
        SUM(s.revenue) as campaign_revenue,
        COUNT(s.sale_id) as attributed_sales,
        ROUND((SUM(s.revenue) - c.budget) / c.budget * 100, 2) as roi_percentage
    FROM {full_sales_table} s
    JOIN {full_campaigns_table} c ON s.campaign_id = c.campaign_id
    GROUP BY c.campaign_name, c.channel, c.budget
    ORDER BY roi_percentage DESC
""")

print("\n📊 Dashboard 2: Campaign ROI Analysis")
df_campaign_roi.show(truncate=False)
```


```python
# Query 3: Product Category Trends
df_category_trends = spark.sql(f"""
    SELECT 
        product_category,
        DATE_TRUNC('week', sale_date) as week_start,
        SUM(revenue) as weekly_revenue,
        SUM(units_sold) as weekly_units
    FROM {full_sales_table}
    WHERE sale_date >= CURRENT_DATE - INTERVAL 60 DAYS
    GROUP BY product_category, DATE_TRUNC('week', sale_date)
    ORDER BY week_start DESC, weekly_revenue DESC
""")

print("\n📊 Dashboard 3: Product Category Trends (Weekly)")
df_category_trends.show(20, truncate=False)
```


```python
# Query 4: Channel Performance
df_channel_performance = spark.sql(f"""
    SELECT 
        c.channel,
        COUNT(DISTINCT c.campaign_id) as num_campaigns,
        SUM(c.budget) as total_budget,
        SUM(s.revenue) as total_revenue,
        AVG(s.revenue) as avg_sale_value
    FROM {full_campaigns_table} c
    LEFT JOIN {full_sales_table} s ON c.campaign_id = s.campaign_id
    GROUP BY c.channel
    ORDER BY total_revenue DESC
""")

print("\n📊 Dashboard 4: Marketing Channel Performance")
df_channel_performance.show(truncate=False)
```

## Superset Configuration (Production)

Instructions for connecting Superset to Trino/Iceberg:


```python
print("""\n💡 Superset Self-Service Setup:

## 1. Add Database Connection in Superset

Navigate to: Settings → Database Connections → + Database

**Connection String**:
```
trino://admin@analytics-trino:8080/iceberg/demo
```

**Advanced Settings**:
```json
{
  "metadata_params": {},
  "engine_params": {},
  "metadata_cache_timeout": {},
  "schemas_allowed_for_file_upload": []
}
```

## 2. Create Datasets (Virtual Tables)

Navigate to: Data → Datasets → + Dataset

**Dataset 1: Sales Performance**
```sql
SELECT 
    region,
    product_category,
    sale_date,
    SUM(revenue) as revenue,
    SUM(units_sold) as units
FROM iceberg.demo.sales_data
GROUP BY region, product_category, sale_date
```

**Dataset 2: Campaign ROI**
```sql
SELECT 
    c.campaign_name,
    c.channel,
    c.budget,
    SUM(s.revenue) as revenue,
    (SUM(s.revenue) - c.budget) / c.budget * 100 as roi_pct
FROM iceberg.demo.sales_data s
JOIN iceberg.demo.marketing_campaigns c ON s.campaign_id = c.campaign_id
GROUP BY c.campaign_name, c.channel, c.budget
```

## 3. Create Charts (Self-Service)

Business users can now create charts:

**Chart Type Examples**:
- Bar Chart: Regional revenue comparison
- Time Series: Weekly category trends
- Pie Chart: Channel budget allocation
- Table: Campaign performance details

**Drag-Drop Interface**:
- Metrics: revenue, units, roi_pct
- Dimensions: region, category, channel
- Filters: date range, region, campaign

## 4. Schedule Dashboard Refresh

Navigate to: Dashboard → Settings → Refresh Interval

**Options**:
- Hourly: For near-real-time dashboards
- Daily: For executive reports
- Weekly: For trend analysis

## 5. Set Up Alerts

Navigate to: Dashboard → Alerts → + Alert

**Example Alert**:
```
Alert: Low ROI Campaign
Condition: roi_pct < 10
Frequency: Daily
Recipients: marketing@company.com
```

## Benefits of Self-Service:

✅ **No Data Team Dependency**: Marketing creates own dashboards
✅ **SQL + Drag-Drop**: Flexible for technical and non-technical users
✅ **Scheduled Refresh**: Always up-to-date data
✅ **Alerts**: Proactive notifications on metric thresholds
✅ **RBAC**: Role-based access control for data security
""")
```

## Validation


```python
if enable_validation:
    sales_count = spark.sql(f"SELECT COUNT(*) FROM {full_sales_table}").collect()[0][0]
    campaigns_count = spark.sql(f"SELECT COUNT(*) FROM {full_campaigns_table}").collect()[0][0]
    
    assert sales_count > 0, "Should have sales data"
    print(f"✅ Sales records: {sales_count}")
    
    assert campaigns_count > 0, "Should have campaigns"
    print(f"✅ Marketing campaigns: {campaigns_count}")
    
    # Verify queries executed successfully
    regional_count = df_regional_sales.count()
    assert regional_count > 0, "Regional sales query should return results"
    print(f"✅ Regional sales analysis: {regional_count} regions")
    
    campaign_roi_count = df_campaign_roi.count()
    assert campaign_roi_count > 0, "Campaign ROI query should return results"
    print(f"✅ Campaign ROI analysis: {campaign_roi_count} campaigns")
    
    test_passed = True
    print("\n✅ All validations passed!")
else:
    test_passed = True
```

## Summary

### ✅ Pattern 5.1: Self-Service SQL Analytics Complete!

Demonstrated:
1. **Data Preparation**: Sales and marketing campaign tables in Iceberg
2. **Self-Service Queries**: Regional performance, campaign ROI, category trends
3. **Superset Integration**: Connection to Trino/Iceberg for BI dashboards
4. **Business Value**: Marketing, sales, and product teams create own analytics

**Self-Service Benefits**:
- 🚀 **Reduced Data Team Backlog**: Business users create own dashboards
- ⚡ **Faster Insights**: No waiting for data engineering tickets
- 📊 **SQL + Drag-Drop**: Accessible to technical and non-technical users
- 🔄 **Scheduled Refresh**: Dashboards auto-update (hourly/daily)
- 🔔 **Proactive Alerts**: Notifications on metric thresholds
- 🔒 **RBAC**: Role-based data access

**Technology Stack**:

| Layer | Technology | Purpose |
|-------|-----------|----------|
| **Storage** | Iceberg | Lakehouse tables |
| **Query** | Trino | Fast SQL analytics |
| **BI** | Superset | Dashboards, charts, alerts |
| **Auth** | LDAP/OAuth | User authentication |

**When to Use Self-Service**:
- ✅ Standard SQL queries (aggregations, joins, filters)
- ✅ Business users comfortable with SQL
- ✅ High volume of ad-hoc requests
- ✅ Need for departmental autonomy

**When to Use Engineered Dashboards**:
- ❌ Complex transformations (feature engineering)
- ❌ Real-time streaming data (use dedicated streaming layer)
- ❌ Custom Python/R analytics
- ❌ Machine learning predictions

**Production Deployment**:
```yaml
# Superset values.yaml
supersetNode:
  connections:
    db_uri: trino://admin@analytics-trino:8080/iceberg/prod
  
  # RBAC configuration
  auth_type: LDAP
  
  # Cache configuration
  cache_config:
    CACHE_TYPE: redis
    CACHE_DEFAULT_TIMEOUT: 3600
```

**Governance Best Practices**:
1. **Data Catalog**: Document tables in OpenMetadata
2. **Certified Datasets**: Pre-built, validated queries
3. **Query Limits**: Prevent runaway queries (timeout, row limit)
4. **Role-Based Access**: Limit sensitive data by department
5. **Usage Monitoring**: Track which dashboards are actively used


```python
if enable_cleanup:
    spark.sql(f"DROP TABLE IF EXISTS {full_sales_table}")
    spark.sql(f"DROP TABLE IF EXISTS {full_campaigns_table}")
    print("🧹 Cleanup completed")
else:
    print("⏭️  Cleanup skipped")
```
