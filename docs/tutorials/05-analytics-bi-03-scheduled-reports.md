# Pattern 5.3: Scheduled Reporting with Airflow

Generate automated daily/weekly reports using Airflow orchestration:

1. **Data Source**: Iceberg lakehouse tables with business data
2. **Compute Engine**: Spark for data aggregation and transformation
3. **Orchestration**: Airflow for scheduling report generation
4. **Output Formats**: CSV, JSON, HTML summary reports
5. **Distribution**: Archive to S3, email notifications

## Architecture

```
┌─────────────────────────────────────────────┐
│  Airflow Scheduler                          │
│  - Daily: 6 AM (sales summary)              │
│  - Weekly: Monday 8 AM (executive report)   │
│  - Monthly: 1st day 9 AM (financial)        │
└────────────────┬────────────────────────────┘
                 │ (triggers DAG)
        ┌────────▼────────┐
        │  Spark Job      │
        │  (Aggregation)  │
        └────────┬────────┘
                 │
        ┌────────▼────────┐
        │  Iceberg Tables │
        │  (Lakehouse)    │
        └────────┬────────┘
                 │
        ┌────────▼────────┐
        │  Report Output  │
        │  - S3 Archive   │
        │  - Email Notify │
        └─────────────────┘
```

**Use Case**: Finance team receives automated daily sales reports, executives get weekly performance summaries, compliance team gets monthly audit reports—all generated automatically without manual intervention.

**Benefits**:
- Automated report generation (no manual exports)
- Consistent scheduling (daily/weekly/monthly)
- Multiple output formats (CSV, Excel, PDF, HTML)
- Email notifications with summaries
- Historical archiving to S3

## Parameters


```python
execution_date = "2025-01-16"
environment = "development"
database_name = "demo"
sales_table = "daily_sales"
products_table = "products"
reports_bucket = "openlakes"
reports_prefix = "reports/scheduled"
num_sales_records = 1000
num_products = 50
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
import json

# Set AWS environment variables for S3FileIO
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "admin123"

spark = SparkSession.builder \
    .appName(f"Pattern-5.3-ScheduledReports-{environment}") \
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

print("✅ Spark initialized for scheduled reporting")
```

## Step 2: Setup - Create Tables


```python
full_database = f"lakehouse.{database_name}"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_database}")

full_sales_table = f"{full_database}.{sales_table}"
full_products_table = f"{full_database}.{products_table}"

# Daily sales transactions
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {full_sales_table} (
        transaction_id STRING,
        transaction_date DATE,
        product_id STRING,
        customer_id STRING,
        region STRING,
        channel STRING,
        quantity BIGINT,
        unit_price DOUBLE,
        total_amount DOUBLE,
        discount_amount DOUBLE
    ) USING iceberg
    PARTITIONED BY (days(transaction_date))
""")

# Product catalog
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {full_products_table} (
        product_id STRING,
        product_name STRING,
        category STRING,
        subcategory STRING,
        cost DOUBLE,
        list_price DOUBLE
    ) USING iceberg
""")

print(f"✅ Created tables for scheduled reporting")
```

## Step 3: Generate Sample Product Data


```python
# Generate product catalog
categories = [
    ("Electronics", ["Smartphones", "Laptops", "Tablets", "Accessories"]),
    ("Clothing", ["Men", "Women", "Kids", "Accessories"]),
    ("Home", ["Furniture", "Decor", "Kitchen", "Bedding"]),
    ("Sports", ["Fitness", "Outdoor", "Team Sports", "Water Sports"]),
    ("Books", ["Fiction", "Non-Fiction", "Educational", "Children"])
]

products_data = []
product_id_counter = 1

for category, subcategories in categories:
    for subcategory in subcategories:
        for i in range(num_products // len(categories) // len(subcategories) + 1):
            if product_id_counter > num_products:
                break
            product_id = f"PROD{product_id_counter:04d}"
            product_name = f"{category} {subcategory} Item {i+1}"
            cost = round(random.uniform(10.0, 200.0), 2)
            list_price = round(cost * random.uniform(1.3, 2.5), 2)
            
            products_data.append((
                product_id, product_name, category, subcategory, cost, list_price
            ))
            product_id_counter += 1

products_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("subcategory", StringType(), False),
    StructField("cost", DoubleType(), False),
    StructField("list_price", DoubleType(), False)
])

df_products = spark.createDataFrame(products_data, schema=products_schema)
df_products.writeTo(full_products_table).using("iceberg").createOrReplace()

print(f"✅ Generated {len(products_data)} products")
df_products.show(10, truncate=False)
```

## Step 4: Generate Sample Sales Data


```python
# Generate sales transactions for last 30 days
regions = ["North America", "Europe", "Asia", "South America", "Australia"]
channels = ["Online", "Retail Store", "Mobile App", "Partner"]
sales_data = []

# Get product IDs
product_ids = [row.product_id for row in df_products.select("product_id").collect()]

for i in range(num_sales_records):
    transaction_id = f"TXN{i+1:08d}"
    transaction_date = (datetime.now() - timedelta(days=random.randint(0, 30))).date()
    product_id = random.choice(product_ids)
    customer_id = f"CUST{random.randint(1, 500):05d}"
    region = random.choice(regions)
    channel = random.choice(channels)
    quantity = random.randint(1, 10)
    
    # Get product price
    product = df_products.filter(df_products.product_id == product_id).first()
    unit_price = product.list_price
    
    # Random discount
    discount_pct = random.choice([0, 0, 0, 0.05, 0.10, 0.15, 0.20])
    discount_amount = round(unit_price * quantity * discount_pct, 2)
    total_amount = round(unit_price * quantity - discount_amount, 2)
    
    sales_data.append((
        transaction_id, transaction_date, product_id, customer_id, region, channel,
        quantity, unit_price, total_amount, discount_amount
    ))

sales_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("transaction_date", DateType(), False),
    StructField("product_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("region", StringType(), False),
    StructField("channel", StringType(), False),
    StructField("quantity", LongType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("total_amount", DoubleType(), False),
    StructField("discount_amount", DoubleType(), False)
])

df_sales = spark.createDataFrame(sales_data, schema=sales_schema)
df_sales.writeTo(full_sales_table).using("iceberg").overwritePartitions()

print(f"✅ Generated {num_sales_records} sales transactions")
df_sales.show(10, truncate=False)
```

## Step 5: Daily Sales Summary Report

This would be scheduled to run daily at 6 AM via Airflow.


```python
# Daily sales summary (yesterday's sales)
report_date = (datetime.now() - timedelta(days=1)).date()

df_daily_summary = spark.sql(f"""
    WITH daily_sales AS (
        SELECT 
            s.transaction_date,
            s.region,
            s.channel,
            p.category,
            COUNT(DISTINCT s.transaction_id) as num_transactions,
            COUNT(DISTINCT s.customer_id) as unique_customers,
            SUM(s.quantity) as total_units,
            SUM(s.total_amount) as gross_revenue,
            SUM(s.discount_amount) as total_discounts,
            SUM(s.total_amount) + SUM(s.discount_amount) as revenue_before_discount,
            SUM(s.quantity * p.cost) as total_cost
        FROM {full_sales_table} s
        JOIN {full_products_table} p ON s.product_id = p.product_id
        WHERE s.transaction_date = CURRENT_DATE - INTERVAL 1 DAY
        GROUP BY s.transaction_date, s.region, s.channel, p.category
    )
    SELECT 
        transaction_date,
        region,
        channel,
        category,
        num_transactions,
        unique_customers,
        total_units,
        ROUND(gross_revenue, 2) as gross_revenue,
        ROUND(total_discounts, 2) as total_discounts,
        ROUND(total_cost, 2) as total_cost,
        ROUND(gross_revenue - total_cost, 2) as gross_profit,
        ROUND((gross_revenue - total_cost) / gross_revenue * 100, 2) as profit_margin_pct,
        ROUND(total_discounts / revenue_before_discount * 100, 2) as discount_rate_pct
    FROM daily_sales
    ORDER BY gross_revenue DESC
""")

print(f"📊 Daily Sales Summary Report - {report_date}")
df_daily_summary.show(20, truncate=False)

# Save to S3 as CSV
daily_report_path = f"s3a://{reports_bucket}/{reports_prefix}/daily/{report_date}/sales_summary.csv"
df_daily_summary.coalesce(1).write.mode("overwrite").option("header", "true").csv(daily_report_path)
print(f"✅ Daily report saved to: {daily_report_path}")
```

## Step 6: Executive Weekly Summary Report

This would be scheduled to run weekly on Monday at 8 AM via Airflow.


```python
# Weekly executive summary (last 7 days)
df_weekly_summary = spark.sql(f"""
    WITH weekly_metrics AS (
        SELECT 
            DATE_TRUNC('week', s.transaction_date) as week_start,
            COUNT(DISTINCT s.transaction_id) as total_transactions,
            COUNT(DISTINCT s.customer_id) as unique_customers,
            SUM(s.quantity) as total_units_sold,
            SUM(s.total_amount) as total_revenue,
            SUM(s.discount_amount) as total_discounts,
            SUM(s.quantity * p.cost) as total_cost,
            AVG(s.total_amount) as avg_transaction_value
        FROM {full_sales_table} s
        JOIN {full_products_table} p ON s.product_id = p.product_id
        WHERE s.transaction_date >= CURRENT_DATE - INTERVAL 7 DAYS
        GROUP BY DATE_TRUNC('week', s.transaction_date)
    ),
    top_regions AS (
        SELECT 
            region,
            SUM(total_amount) as revenue,
            ROW_NUMBER() OVER (ORDER BY SUM(total_amount) DESC) as rank
        FROM {full_sales_table}
        WHERE transaction_date >= CURRENT_DATE - INTERVAL 7 DAYS
        GROUP BY region
    ),
    top_categories AS (
        SELECT 
            p.category,
            SUM(s.total_amount) as revenue,
            ROW_NUMBER() OVER (ORDER BY SUM(s.total_amount) DESC) as rank
        FROM {full_sales_table} s
        JOIN {full_products_table} p ON s.product_id = p.product_id
        WHERE s.transaction_date >= CURRENT_DATE - INTERVAL 7 DAYS
        GROUP BY p.category
    )
    SELECT 
        week_start,
        total_transactions,
        unique_customers,
        total_units_sold,
        ROUND(total_revenue, 2) as total_revenue,
        ROUND(total_cost, 2) as total_cost,
        ROUND(total_revenue - total_cost, 2) as gross_profit,
        ROUND((total_revenue - total_cost) / total_revenue * 100, 2) as profit_margin_pct,
        ROUND(avg_transaction_value, 2) as avg_transaction_value,
        (
            SELECT STRING_AGG(CONCAT(region, ': $', CAST(ROUND(revenue, 0) AS STRING)), ', ')
            FROM top_regions
            WHERE rank <= 3
        ) as top_3_regions,
        (
            SELECT STRING_AGG(CONCAT(category, ': $', CAST(ROUND(revenue, 0) AS STRING)), ', ')
            FROM top_categories
            WHERE rank <= 3
        ) as top_3_categories
    FROM weekly_metrics
""")

print("📊 Executive Weekly Summary Report (Last 7 Days)")
df_weekly_summary.show(truncate=False)

# Save to S3 as JSON for programmatic access
week_start = (datetime.now() - timedelta(days=datetime.now().weekday())).strftime("%Y-%m-%d")
weekly_report_path = f"s3a://{reports_bucket}/{reports_prefix}/weekly/{week_start}/executive_summary.json"
df_weekly_summary.coalesce(1).write.mode("overwrite").json(weekly_report_path)
print(f"✅ Weekly report saved to: {weekly_report_path}")
```

## Step 7: Channel Performance Report

Detailed breakdown by sales channel for operational teams.


```python
# Channel performance report
df_channel_report = spark.sql(f"""
    SELECT 
        s.channel,
        COUNT(DISTINCT s.transaction_id) as total_orders,
        COUNT(DISTINCT s.customer_id) as unique_customers,
        SUM(s.quantity) as total_units,
        ROUND(SUM(s.total_amount), 2) as total_revenue,
        ROUND(AVG(s.total_amount), 2) as avg_order_value,
        ROUND(SUM(s.discount_amount) / (SUM(s.total_amount) + SUM(s.discount_amount)) * 100, 2) as discount_rate_pct,
        ROUND(SUM(s.total_amount) / COUNT(DISTINCT s.customer_id), 2) as revenue_per_customer
    FROM {full_sales_table} s
    WHERE s.transaction_date >= CURRENT_DATE - INTERVAL 7 DAYS
    GROUP BY s.channel
    ORDER BY total_revenue DESC
""")

print("📊 Channel Performance Report (Last 7 Days)")
df_channel_report.show(truncate=False)

# Save to S3
channel_report_path = f"s3a://{reports_bucket}/{reports_prefix}/weekly/{week_start}/channel_performance.csv"
df_channel_report.coalesce(1).write.mode("overwrite").option("header", "true").csv(channel_report_path)
print(f"✅ Channel report saved to: {channel_report_path}")
```

## Step 8: Generate Email Summary Metrics

Create summary metrics for email notifications.


```python
# Generate summary metrics for email notification
summary_metrics = spark.sql(f"""
    SELECT 
        COUNT(DISTINCT transaction_id) as total_transactions,
        COUNT(DISTINCT customer_id) as unique_customers,
        ROUND(SUM(total_amount), 2) as total_revenue,
        ROUND(AVG(total_amount), 2) as avg_transaction,
        ROUND(SUM(discount_amount), 2) as total_discounts
    FROM {full_sales_table}
    WHERE transaction_date >= CURRENT_DATE - INTERVAL 1 DAY
""").first()

# Create email summary
email_summary = {
    "report_type": "Daily Sales Summary",
    "report_date": str(report_date),
    "metrics": {
        "total_transactions": summary_metrics.total_transactions,
        "unique_customers": summary_metrics.unique_customers,
        "total_revenue": float(summary_metrics.total_revenue),
        "avg_transaction": float(summary_metrics.avg_transaction),
        "total_discounts": float(summary_metrics.total_discounts)
    },
    "report_links": {
        "daily_detail": daily_report_path,
        "weekly_summary": weekly_report_path,
        "channel_performance": channel_report_path
    }
}

print("📧 Email Summary Metrics:")
print(json.dumps(email_summary, indent=2))

# In production, this would be sent via Airflow EmailOperator
print("\n💡 In production, this would trigger:")
print("   - EmailOperator to send summary to executives")
print("   - SlackWebhookOperator for team notifications")
print("   - Archive reports to S3 with lifecycle policies")
```

## Airflow DAG Configuration (Production)


```python
print("""
💡 Airflow DAG for Scheduled Reporting:

## Daily Sales Report DAG

```python
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email': ['analytics@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'daily_sales_report',
    default_args=default_args,
    description='Generate daily sales summary report',
    schedule_interval='0 6 * * *',  # Daily at 6 AM
    catchup=False,
    tags=['reporting', 'sales', 'daily']
) as dag:

    # Task 1: Generate daily report
    generate_report = SparkSubmitOperator(
        task_id='generate_daily_sales_report',
        application='/opt/airflow/dags/reports/daily_sales_report.py',
        name='daily-sales-report',
        conn_id='spark_default',
        conf={
            'spark.executor.instances': '2',
            'spark.executor.memory': '2g',
            'spark.executor.cores': '2'
        },
        application_args=[
            '--execution-date', '{{ ds }}',
            '--environment', 'production'
        ]
    )

    # Task 2: Send email notification
    send_email = EmailOperator(
        task_id='send_email_notification',
        to=['executives@company.com', 'sales@company.com'],
        subject='Daily Sales Report - {{ ds }}',
        html_content='''
            <h2>Daily Sales Summary - {{ ds }}</h2>
            <p>Your daily sales report is ready.</p>
            <ul>
                <li><strong>Total Revenue:</strong> {{ task_instance.xcom_pull(task_ids='generate_daily_sales_report', key='total_revenue') }}</li>
                <li><strong>Transactions:</strong> {{ task_instance.xcom_pull(task_ids='generate_daily_sales_report', key='total_transactions') }}</li>
            </ul>
            <p><a href="https://s3.company.com/reports/daily/{{ ds }}/sales_summary.csv">Download Full Report</a></p>
        '''
    )

    generate_report >> send_email
```

## Weekly Executive Report DAG

```python
with DAG(
    'weekly_executive_report',
    default_args=default_args,
    description='Generate weekly executive summary',
    schedule_interval='0 8 * * 1',  # Every Monday at 8 AM
    catchup=False,
    tags=['reporting', 'executive', 'weekly']
) as dag:

    generate_weekly = SparkSubmitOperator(
        task_id='generate_weekly_executive_report',
        application='/opt/airflow/dags/reports/weekly_executive_report.py',
        name='weekly-executive-report',
        conn_id='spark_default'
    )

    send_exec_email = EmailOperator(
        task_id='send_executive_email',
        to=['ceo@company.com', 'cfo@company.com'],
        subject='Weekly Executive Summary - Week of {{ ds }}',
        html_content='''...(executive dashboard HTML)...'''
    )

    generate_weekly >> send_exec_email
```

## Key Features:

1. **Scheduling**:
   - Daily: 6 AM (after overnight data loads)
   - Weekly: Monday 8 AM
   - Monthly: 1st day at 9 AM

2. **Error Handling**:
   - Retry failed tasks (2 retries, 5-min delay)
   - Email on failure
   - SLA monitoring

3. **Distribution**:
   - Email with summary metrics
   - S3 links to full reports
   - Slack notifications

4. **Archiving**:
   - Reports saved to S3 with date partitions
   - 90-day retention policy
   - Compressed for long-term storage

5. **Monitoring**:
   - Track report generation time
   - Alert if report missing
   - Dashboard of report metrics
""")
```

## Validation


```python
if enable_validation:
    # Validate data exists
    sales_count = spark.sql(f"SELECT COUNT(*) FROM {full_sales_table}").collect()[0][0]
    products_count = spark.sql(f"SELECT COUNT(*) FROM {full_products_table}").collect()[0][0]
    
    assert sales_count > 0, "Should have sales data"
    print(f"✅ Sales transactions: {sales_count}")
    
    assert products_count > 0, "Should have products"
    print(f"✅ Products: {products_count}")
    
    # Validate reports generated
    assert df_daily_summary.count() > 0, "Daily summary should have data"
    print(f"✅ Daily summary report: {df_daily_summary.count()} rows")
    
    assert df_weekly_summary.count() > 0, "Weekly summary should have data"
    print(f"✅ Weekly summary report: {df_weekly_summary.count()} rows")
    
    assert df_channel_report.count() > 0, "Channel report should have data"
    print(f"✅ Channel performance report: {df_channel_report.count()} rows")
    
    # Validate email summary
    assert 'metrics' in email_summary, "Email summary should have metrics"
    assert 'report_links' in email_summary, "Email summary should have report links"
    print(f"✅ Email summary generated with {len(email_summary['metrics'])} metrics")
    
    test_passed = True
    print("\n✅ All validations passed!")
else:
    test_passed = True
```

## Summary

### ✅ Pattern 5.3: Scheduled Reporting Complete!

Demonstrated:
1. **Daily Reports**: Automated sales summary with detailed metrics
2. **Weekly Reports**: Executive summary with KPIs and trends
3. **Channel Analysis**: Performance breakdown by sales channel
4. **Email Notifications**: Summary metrics and report links
5. **S3 Archiving**: Historical reports with date partitioning

**Reporting Benefits**:
- 📅 **Automated Scheduling**: Daily/weekly/monthly reports without manual intervention
- 📊 **Consistent Metrics**: Same calculations every time, no human error
- 📧 **Proactive Distribution**: Email/Slack notifications to stakeholders
- 📁 **Historical Archive**: S3 storage for compliance and trend analysis
- 🔄 **Multiple Formats**: CSV, JSON, HTML for different use cases
- ⚡ **Fast Processing**: Spark aggregations on lakehouse data

**Technology Stack**:

| Layer | Technology | Purpose |
|-------|-----------|----------|
| **Orchestration** | Airflow | Scheduling and workflow management |
| **Compute** | Spark | Data aggregation and transformation |
| **Storage** | Iceberg | Source data in lakehouse |
| **Archive** | S3 (MinIO) | Report storage and retention |
| **Notification** | Email/Slack | Report distribution |

**Report Types**:

1. **Daily Operational Reports**:
   - Sales summary by region/channel/category
   - Inventory levels and reorder alerts
   - Customer service metrics
   - Schedule: 6 AM daily

2. **Weekly Business Reports**:
   - Executive KPI dashboard
   - Sales trends and forecasts
   - Marketing campaign performance
   - Schedule: Monday 8 AM

3. **Monthly Compliance Reports**:
   - Financial reconciliation
   - Audit trails and data quality
   - Regulatory compliance metrics
   - Schedule: 1st day 9 AM

**Output Formats**:

- **CSV**: For Excel import, data analysis
- **JSON**: For programmatic access, APIs
- **Parquet**: For data archival, reprocessing
- **HTML**: For email body (future enhancement)
- **PDF**: For formal reports (future enhancement)

**Production Best Practices**:

1. **Idempotency**: Reports can be re-run safely for same date
2. **Partitioning**: Date-based folders in S3 for easy access
3. **Retention**: Lifecycle policies (90 days hot, 1 year archive, then delete)
4. **Monitoring**: Track report generation time, failure alerts
5. **SLA**: Define expected completion times (e.g., daily by 7 AM)

**Airflow DAG Pattern**:

```
Extract Data → Transform/Aggregate → Save Report → Send Notification
     ↓                 ↓                   ↓               ↓
  Iceberg         Spark SQL           S3 Write      Email/Slack
```

**When to Use Scheduled Reports**:
- ✅ Regular business metrics (daily/weekly/monthly)
- ✅ Executive dashboards and KPIs
- ✅ Compliance and audit reports
- ✅ Email distribution to stakeholders

**When to Use Interactive Dashboards**:
- ❌ Ad-hoc exploration (use Superset)
- ❌ Real-time metrics (use streaming dashboards)
- ❌ Self-service analytics (use BI tools)

**Future Enhancements**:
- PDF generation with charts (using matplotlib/seaborn)
- Excel formatting with multiple sheets
- Interactive HTML emails with embedded visualizations
- Microsoft Teams / Slack rich notifications
- Report subscriptions (users choose reports/frequency)


```python
if enable_cleanup:
    spark.sql(f"DROP TABLE IF EXISTS {full_sales_table}")
    spark.sql(f"DROP TABLE IF EXISTS {full_products_table}")
    print("🧹 Cleanup completed")
else:
    print("⏭️  Cleanup skipped")
```
