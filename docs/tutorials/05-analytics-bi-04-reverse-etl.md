# Pattern 5.4: Reverse ETL - Analytics to Operational Systems

Push analytics results from the lakehouse back to operational databases and external systems:

1. **Data Flow**: Lakehouse (analytics) → Operational databases (PostgreSQL, APIs)
2. **Use Cases**: Customer segmentation, product recommendations, ML predictions
3. **Target Systems**: PostgreSQL for app personalization, external APIs for CRM/marketing tools
4. **Compute**: Spark reads from Iceberg, writes to operational stores

## Architecture

```
┌─────────────────────────────────────────────┐
│  Iceberg Lakehouse (Analytics Layer)        │
│  - ML model predictions                     │
│  - Customer segments                        │
│  - Product recommendations                  │
│  - Aggregated metrics                       │
└────────────────┬────────────────────────────┘
                 │ (Reverse ETL)
        ┌────────▼────────┐
        │  Spark Job      │
        │  (Transform)    │
        └────────┬────────┘
                 │
      ┌──────────┴──────────┐
      ▼                     ▼
┌─────────────┐    ┌────────────────┐
│ PostgreSQL  │    │  External APIs │
│ (App DB)    │    │  - Salesforce  │
│             │    │  - HubSpot     │
│ - Segments  │    │  - Braze       │
│ - Scores    │    │  - Segment     │
└─────────────┘    └────────────────┘
      │                     │
      ▼                     ▼
┌─────────────┐    ┌────────────────┐
│ Web/Mobile  │    │  Marketing     │
│ Application │    │  Automation    │
└─────────────┘    └────────────────┘
```

**Use Case**: ML team trains customer churn model on lakehouse data → predictions pushed to PostgreSQL → app shows retention offers to at-risk customers in real-time.

**Benefits**:
- Operationalize analytics (ML predictions in production apps)
- Real-time personalization (segments, recommendations)
- Close the loop (analytics drives action)
- Sync lakehouse to operational systems

## Parameters


```python
execution_date = "2025-01-16"
environment = "development"
database_name = "demo"
customer_analytics_table = "customer_analytics"
product_recommendations_table = "product_recommendations"
postgres_host = "infrastructure-postgres"
postgres_port = "5432"
postgres_db = "openlakes"
postgres_user = "admin"
postgres_password = "admin123"
num_customers = 500
num_recommendations = 2000
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
    .appName(f"Pattern-5.4-ReverseETL-{environment}") \
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

print("✅ Spark initialized for reverse ETL (includes PostgreSQL JDBC driver)")
```

## Step 2: Setup - Create Lakehouse Analytics Tables


```python
full_database = f"lakehouse.{database_name}"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_database}")

full_customer_analytics = f"{full_database}.{customer_analytics_table}"
full_product_recommendations = f"{full_database}.{product_recommendations_table}"

# Customer analytics (ML predictions and segments)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {full_customer_analytics} (
        customer_id STRING,
        customer_segment STRING,
        lifetime_value DOUBLE,
        churn_probability DOUBLE,
        churn_risk_level STRING,
        engagement_score DOUBLE,
        last_purchase_days BIGINT,
        total_purchases BIGINT,
        total_spent DOUBLE,
        predicted_next_purchase_days BIGINT,
        analysis_date DATE
    ) USING iceberg
    PARTITIONED BY (days(analysis_date))
""")

# Product recommendations (collaborative filtering results)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {full_product_recommendations} (
        customer_id STRING,
        product_id STRING,
        recommendation_score DOUBLE,
        recommendation_reason STRING,
        rank BIGINT,
        analysis_date DATE
    ) USING iceberg
    PARTITIONED BY (days(analysis_date))
""")

print(f"✅ Created lakehouse analytics tables")
```

## Step 3: Generate Customer Analytics Data (ML Predictions)


```python
# Simulate ML model output: customer segments, churn predictions, lifetime value
segments = ["high_value", "medium_value", "low_value", "at_risk", "churned"]
customer_analytics_data = []

for i in range(num_customers):
    customer_id = f"CUST{i+1:05d}"
    segment = random.choice(segments)
    
    # Segment influences other metrics
    if segment == "high_value":
        ltv = round(random.uniform(5000, 20000), 2)
        churn_prob = round(random.uniform(0.05, 0.20), 3)
        engagement = round(random.uniform(0.7, 1.0), 3)
        total_purchases = random.randint(20, 100)
        total_spent = round(random.uniform(3000, 15000), 2)
        last_purchase = random.randint(1, 30)
    elif segment == "medium_value":
        ltv = round(random.uniform(1000, 5000), 2)
        churn_prob = round(random.uniform(0.15, 0.35), 3)
        engagement = round(random.uniform(0.4, 0.7), 3)
        total_purchases = random.randint(5, 20)
        total_spent = round(random.uniform(500, 3000), 2)
        last_purchase = random.randint(10, 60)
    elif segment == "low_value":
        ltv = round(random.uniform(100, 1000), 2)
        churn_prob = round(random.uniform(0.30, 0.50), 3)
        engagement = round(random.uniform(0.2, 0.5), 3)
        total_purchases = random.randint(1, 5)
        total_spent = round(random.uniform(50, 500), 2)
        last_purchase = random.randint(30, 120)
    elif segment == "at_risk":
        ltv = round(random.uniform(500, 3000), 2)
        churn_prob = round(random.uniform(0.60, 0.85), 3)
        engagement = round(random.uniform(0.1, 0.3), 3)
        total_purchases = random.randint(3, 15)
        total_spent = round(random.uniform(200, 2000), 2)
        last_purchase = random.randint(60, 180)
    else:  # churned
        ltv = round(random.uniform(50, 500), 2)
        churn_prob = round(random.uniform(0.90, 0.99), 3)
        engagement = round(random.uniform(0.0, 0.1), 3)
        total_purchases = random.randint(1, 3)
        total_spent = round(random.uniform(20, 200), 2)
        last_purchase = random.randint(180, 365)
    
    # Risk level based on churn probability
    if churn_prob < 0.3:
        risk_level = "low"
    elif churn_prob < 0.6:
        risk_level = "medium"
    else:
        risk_level = "high"
    
    predicted_next = max(7, int(last_purchase * (1 - engagement) + random.randint(-5, 5)))
    analysis_date = datetime.now().date()
    
    customer_analytics_data.append((
        customer_id, segment, ltv, churn_prob, risk_level, engagement,
        last_purchase, total_purchases, total_spent, predicted_next, analysis_date
    ))

analytics_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_segment", StringType(), False),
    StructField("lifetime_value", DoubleType(), False),
    StructField("churn_probability", DoubleType(), False),
    StructField("churn_risk_level", StringType(), False),
    StructField("engagement_score", DoubleType(), False),
    StructField("last_purchase_days", LongType(), False),
    StructField("total_purchases", LongType(), False),
    StructField("total_spent", DoubleType(), False),
    StructField("predicted_next_purchase_days", LongType(), False),
    StructField("analysis_date", DateType(), False)
])

df_customer_analytics = spark.createDataFrame(customer_analytics_data, schema=analytics_schema)
df_customer_analytics.writeTo(full_customer_analytics).using("iceberg").overwritePartitions()

print(f"✅ Generated {num_customers} customer analytics records (ML predictions)")
df_customer_analytics.show(10, truncate=False)
```

## Step 4: Generate Product Recommendations Data


```python
# Simulate collaborative filtering recommendations
product_ids = [f"PROD{i:04d}" for i in range(1, 101)]
reasons = [
    "Frequently bought together",
    "Customers like you also bought",
    "Based on your browsing history",
    "Popular in your region",
    "Trending now",
    "Similar to your purchases"
]

recommendations_data = []
customer_ids = [f"CUST{i+1:05d}" for i in range(num_customers)]

for _ in range(num_recommendations):
    customer_id = random.choice(customer_ids)
    product_id = random.choice(product_ids)
    score = round(random.uniform(0.5, 1.0), 3)
    reason = random.choice(reasons)
    rank = random.randint(1, 10)
    analysis_date = datetime.now().date()
    
    recommendations_data.append((
        customer_id, product_id, score, reason, rank, analysis_date
    ))

recommendations_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("recommendation_score", DoubleType(), False),
    StructField("recommendation_reason", StringType(), False),
    StructField("rank", LongType(), False),
    StructField("analysis_date", DateType(), False)
])

df_recommendations = spark.createDataFrame(recommendations_data, schema=recommendations_schema)
df_recommendations.writeTo(full_product_recommendations).using("iceberg").overwritePartitions()

print(f"✅ Generated {num_recommendations} product recommendations")
df_recommendations.show(10, truncate=False)
```

## Step 5: Reverse ETL - Push Customer Segments to PostgreSQL

Push customer segments and scores to PostgreSQL for application use.


```python
# Read customer analytics from lakehouse
df_to_postgres = spark.sql(f"""
    SELECT 
        customer_id,
        customer_segment,
        CAST(lifetime_value AS DECIMAL(10,2)) as lifetime_value,
        CAST(churn_probability AS DECIMAL(5,3)) as churn_probability,
        churn_risk_level,
        CAST(engagement_score AS DECIMAL(5,3)) as engagement_score,
        last_purchase_days,
        predicted_next_purchase_days,
        CURRENT_TIMESTAMP as synced_at
    FROM {full_customer_analytics}
    WHERE analysis_date = CURRENT_DATE
""")

# PostgreSQL connection properties
postgres_url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"
postgres_properties = {
    "user": postgres_user,
    "password": postgres_password,
    "driver": "org.postgresql.Driver"
}

postgres_table = "customer_segments"

# Write to PostgreSQL (overwrite mode for simplicity)
df_to_postgres.write \
    .jdbc(url=postgres_url, table=postgres_table, mode="overwrite", properties=postgres_properties)

print(f"✅ Pushed {df_to_postgres.count()} customer segments to PostgreSQL table '{postgres_table}'")
print(f"   Application can now query: SELECT * FROM {postgres_table} WHERE customer_id = 'CUST00001'")
```

## Step 6: Reverse ETL - Push Product Recommendations to PostgreSQL


```python
# Get top 5 recommendations per customer
df_top_recommendations = spark.sql(f"""
    WITH ranked_recs AS (
        SELECT 
            customer_id,
            product_id,
            CAST(recommendation_score AS DECIMAL(5,3)) as recommendation_score,
            recommendation_reason,
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY recommendation_score DESC) as rn
        FROM {full_product_recommendations}
        WHERE analysis_date = CURRENT_DATE
    )
    SELECT 
        customer_id,
        product_id,
        recommendation_score,
        recommendation_reason,
        rn as rank,
        CURRENT_TIMESTAMP as synced_at
    FROM ranked_recs
    WHERE rn <= 5
""")

postgres_recs_table = "product_recommendations"

# Write to PostgreSQL
df_top_recommendations.write \
    .jdbc(url=postgres_url, table=postgres_recs_table, mode="overwrite", properties=postgres_properties)

print(f"✅ Pushed {df_top_recommendations.count()} product recommendations to PostgreSQL table '{postgres_recs_table}'")
print(f"   Application can show: SELECT * FROM {postgres_recs_table} WHERE customer_id = 'CUST00001' ORDER BY rank")
```

## Step 7: Verify Data in PostgreSQL


```python
# Read back from PostgreSQL to verify
df_verify_segments = spark.read \
    .jdbc(url=postgres_url, table=postgres_table, properties=postgres_properties)

print(f"📊 Verification: Customer Segments in PostgreSQL")
df_verify_segments.orderBy(desc("lifetime_value")).show(10, truncate=False)

df_verify_recommendations = spark.read \
    .jdbc(url=postgres_url, table=postgres_recs_table, properties=postgres_properties)

print(f"\n📊 Verification: Product Recommendations in PostgreSQL")
df_verify_recommendations.filter(col("customer_id") == "CUST00001").orderBy("rank").show(truncate=False)
```

## Step 8: Simulate External API Push (Marketing Automation)

Demonstrate how to push high-risk churn customers to external marketing tools.


```python
# Identify high-risk churn customers for retention campaign
df_churn_customers = spark.sql(f"""
    SELECT 
        customer_id,
        customer_segment,
        churn_probability,
        churn_risk_level,
        lifetime_value,
        last_purchase_days
    FROM {full_customer_analytics}
    WHERE churn_risk_level = 'high'
      AND customer_segment IN ('high_value', 'medium_value', 'at_risk')
      AND analysis_date = CURRENT_DATE
    ORDER BY lifetime_value DESC
""")

print("📧 High-Risk Churn Customers for Marketing Campaign:")
df_churn_customers.show(10, truncate=False)

# In production, this would push to external APIs
churn_count = df_churn_customers.count()
print(f"\n💡 In production, {churn_count} customers would be pushed to:")
print("   - Salesforce: Create retention campaign tasks")
print("   - HubSpot: Add to 'Churn Risk' list for email campaign")
print("   - Braze: Trigger personalized retention offers")
print("   - Segment: Send to ad platforms for remarketing")

# Example API payload (not actually sent in this demo)
example_payload = df_churn_customers.limit(1).toJSON().collect()[0]
print(f"\n📤 Example API Payload:")
print(example_payload)
```

## Use Case Examples


```python
print("""
💡 Reverse ETL Use Cases:

## 1. Application Personalization (PostgreSQL)

**Scenario**: E-commerce website shows personalized homepage

**Data Flow**:
1. ML model in lakehouse predicts customer segments
2. Reverse ETL pushes segments to PostgreSQL
3. Web app reads from PostgreSQL:

```sql
-- App queries PostgreSQL
SELECT customer_segment, lifetime_value, churn_risk_level
FROM customer_segments
WHERE customer_id = :logged_in_user_id;
```

4. App shows personalized content:
   - high_value → Premium products, loyalty rewards
   - at_risk → Discount offers, "We miss you" messages
   - low_value → Popular items, free shipping offers

**Benefits**:
- Real-time personalization (millisecond latency)
- No ML infrastructure in app (just read from DB)
- ML team owns predictions, app team owns UI

---

## 2. Product Recommendations (PostgreSQL)

**Scenario**: Show "You might also like" section

**Data Flow**:
1. Collaborative filtering in lakehouse (Spark MLlib ALS)
2. Reverse ETL pushes top 5 recommendations per user
3. App reads from PostgreSQL:

```sql
-- App queries PostgreSQL
SELECT product_id, recommendation_score, recommendation_reason
FROM product_recommendations
WHERE customer_id = :logged_in_user_id
ORDER BY rank
LIMIT 5;
```

4. App displays product cards with recommendations

**Benefits**:
- Pre-computed recommendations (fast response)
- Daily/hourly updates (fresh without real-time compute)
- Explainable ("Customers like you also bought")

---

## 3. Marketing Automation (External APIs)

**Scenario**: Automated retention campaign for churn-risk customers

**Data Flow**:
1. Churn prediction model in lakehouse
2. Reverse ETL identifies high-risk customers
3. Push to marketing tools:

```python
# Pseudo-code for API integration
import requests

# HubSpot API
for customer in high_risk_customers:
    requests.post('https://api.hubspot.com/contacts/v1/lists/churn-risk/add', json={
        'email': customer.email,
        'properties': {
            'churn_probability': customer.churn_probability,
            'lifetime_value': customer.lifetime_value
        }
    })

# Braze (mobile push notifications)
requests.post('https://rest.iad-01.braze.com/campaigns/trigger/send', json={
    'campaign_id': 'retention_offer',
    'recipients': [{'external_user_id': c.customer_id} for c in high_risk_customers]
})
```

4. Marketing team sees enriched customer lists
5. Automated emails/push notifications sent

**Benefits**:
- Proactive retention (act before churn)
- Targeted campaigns (high LTV customers only)
- Closed loop (analytics → action → measure)

---

## 4. Sales CRM Enrichment (Salesforce)

**Scenario**: Sales team sees customer health scores in CRM

**Data Flow**:
1. Customer health score model in lakehouse
2. Reverse ETL pushes scores to Salesforce via API
3. Sales reps see in CRM dashboard:
   - Account health: Green/Yellow/Red
   - Expansion opportunity score
   - Churn risk flag

**Benefits**:
- Sales team acts on data-driven insights
- No manual reporting (auto-updated daily)
- Prioritize outreach (focus on high-value at-risk)

---

## 5. Operational Dashboards (PostgreSQL)

**Scenario**: Customer support dashboard shows real-time segments

**Data Flow**:
1. Analytics in lakehouse (daily batch)
2. Reverse ETL to PostgreSQL
3. Support dashboard reads:

```sql
-- Support tool query
SELECT c.name, c.email, s.customer_segment, s.lifetime_value, s.last_purchase_days
FROM customers c
JOIN customer_segments s ON c.customer_id = s.customer_id
WHERE c.customer_id = :search_customer_id;
```

4. Support agent sees:
   - "VIP Customer - Lifetime Value: $15,000"
   - "Last purchase: 90 days ago (at-risk)"

**Benefits**:
- Support provides personalized service
- Prioritize VIP customer issues
- Proactive outreach (at-risk customers)

---

## Technology Patterns:

| Target | Method | Use Case | Latency |
|--------|--------|----------|----------|
| **PostgreSQL** | JDBC write | App personalization | Sub-second |
| **REST APIs** | HTTP POST | Marketing tools | Seconds |
| **Salesforce** | Bulk API | CRM enrichment | Minutes |
| **S3 + Lambda** | Event-driven | Trigger workflows | Seconds |
| **Kafka** | Stream write | Real-time sync | Milliseconds |

## Scheduling (Airflow):

- **Daily**: Customer segments, product recommendations
- **Hourly**: High-frequency updates (active campaigns)
- **Real-time**: Event-triggered (new purchase → update segment)

## Data Quality:

- Validate before push (no nulls in customer_id)
- Idempotent writes (same input → same output)
- Monitor sync success (alert on failures)
- Track sync lag (lakehouse → operational DB time)
""")
```

## Validation


```python
if enable_validation:
    # Validate lakehouse data
    analytics_count = spark.sql(f"SELECT COUNT(*) FROM {full_customer_analytics}").collect()[0][0]
    recs_count = spark.sql(f"SELECT COUNT(*) FROM {full_product_recommendations}").collect()[0][0]
    
    assert analytics_count > 0, "Should have customer analytics data"
    print(f"✅ Customer analytics in lakehouse: {analytics_count}")
    
    assert recs_count > 0, "Should have recommendations"
    print(f"✅ Product recommendations in lakehouse: {recs_count}")
    
    # Validate PostgreSQL data
    postgres_segments_count = df_verify_segments.count()
    postgres_recs_count = df_verify_recommendations.count()
    
    assert postgres_segments_count > 0, "Should have segments in PostgreSQL"
    print(f"✅ Customer segments in PostgreSQL: {postgres_segments_count}")
    
    assert postgres_recs_count > 0, "Should have recommendations in PostgreSQL"
    print(f"✅ Product recommendations in PostgreSQL: {postgres_recs_count}")
    
    # Validate churn campaign list
    assert df_churn_customers.count() > 0, "Should have churn-risk customers"
    print(f"✅ Churn-risk customers for campaign: {df_churn_customers.count()}")
    
    test_passed = True
    print("\n✅ All validations passed!")
else:
    test_passed = True
```

## Summary

### ✅ Pattern 5.4: Reverse ETL Complete!

Demonstrated:
1. **Customer Segmentation**: ML predictions from lakehouse → PostgreSQL for app use
2. **Product Recommendations**: Collaborative filtering results → PostgreSQL for real-time serving
3. **Churn Prevention**: High-risk customers → Marketing automation tools
4. **Operational Integration**: Analytics results in production databases

**Reverse ETL Benefits**:
- 🔄 **Close the Loop**: Analytics drives action (predictions → production apps)
- ⚡ **Real-Time Personalization**: Pre-computed results, sub-second latency
- 🎯 **Targeted Campaigns**: Push segments to marketing tools automatically
- 🔌 **Decouple Systems**: ML team owns predictions, app team owns UI
- 📊 **Operational Analytics**: Dashboards, CRM enrichment, support tools
- 🚀 **No ML in App**: Apps just read from DB (no model serving)

**Technology Stack**:

| Layer | Technology | Purpose |
|-------|-----------|----------|
| **Analytics** | Iceberg Lakehouse | ML predictions, aggregations |
| **Compute** | Spark | Read lakehouse, write to operational DBs |
| **Target DB** | PostgreSQL | App database (segments, recommendations) |
| **APIs** | REST/GraphQL | Marketing tools (HubSpot, Salesforce) |
| **Orchestration** | Airflow | Schedule reverse ETL jobs |

**Data Flow Pattern**:

```
Analytics (Lakehouse) → Reverse ETL (Spark) → Operational Systems (PostgreSQL/APIs)
       ↑                                              ↓
   ML Models                              Production Applications
   Aggregations                           Marketing Automation
   Predictions                            CRM Systems
```

**Use Cases**:

1. **Personalization**:
   - Customer segments → PostgreSQL → Web/mobile app
   - Product recommendations → App homepage
   - Content recommendations → Streaming platform

2. **Marketing Automation**:
   - Churn predictions → HubSpot → Retention emails
   - Lead scoring → Salesforce → Sales prioritization
   - Customer 360 → Segment → Ad platforms

3. **Operational Dashboards**:
   - Customer health scores → Support dashboard
   - Fraud risk scores → Transaction monitoring
   - Inventory predictions → Warehouse management

4. **Business Intelligence**:
   - Daily metrics → PostgreSQL → Metabase/Tableau
   - KPIs → Operational DB → Executive dashboard

**Scheduling Patterns**:

- **Daily**: Customer segments, product recommendations (6 AM)
- **Hourly**: Active campaign updates, real-time dashboards
- **Event-Driven**: New purchase → update customer score
- **On-Demand**: Ad-hoc sync for specific campaigns

**Best Practices**:

1. **Idempotency**: Same input always produces same output
2. **Incremental Updates**: Only sync changed records (use timestamps)
3. **Data Quality**: Validate before push (no nulls, valid ranges)
4. **Monitoring**: Track sync success, lag, row counts
5. **Error Handling**: Retry failed syncs, alert on failures
6. **UPSERT**: Use merge/upsert for PostgreSQL (not overwrite)

**Production Airflow DAG**:

```python
with DAG('reverse_etl_customer_segments', schedule_interval='0 6 * * *') as dag:
    
    # Task 1: Read from lakehouse
    read_analytics = SparkSubmitOperator(...)
    
    # Task 2: Write to PostgreSQL
    sync_postgres = SparkSubmitOperator(...)
    
    # Task 3: Push to HubSpot (churn-risk customers)
    sync_hubspot = PythonOperator(python_callable=push_to_hubspot)
    
    # Task 4: Validate sync
    validate = SQLCheckOperator(
        sql="SELECT COUNT(*) FROM customer_segments WHERE synced_at > NOW() - INTERVAL '1 hour'",
        conn_id='postgres_app_db'
    )
    
    read_analytics >> sync_postgres >> validate
    read_analytics >> sync_hubspot
```

**Performance Optimization**:

- **Partitioning**: Only sync today's data (partition by date)
- **Batching**: Write in batches (10K rows per batch)
- **Parallel Writes**: Spark parallelism for multiple targets
- **Indexes**: Create indexes on customer_id in PostgreSQL

**When to Use Reverse ETL**:
- ✅ ML predictions for production apps
- ✅ Customer segments for personalization
- ✅ Analytics results in CRM/marketing tools
- ✅ Daily metrics in operational dashboards

**When NOT to Use Reverse ETL**:
- ❌ Real-time event streaming (use Kafka → app)
- ❌ Transactional data (OLTP should stay in source)
- ❌ Large datasets (GB+) to app DB (use cache layer)
- ❌ Frequent updates (second-level) (use streaming)

**Alternative Approaches**:

1. **Model Serving**: Deploy ML models as APIs (not reverse ETL)
2. **Streaming**: Kafka → App for real-time updates
3. **Cache Layer**: Redis for high-frequency reads
4. **Federated Query**: Query lakehouse directly from app (Trino)


```python
if enable_cleanup:
    spark.sql(f"DROP TABLE IF EXISTS {full_customer_analytics}")
    spark.sql(f"DROP TABLE IF EXISTS {full_product_recommendations}")
    
    # Note: PostgreSQL tables not dropped (would require separate JDBC connection)
    print("🧹 Cleanup completed (Iceberg tables dropped)")
    print("   Note: PostgreSQL tables not auto-dropped. To clean up:")
    print("   psql -h infrastructure-postgres -U admin -d openlakes")
    print("   DROP TABLE customer_segments, product_recommendations;")
else:
    print("⏭️  Cleanup skipped")
```
