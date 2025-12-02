# Pattern 6.3: Clickstream Analytics Pipeline

Build real-time clickstream analytics for web/mobile applications:

1. **Kafka**: User interaction event stream
2. **Spark Streaming**: Process and sessionize clickstream data
3. **Iceberg**: Historical clickstream storage
4. **Trino/Superset**: Analytics dashboards and reports

## Architecture

```
Web/Mobile Apps (user interactions) → Kafka
                                        ↓
                          Spark Streaming (sessionization)
                                        ↓
                          Iceberg (clickstream data lake)
                                        ↓
                          Trino + Superset (analytics)
```

**Clickstream Pattern**:
- **Real-Time**: Process user interactions as they happen
- **Sessionization**: Group events by user session
- **Behavioral Analytics**: User journey, funnels, engagement
- **Performance**: Handle high-volume event streams (millions/day)

**Use Cases**:
- E-commerce: Product views, cart actions, purchases
- Media: Content engagement, video playback, scrolling
- SaaS: Feature usage, user onboarding, retention
- Marketing: Campaign attribution, conversion funnels
- Product Analytics: A/B testing, feature adoption

**Events Tracked**:
- Page views and navigation
- Clicks and interactions (buttons, links)
- Form submissions and inputs
- Scrolling and engagement
- Video/media playback
- Search queries
- Add to cart, checkout, purchase

**Benefits**:
- Real-time user behavior insights
- Conversion funnel analysis
- User journey mapping
- A/B test analytics
- Cost-efficient (S3 storage)

## Parameters


```python
execution_date = "2025-01-16"
environment = "development"
database_name = "demo"
kafka_topic = "clickstream_events"
iceberg_table = "clickstream"
kafka_bootstrap_servers = "infrastructure-kafka.openlakes.svc.cluster.local:9092"
num_clickstream_events = 300
streaming_duration_seconds = 35
session_timeout_minutes = 30  # Session expires after 30min inactivity
enable_validation = True
enable_cleanup = False
```

## Setup: Initialize Spark with Iceberg


```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, lit, expr, unix_timestamp, window,
    count, countDistinct, min as spark_min, max as spark_max, avg, sum as spark_sum,
    lag, lead, datediff, when, concat_ws, collect_list, first, last
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, IntegerType, 
    BooleanType, DoubleType, MapType
)
from pyspark.sql.window import Window
from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime, timedelta
import uuid

import os

# Set AWS environment variables for S3FileIO
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "admin123"

spark = SparkSession.builder \
    .appName(f"Pattern-6.3-Clickstream-{environment}") \
    .config("spark.jars.packages", 
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.8.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.4") \
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

print("✅ Spark initialized for clickstream analytics")
```

## Setup: Create Clickstream Table


```python
full_database = f"lakehouse.{database_name}"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_database}")

full_table = f"{full_database}.{iceberg_table}"

# Create clickstream table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {full_table} (
        event_id STRING,
        event_timestamp TIMESTAMP,
        user_id STRING,
        session_id STRING,
        event_type STRING,
        page_url STRING,
        page_title STRING,
        referrer_url STRING,
        element_id STRING,
        element_text STRING,
        device_type STRING,
        browser STRING,
        os STRING,
        screen_width INT,
        screen_height INT,
        viewport_width INT,
        viewport_height INT,
        scroll_depth INT,
        time_on_page INT,
        utm_source STRING,
        utm_medium STRING,
        utm_campaign STRING,
        ip_address STRING,
        country STRING,
        city STRING,
        ingestion_timestamp TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (days(event_timestamp), event_type)
    TBLPROPERTIES (
        'write.metadata.delete-after-commit.enabled' = 'true',
        'write.metadata.previous-versions-max' = '100',
        'format-version' = '2',
        'comment' = 'Clickstream analytics for web/mobile applications'
    )
""")

print(f"✅ Clickstream table created: {full_table}")
print("   Partitioned by: event_timestamp (days), event_type")
```

## Kafka Producer: Generate Clickstream Events

Simulate realistic user journeys:
- Page views and navigation
- Clicks and interactions
- Form submissions
- Scrolling and engagement
- E-commerce actions (cart, checkout, purchase)


```python
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"📊 Producing {num_clickstream_events} clickstream events to Kafka...")

# Realistic clickstream data
users = [f"user_{i:04d}" for i in range(1, 21)]  # 20 users

# E-commerce site structure
pages = [
    {"url": "/", "title": "Home", "typical_time": 15},
    {"url": "/products", "title": "Products", "typical_time": 30},
    {"url": "/products/electronics", "title": "Electronics", "typical_time": 45},
    {"url": "/products/clothing", "title": "Clothing", "typical_time": 60},
    {"url": "/product/laptop-pro-15", "title": "Laptop Pro 15", "typical_time": 90},
    {"url": "/product/smartphone-x", "title": "Smartphone X", "typical_time": 75},
    {"url": "/cart", "title": "Shopping Cart", "typical_time": 30},
    {"url": "/checkout", "title": "Checkout", "typical_time": 60},
    {"url": "/checkout/payment", "title": "Payment", "typical_time": 45},
    {"url": "/order/confirmation", "title": "Order Confirmation", "typical_time": 20},
    {"url": "/search", "title": "Search Results", "typical_time": 25},
    {"url": "/account", "title": "My Account", "typical_time": 30},
]

event_types = [
    "page_view", "click", "scroll", "form_submit", "search",
    "add_to_cart", "remove_from_cart", "checkout_start", "purchase"
]

devices = [
    {"type": "desktop", "browser": "Chrome", "os": "Windows", "screen": (1920, 1080)},
    {"type": "desktop", "browser": "Firefox", "os": "Windows", "screen": (1920, 1080)},
    {"type": "desktop", "browser": "Safari", "os": "MacOS", "screen": (2560, 1440)},
    {"type": "mobile", "browser": "Chrome Mobile", "os": "Android", "screen": (412, 915)},
    {"type": "mobile", "browser": "Safari Mobile", "os": "iOS", "screen": (390, 844)},
    {"type": "tablet", "browser": "Safari", "os": "iOS", "screen": (1024, 1366)},
]

utm_campaigns = [
    {"source": "google", "medium": "cpc", "campaign": "winter_sale_2025"},
    {"source": "facebook", "medium": "social", "campaign": "product_launch"},
    {"source": "email", "medium": "newsletter", "campaign": "weekly_deals"},
    {"source": "organic", "medium": "organic", "campaign": None},
]

locations = [
    {"ip": "203.0.113.10", "country": "US", "city": "New York"},
    {"ip": "203.0.113.11", "country": "US", "city": "San Francisco"},
    {"ip": "203.0.113.12", "country": "UK", "city": "London"},
    {"ip": "203.0.113.13", "country": "DE", "city": "Berlin"},
    {"ip": "203.0.113.14", "country": "JP", "city": "Tokyo"},
]

# Generate realistic user sessions
base_time = datetime.utcnow() - timedelta(hours=3)
sessions = {}
current_pages = {}

for i in range(num_clickstream_events):
    user_id = random.choice(users)
    
    # Create new session or continue existing
    if user_id not in sessions or random.random() < 0.1:  # 10% chance of new session
        sessions[user_id] = str(uuid.uuid4())
        current_pages[user_id] = pages[0]  # Start at home page
        device = random.choice(devices)
        utm = random.choice(utm_campaigns)
        location = random.choice(locations)
        referrer = "https://google.com" if utm["source"] == "google" else ""
    
    session_id = sessions[user_id]
    current_page = current_pages.get(user_id, pages[0])
    
    # Determine event type based on user journey
    if "/product/" in current_page["url"]:
        # On product page: view, scroll, add to cart
        event_type = random.choice(["page_view", "scroll", "click", "add_to_cart"])
    elif current_page["url"] == "/cart":
        # In cart: view, remove items, checkout
        event_type = random.choice(["page_view", "click", "remove_from_cart", "checkout_start"])
    elif "/checkout" in current_page["url"]:
        # Checkout: form submit, purchase
        event_type = random.choice(["page_view", "form_submit", "purchase"])
    else:
        # Other pages: view, click, search
        event_type = random.choice(["page_view", "click", "scroll", "search"])
    
    # Event details
    if event_type == "click":
        element_id = random.choice(["nav-products", "btn-view-details", "link-category", "btn-add-cart"])
        element_text = random.choice(["View Products", "See Details", "Electronics", "Add to Cart"])
    elif event_type == "search":
        element_id = "search-input"
        element_text = random.choice(["laptop", "smartphone", "headphones", "camera"])
    else:
        element_id = None
        element_text = None
    
    # Generate event
    event_time = base_time + timedelta(seconds=i * 20 + random.randint(0, 10))
    
    viewport_ratio = random.uniform(0.7, 1.0)
    screen_w, screen_h = device["screen"]
    
    event = {
        "event_id": str(uuid.uuid4()),
        "event_timestamp": event_time.isoformat(),
        "user_id": user_id,
        "session_id": session_id,
        "event_type": event_type,
        "page_url": current_page["url"],
        "page_title": current_page["title"],
        "referrer_url": referrer if i == 0 else current_pages.get(user_id, pages[0])["url"],
        "element_id": element_id,
        "element_text": element_text,
        "device_type": device["type"],
        "browser": device["browser"],
        "os": device["os"],
        "screen_width": screen_w,
        "screen_height": screen_h,
        "viewport_width": int(screen_w * viewport_ratio),
        "viewport_height": int(screen_h * viewport_ratio),
        "scroll_depth": random.randint(0, 100) if event_type == "scroll" else 0,
        "time_on_page": current_page["typical_time"] + random.randint(-10, 10),
        "utm_source": utm["source"],
        "utm_medium": utm["medium"],
        "utm_campaign": utm["campaign"],
        "ip_address": location["ip"],
        "country": location["country"],
        "city": location["city"]
    }
    
    producer.send(kafka_topic, value=event)
    
    # Navigate to next page (simulate user journey)
    if event_type == "click" and random.random() > 0.3:
        current_pages[user_id] = random.choice(pages)
    
    if (i + 1) % 50 == 0:
        print(f"  Produced {i+1}/{num_clickstream_events} events...")
        time.sleep(0.1)

producer.flush()
print(f"✅ Produced {num_clickstream_events} clickstream events to Kafka")
print(f"   Users: {len(users)}, Sessions: {len(sessions)}")
```

## Streaming: Process Clickstream to Iceberg

Process clickstream events and write to Iceberg.


```python
# Define clickstream event schema
clickstream_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("page_title", StringType(), True),
    StructField("referrer_url", StringType(), True),
    StructField("element_id", StringType(), True),
    StructField("element_text", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("browser", StringType(), True),
    StructField("os", StringType(), True),
    StructField("screen_width", IntegerType(), True),
    StructField("screen_height", IntegerType(), True),
    StructField("viewport_width", IntegerType(), True),
    StructField("viewport_height", IntegerType(), True),
    StructField("scroll_depth", IntegerType(), True),
    StructField("time_on_page", IntegerType(), True),
    StructField("utm_source", StringType(), True),
    StructField("utm_medium", StringType(), True),
    StructField("utm_campaign", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True)
])

# Read from Kafka
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse clickstream events
df_parsed = df_stream.select(
    from_json(col("value").cast("string"), clickstream_schema).alias("data")
).select(
    col("data.event_id").alias("event_id"),
    col("data.event_timestamp").cast(TimestampType()).alias("event_timestamp"),
    col("data.user_id").alias("user_id"),
    col("data.session_id").alias("session_id"),
    col("data.event_type").alias("event_type"),
    col("data.page_url").alias("page_url"),
    col("data.page_title").alias("page_title"),
    col("data.referrer_url").alias("referrer_url"),
    col("data.element_id").alias("element_id"),
    col("data.element_text").alias("element_text"),
    col("data.device_type").alias("device_type"),
    col("data.browser").alias("browser"),
    col("data.os").alias("os"),
    col("data.screen_width").alias("screen_width"),
    col("data.screen_height").alias("screen_height"),
    col("data.viewport_width").alias("viewport_width"),
    col("data.viewport_height").alias("viewport_height"),
    col("data.scroll_depth").alias("scroll_depth"),
    col("data.time_on_page").alias("time_on_page"),
    col("data.utm_source").alias("utm_source"),
    col("data.utm_medium").alias("utm_medium"),
    col("data.utm_campaign").alias("utm_campaign"),
    col("data.ip_address").alias("ip_address"),
    col("data.country").alias("country"),
    col("data.city").alias("city"),
    current_timestamp().alias("ingestion_timestamp")
)

# Write to Iceberg
def write_clickstream(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    
    print(f"\n  Batch {batch_id}: Processing {batch_df.count()} clickstream events")
    
    # Write to Iceberg
    batch_df.writeTo(full_table).using("iceberg").append()
    
    # Log statistics
    event_counts = batch_df.groupBy("event_type").count().collect()
    device_counts = batch_df.groupBy("device_type").count().collect()
    
    print(f"  ✅ Written to clickstream table")
    print(f"     Events: {', '.join([f'{r.event_type}({r.count})' for r in event_counts[:5]])}")
    print(f"     Devices: {', '.join([f'{r.device_type}({r.count})' for r in device_counts])}")

# Start streaming query
query = df_parsed.writeStream \
    .foreachBatch(write_clickstream) \
    .option("checkpointLocation", f"/tmp/checkpoint_clickstream_{iceberg_table}") \
    .trigger(processingTime='5 seconds') \
    .start()

print(f"\n✅ Clickstream processing started")
print(f"   → Target: {full_table}")
print(f"⏳ Processing for {streaming_duration_seconds} seconds...\n")

time.sleep(streaming_duration_seconds)

query.stop()
print("\n✅ Streaming query stopped")
```

## Analytics Query 1: User Engagement Metrics

Calculate key engagement metrics: page views, sessions, bounce rate.


```python
# Overall engagement metrics
df_engagement = spark.sql(f"""
    SELECT
        COUNT(*) as total_events,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(DISTINCT session_id) as total_sessions,
        COUNT(DISTINCT CASE WHEN event_type = 'page_view' THEN event_id END) as page_views,
        COUNT(DISTINCT CASE WHEN event_type = 'click' THEN event_id END) as clicks,
        COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN event_id END) as purchases,
        ROUND(AVG(time_on_page), 2) as avg_time_on_page_sec,
        ROUND(AVG(scroll_depth), 2) as avg_scroll_depth_pct
    FROM {full_table}
""")

print("📊 Overall User Engagement Metrics:")
df_engagement.show(truncate=False)

# Top pages by views
df_top_pages = spark.sql(f"""
    SELECT
        page_url,
        page_title,
        COUNT(*) as page_views,
        COUNT(DISTINCT user_id) as unique_visitors,
        ROUND(AVG(time_on_page), 2) as avg_time_on_page,
        ROUND(AVG(scroll_depth), 2) as avg_scroll_depth
    FROM {full_table}
    WHERE event_type = 'page_view'
    GROUP BY page_url, page_title
    ORDER BY page_views DESC
    LIMIT 10
""")

print("\n📊 Top Pages by Views:")
df_top_pages.show(truncate=False)
```

## Analytics Query 2: Session Analysis

Analyze user sessions: duration, pages per session, bounce rate.


```python
# Session metrics
df_sessions = spark.sql(f"""
    SELECT
        session_id,
        user_id,
        MIN(event_timestamp) as session_start,
        MAX(event_timestamp) as session_end,
        COUNT(*) as total_events,
        COUNT(DISTINCT page_url) as unique_pages,
        SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) as page_views,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as converted,
        ROUND((UNIX_TIMESTAMP(MAX(event_timestamp)) - UNIX_TIMESTAMP(MIN(event_timestamp))) / 60.0, 2) as session_duration_min
    FROM {full_table}
    GROUP BY session_id, user_id
""")

df_sessions.createOrReplaceTempView("sessions")

# Session statistics
df_session_stats = spark.sql("""
    SELECT
        COUNT(*) as total_sessions,
        ROUND(AVG(session_duration_min), 2) as avg_session_duration_min,
        ROUND(AVG(page_views), 2) as avg_pages_per_session,
        ROUND(AVG(unique_pages), 2) as avg_unique_pages,
        SUM(CASE WHEN page_views = 1 THEN 1 ELSE 0 END) as bounced_sessions,
        ROUND(100.0 * SUM(CASE WHEN page_views = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) as bounce_rate_pct,
        SUM(converted) as conversions,
        ROUND(100.0 * SUM(converted) / COUNT(*), 2) as conversion_rate_pct
    FROM sessions
""")

print("📊 Session Analysis:")
df_session_stats.show(truncate=False)

# Sample sessions
print("\n📊 Sample User Sessions:")
df_sessions.orderBy(col("session_duration_min").desc()).show(10, truncate=False)
```

## Analytics Query 3: Conversion Funnel Analysis

E-commerce funnel: Home → Products → Product Detail → Cart → Checkout → Purchase


```python
# Conversion funnel
df_funnel = spark.sql(f"""
    WITH funnel_steps AS (
        SELECT
            session_id,
            MAX(CASE WHEN page_url = '/' THEN 1 ELSE 0 END) as visited_home,
            MAX(CASE WHEN page_url LIKE '/products%' THEN 1 ELSE 0 END) as visited_products,
            MAX(CASE WHEN page_url LIKE '/product/%' THEN 1 ELSE 0 END) as visited_product_detail,
            MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) as added_to_cart,
            MAX(CASE WHEN page_url = '/cart' THEN 1 ELSE 0 END) as visited_cart,
            MAX(CASE WHEN page_url LIKE '/checkout%' THEN 1 ELSE 0 END) as started_checkout,
            MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as completed_purchase
        FROM {full_table}
        GROUP BY session_id
    )
    SELECT
        'Home' as funnel_step,
        SUM(visited_home) as sessions,
        100.0 as conversion_rate
    FROM funnel_steps
    WHERE visited_home = 1
    
    UNION ALL
    
    SELECT
        'Products',
        SUM(visited_products),
        ROUND(100.0 * SUM(visited_products) / SUM(visited_home), 2)
    FROM funnel_steps
    WHERE visited_home = 1
    
    UNION ALL
    
    SELECT
        'Product Detail',
        SUM(visited_product_detail),
        ROUND(100.0 * SUM(visited_product_detail) / SUM(visited_home), 2)
    FROM funnel_steps
    WHERE visited_home = 1
    
    UNION ALL
    
    SELECT
        'Add to Cart',
        SUM(added_to_cart),
        ROUND(100.0 * SUM(added_to_cart) / SUM(visited_home), 2)
    FROM funnel_steps
    WHERE visited_home = 1
    
    UNION ALL
    
    SELECT
        'Cart',
        SUM(visited_cart),
        ROUND(100.0 * SUM(visited_cart) / SUM(visited_home), 2)
    FROM funnel_steps
    WHERE visited_home = 1
    
    UNION ALL
    
    SELECT
        'Checkout',
        SUM(started_checkout),
        ROUND(100.0 * SUM(started_checkout) / SUM(visited_home), 2)
    FROM funnel_steps
    WHERE visited_home = 1
    
    UNION ALL
    
    SELECT
        'Purchase',
        SUM(completed_purchase),
        ROUND(100.0 * SUM(completed_purchase) / SUM(visited_home), 2)
    FROM funnel_steps
    WHERE visited_home = 1
""")

print("📊 E-Commerce Conversion Funnel:")
df_funnel.show(truncate=False)
```

## Analytics Query 4: Device & Browser Analytics

Analyze user behavior by device type and browser.


```python
# Device analysis
df_devices = spark.sql(f"""
    SELECT
        device_type,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(DISTINCT session_id) as sessions,
        COUNT(*) as total_events,
        ROUND(AVG(time_on_page), 2) as avg_time_on_page,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases,
        ROUND(100.0 * SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) / COUNT(DISTINCT session_id), 2) as conversion_rate
    FROM {full_table}
    GROUP BY device_type
    ORDER BY unique_users DESC
""")

print("📊 Device Performance:")
df_devices.show(truncate=False)

# Browser analysis
df_browsers = spark.sql(f"""
    SELECT
        browser,
        os,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(DISTINCT session_id) as sessions,
        ROUND(AVG(time_on_page), 2) as avg_time_on_page
    FROM {full_table}
    GROUP BY browser, os
    ORDER BY unique_users DESC
""")

print("\n📊 Browser & OS Distribution:")
df_browsers.show(truncate=False)
```

## Analytics Query 5: Campaign Attribution

Track marketing campaign performance and ROI.


```python
# Campaign performance
df_campaigns = spark.sql(f"""
    SELECT
        utm_source,
        utm_medium,
        utm_campaign,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(DISTINCT session_id) as sessions,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as conversions,
        ROUND(100.0 * SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) / COUNT(DISTINCT session_id), 2) as conversion_rate_pct,
        ROUND(AVG(time_on_page), 2) as avg_time_on_page
    FROM {full_table}
    WHERE utm_source IS NOT NULL
    GROUP BY utm_source, utm_medium, utm_campaign
    ORDER BY conversions DESC
""")

print("📊 Marketing Campaign Performance:")
df_campaigns.show(truncate=False)

# Traffic source summary
df_traffic = spark.sql(f"""
    SELECT
        utm_source,
        COUNT(DISTINCT session_id) as sessions,
        ROUND(100.0 * COUNT(DISTINCT session_id) / SUM(COUNT(DISTINCT session_id)) OVER(), 2) as pct_of_traffic
    FROM {full_table}
    WHERE utm_source IS NOT NULL
    GROUP BY utm_source
    ORDER BY sessions DESC
""")

print("\n📊 Traffic Source Distribution:")
df_traffic.show(truncate=False)
```

## Analytics Query 6: Geographic Analysis

Analyze user behavior by location.


```python
# Geographic performance
df_geography = spark.sql(f"""
    SELECT
        country,
        city,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(DISTINCT session_id) as sessions,
        COUNT(*) as total_events,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases,
        ROUND(100.0 * SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) / COUNT(DISTINCT session_id), 2) as conversion_rate
    FROM {full_table}
    GROUP BY country, city
    ORDER BY unique_users DESC
""")

print("📊 Geographic Performance:")
df_geography.show(truncate=False)
```

## Production Deployment Example


```python
print("""
💡 Production Deployment: Clickstream Analytics Pipeline

## Kubernetes SparkApplication

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: clickstream-pipeline
  namespace: openlakes
spec:
  type: Python
  mode: cluster
  image: ghcr.io/openlakes/openlakes-core/spark-openmetadata:1.0.0
  mainApplicationFile: local:///opt/spark/apps/clickstream.py
  
  arguments:
    - --kafka-topic=clickstream_events
    - --iceberg-table=lakehouse.analytics.clickstream
    - --checkpoint-location=s3a://openlakes/checkpoints/clickstream/
  
  sparkConf:
    "spark.sql.catalog.lakehouse": "org.apache.iceberg.spark.SparkCatalog"
    "spark.sql.catalog.lakehouse.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog"
    "spark.streaming.stopGracefullyOnShutdown": "true"
  
  driver:
    cores: 2
    memory: "4g"
  
  executor:
    cores: 4
    instances: 5
    memory: "8g"
  
  restartPolicy:
    type: Always
```

## JavaScript Event Tracking (Frontend)

```javascript
// analytics.js - Client-side event tracking

class ClickstreamTracker {
  constructor(kafkaEndpoint) {
    this.endpoint = kafkaEndpoint;
    this.sessionId = this.getOrCreateSession();
    this.userId = this.getUserId();
  }
  
  // Track page view
  trackPageView() {
    this.sendEvent({
      event_type: 'page_view',
      page_url: window.location.pathname,
      page_title: document.title,
      referrer_url: document.referrer
    });
  }
  
  // Track click
  trackClick(element) {
    this.sendEvent({
      event_type: 'click',
      element_id: element.id,
      element_text: element.textContent,
      page_url: window.location.pathname
    });
  }
  
  // Track scroll depth
  trackScroll() {
    const scrollDepth = Math.round(
      (window.scrollY / (document.body.scrollHeight - window.innerHeight)) * 100
    );
    
    this.sendEvent({
      event_type: 'scroll',
      scroll_depth: scrollDepth,
      page_url: window.location.pathname
    });
  }
  
  // Send event to Kafka via HTTP proxy
  sendEvent(eventData) {
    const event = {
      event_id: this.generateUUID(),
      event_timestamp: new Date().toISOString(),
      user_id: this.userId,
      session_id: this.sessionId,
      device_type: this.getDeviceType(),
      browser: this.getBrowser(),
      os: this.getOS(),
      screen_width: window.screen.width,
      screen_height: window.screen.height,
      viewport_width: window.innerWidth,
      viewport_height: window.innerHeight,
      utm_source: this.getUTMParam('utm_source'),
      utm_medium: this.getUTMParam('utm_medium'),
      utm_campaign: this.getUTMParam('utm_campaign'),
      ...eventData
    };
    
    // Send via beacon (survives page unload)
    navigator.sendBeacon(this.endpoint, JSON.stringify(event));
  }
}

// Initialize tracker
const tracker = new ClickstreamTracker('/api/events');

// Track page views
tracker.trackPageView();

// Track clicks on all buttons
document.querySelectorAll('button, a').forEach(el => {
  el.addEventListener('click', () => tracker.trackClick(el));
});

// Track scroll depth every 5 seconds
setInterval(() => tracker.trackScroll(), 5000);
```

## Superset Dashboards (Real-Time Analytics)

```sql
-- Dashboard 1: Real-Time Traffic Overview
SELECT
    DATE_TRUNC('minute', event_timestamp) as time_bucket,
    COUNT(*) as events,
    COUNT(DISTINCT user_id) as active_users,
    COUNT(DISTINCT session_id) as active_sessions
FROM lakehouse.analytics.clickstream
WHERE event_timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR
GROUP BY DATE_TRUNC('minute', event_timestamp)
ORDER BY time_bucket DESC;

-- Dashboard 2: Conversion Funnel (Last 24h)
WITH funnel AS (
    SELECT
        session_id,
        MAX(CASE WHEN page_url = '/' THEN 1 ELSE 0 END) as home,
        MAX(CASE WHEN page_url LIKE '/product/%' THEN 1 ELSE 0 END) as product,
        MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) as cart,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchase
    FROM lakehouse.analytics.clickstream
    WHERE event_timestamp >= CURRENT_DATE - INTERVAL '1' DAY
    GROUP BY session_id
)
SELECT
    SUM(home) as home_visits,
    SUM(product) as product_views,
    SUM(cart) as cart_adds,
    SUM(purchase) as purchases
FROM funnel;

-- Dashboard 3: Device Performance
SELECT
    device_type,
    COUNT(DISTINCT session_id) as sessions,
    AVG(time_on_page) as avg_time_on_page,
    100.0 * SUM(CASE WHEN event_type='purchase' THEN 1 ELSE 0 END) / 
            COUNT(DISTINCT session_id) as conversion_rate
FROM lakehouse.analytics.clickstream
WHERE event_timestamp >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY device_type;
```

## Airflow DAG (Aggregations)

```python
# Daily clickstream aggregations
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG('clickstream_daily_agg', schedule_interval='@daily') as dag:
    
    # Aggregate daily metrics
    daily_metrics = SparkSubmitOperator(
        task_id='compute_daily_metrics',
        application='/opt/airflow/jobs/clickstream_daily.py'
    )
    
    # User cohort analysis
    cohort_analysis = SparkSubmitOperator(
        task_id='user_cohort_analysis',
        application='/opt/airflow/jobs/cohort_analysis.py'
    )
    
    # Funnel analysis
    funnel_analysis = SparkSubmitOperator(
        task_id='conversion_funnel',
        application='/opt/airflow/jobs/funnel_analysis.py'
    )
    
    daily_metrics >> [cohort_analysis, funnel_analysis]
```

## Performance Optimization

```sql
-- Optimize Iceberg table for queries
ALTER TABLE lakehouse.analytics.clickstream
SET TBLPROPERTIES (
    'write.target-file-size-bytes' = '134217728',  -- 128MB files
    'write.distribution-mode' = 'hash',
    'write.metadata.compression-codec' = 'gzip'
);

-- Compact small files (run weekly)
CALL lakehouse.system.rewrite_data_files(
    table => 'analytics.clickstream',
    strategy => 'binpack'
);

-- Remove old snapshots (keep 30 days)
CALL lakehouse.system.expire_snapshots(
    table => 'analytics.clickstream',
    older_than => TIMESTAMP '2025-01-01 00:00:00',
    retain_last => 30
);
```
""")
```

## Validation


```python
if enable_validation:
    # Check clickstream events
    total_events = spark.sql(f"SELECT COUNT(*) FROM {full_table}").collect()[0][0]
    assert total_events > 0, "Should have clickstream events"
    print(f"✅ Total clickstream events: {total_events}")
    
    # Verify event types
    event_types_count = spark.sql(f"""
        SELECT COUNT(DISTINCT event_type) FROM {full_table}
    """).collect()[0][0]
    assert event_types_count > 0, "Should have multiple event types"
    print(f"✅ Unique event types: {event_types_count}")
    
    # Verify sessions
    sessions_count = spark.sql(f"""
        SELECT COUNT(DISTINCT session_id) FROM {full_table}
    """).collect()[0][0]
    assert sessions_count > 0, "Should have sessions"
    print(f"✅ Total sessions: {sessions_count}")
    
    # Verify users
    users_count = spark.sql(f"""
        SELECT COUNT(DISTINCT user_id) FROM {full_table}
    """).collect()[0][0]
    assert users_count > 0, "Should have users"
    print(f"✅ Unique users: {users_count}")
    
    # Verify partitioning
    partition_count = spark.sql(f"""
        SELECT COUNT(DISTINCT DATE(event_timestamp)) FROM {full_table}
    """).collect()[0][0]
    print(f"✅ Partitions created: {partition_count} day(s)")
    
    test_passed = True
    print("\n✅ All validations passed!")
else:
    test_passed = True
```

## Summary

### ✅ Pattern 6.3: Clickstream Analytics Pipeline Complete!

Demonstrated:
1. **Real-Time Processing**: Kafka → Spark → Iceberg clickstream pipeline
2. **Session Analysis**: User sessions, duration, bounce rate
3. **Conversion Funnels**: E-commerce funnel analysis
4. **Device Analytics**: Performance by device and browser
5. **Campaign Attribution**: Marketing campaign tracking and ROI
6. **Geographic Analysis**: User behavior by location

**Key Metrics Calculated**:

| Metric | Description | Use Case |
|--------|-------------|----------|
| **Page Views** | Total page visits | Traffic volume |
| **Unique Users** | Distinct visitors | Audience size |
| **Sessions** | User visits | Engagement frequency |
| **Bounce Rate** | Single-page sessions | Content relevance |
| **Avg Session Duration** | Time spent per session | Engagement quality |
| **Conversion Rate** | Purchase / Sessions | E-commerce efficiency |
| **Pages per Session** | Page views / Sessions | Content consumption |

**Clickstream Events**:
- 📄 **page_view**: Page navigation
- 🖱️ **click**: Button/link clicks
- 📜 **scroll**: Scrolling engagement
- 📝 **form_submit**: Form submissions
- 🔍 **search**: Search queries
- 🛒 **add_to_cart**: Product additions
- 🗑️ **remove_from_cart**: Product removals
- 💳 **checkout_start**: Checkout initiation
- ✅ **purchase**: Transaction completion

**Production Use Cases**:
- ✅ **E-commerce**: Product analytics, conversion optimization
- ✅ **Media**: Content engagement, video analytics
- ✅ **SaaS**: Feature usage, onboarding funnels
- ✅ **Marketing**: Campaign attribution, A/B testing
- ✅ **Product Analytics**: User behavior, retention analysis

**Real-Time Dashboards** (Superset/Trino):
1. **Traffic Overview**: Live visitor count, page views
2. **Conversion Funnel**: Step-by-step funnel visualization
3. **Device Breakdown**: Mobile vs Desktop performance
4. **Geographic Map**: Visitor locations and conversions
5. **Campaign Performance**: ROI by marketing channel

**Data Warehouse Integration**:
```sql
-- Analysts can query via Trino
SELECT
    DATE(event_timestamp) as date,
    COUNT(DISTINCT session_id) as sessions,
    100.0 * SUM(CASE WHEN event_type='purchase' THEN 1 ELSE 0 END) / 
            COUNT(DISTINCT session_id) as conversion_rate
FROM lakehouse.analytics.clickstream
WHERE event_timestamp >= CURRENT_DATE - INTERVAL '30' DAY
GROUP BY DATE(event_timestamp)
ORDER BY date;
```

**When to Use This Pattern**:
- ✅ Web/mobile analytics (track all user interactions)
- ✅ E-commerce (conversion funnel optimization)
- ✅ Product analytics (feature usage, A/B testing)
- ✅ Marketing attribution (campaign ROI tracking)
- ✅ User behavior analysis (journey mapping, retention)

## Cleanup


```python
if enable_cleanup:
    spark.sql(f"DROP TABLE IF EXISTS {full_table}")
    print("🧹 Cleanup completed")
else:
    print("⏭️  Cleanup skipped")
```
