```
from pyspark.sql.functions import col, sum as spark_sum, count, avg, when, expr, lit, window, current_timestamp, from_json

```

# Pattern 1.3: Multi-Source Consolidation

Join data from 3 sources into a unified Customer 360 view:

1. **Extract**: Load CRM, ERP, and Marketing data
2. **Transform**: Join all sources on customer_id
3. **Enrich**: Add derived fields (engagement_score, customer_tier)
4. **Load**: Write consolidated view

## Architecture

```
CRM + ERP + Marketing → Spark (join) → Iceberg (customer_360)
```

## Parameters


```
execution_date = "2025-01-16"
environment = "development"
database_name = "demo"
crm_table = "raw_crm"
erp_table = "raw_erp"
marketing_table = "raw_marketing"
consolidated_table = "customer_360"
enable_validation = True
enable_cleanup = False
```


```
import os

from pyspark.sql import SparkSession
# Set AWS environment variables for S3FileIO
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "admin123"

spark = SparkSession.builder \
    .appName(f"Pattern-1.3-MultiSource-{environment}") \
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

print("✅ Spark initialized")
```

## Load Source 1: CRM Data


```
full_database = f"lakehouse.{database_name}"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_database}")

crm_data = [
    (1, "Alice", "Johnson", "alice@tech.com", "USA", "Technology"),
    (2, "Bob", "Smith", "bob@retail.com", "Canada", "Retail"),
]

df_crm = spark.createDataFrame(crm_data, ["customer_id", "first_name", "last_name", "email", "country", "industry"])
full_crm = f"{full_database}.{crm_table}"
df_crm.writeTo(full_crm).using("iceberg").createOrReplace()
print(f"✅ CRM: {df_crm.count()} records")
```

## Load Source 2: ERP Data


```
erp_data = [
    (1, "Active", 125000.00, 45, 2777.78),
    (2, "Active", 48000.00, 18, 2666.67),
]

df_erp = spark.createDataFrame(erp_data, ["customer_id", "account_status", "lifetime_value", "total_orders", "avg_order_value"])
full_erp = f"{full_database}.{erp_table}"
df_erp.writeTo(full_erp).using("iceberg").createOrReplace()
print(f"✅ ERP: {df_erp.count()} records")
```

## Load Source 3: Marketing Data


```
marketing_data = [
    (1, True, 28, 12, "Webinar"),
    (2, True, 15, 5, "Trade Show"),
]

df_marketing = spark.createDataFrame(marketing_data, ["customer_id", "email_subscribed", "campaign_opens", "campaign_clicks", "lead_source"])
full_marketing = f"{full_database}.{marketing_table}"
df_marketing.writeTo(full_marketing).using("iceberg").createOrReplace()
print(f"✅ Marketing: {df_marketing.count()} records")
```

## Join All Sources into Customer 360


```
df_consolidated = df_crm \
    .join(df_erp, on="customer_id", how="inner") \
    .join(df_marketing, on="customer_id", how="inner")

# Add derived fields
df_customer_360 = df_consolidated.select(
    col("customer_id"),
    col("first_name"),
    col("last_name"),
    col("email"),
    col("country"),
    col("industry"),
    col("account_status"),
    col("lifetime_value"),
    col("total_orders"),
    col("campaign_opens"),
    col("campaign_clicks"),
    col("lead_source"),
    when(col("campaign_opens") > 0, (col("campaign_clicks") / col("campaign_opens") * 100).cast("int")).otherwise(0).alias("engagement_score"),
    when(col("lifetime_value") >= 100000, "Platinum").when(col("lifetime_value") >= 50000, "Gold").otherwise("Silver").alias("customer_tier")
)

print("📊 Customer 360 View:")
df_customer_360.show(truncate=False)
```

## Write Consolidated Table


```
full_consolidated = f"{full_database}.{consolidated_table}"
df_customer_360.writeTo(full_consolidated).using("iceberg").createOrReplace()

consolidated_count = df_customer_360.count()
print(f"✅ Written {consolidated_count} rows to Customer 360")
print(f"   Combined 3 sources into single unified view")
```

## Validation


```
if enable_validation:
    source_count = df_crm.count()
    assert consolidated_count == source_count, f"Data loss: {source_count} → {consolidated_count}"
    print(f"✅ No data loss ({consolidated_count} customers)")
    
    null_tiers = df_customer_360.filter(col("customer_tier").isNull()).count()
    assert null_tiers == 0
    print(f"✅ All customers have tier assignment")
    
    test_passed = True
    print("\n✅ All validations passed!")
else:
    test_passed = True
```

## Summary

### ✅ Pattern 1.3: Multi-Source Consolidation Complete!

Created unified Customer 360 view by:
1. Loading CRM, ERP, Marketing data
2. Joining on customer_id
3. Adding derived fields (engagement_score, customer_tier)

**Benefits**: Single source of truth for customer data


```
if enable_cleanup:
    spark.sql(f"DROP TABLE IF EXISTS {full_crm}")
    spark.sql(f"DROP TABLE IF EXISTS {full_erp}")
    spark.sql(f"DROP TABLE IF EXISTS {full_marketing}")
    spark.sql(f"DROP TABLE IF EXISTS {full_consolidated}")
    print("🧹 Cleanup completed")
else:
    print("⏭️  Cleanup skipped")
```
