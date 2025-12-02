# Pattern 7.4: Data Cataloging & Discovery

Comprehensive data catalog implementation using Iceberg metadata and OpenMetadata:

1. **Metadata Management**: Table/column descriptions, tags, owners
2. **Business Glossary**: Standardized terminology and definitions
3. **Data Classification**: PII, sensitive, confidential, public
4. **Ownership Tracking**: Data stewards and domain experts
5. **Search & Discovery**: Find datasets by business terms
6. **Documentation**: Automated catalog generation

## Architecture

```
Data Sources (Tables)
    ↓
Metadata Enrichment (descriptions, tags, classifications)
    ↓
Catalog Storage (Iceberg properties + OpenMetadata)
    ↓
Discovery Layer (Search API, UI, Documentation)
    ↓
Data Consumers (Analysts, Scientists, Engineers)
```

## Components

- **Iceberg Table Properties**: Store metadata in table/column properties
- **OpenMetadata**: Central catalog with UI and API
- **Business Glossary**: Standardized terminology
- **Data Classification**: Automated PII detection
- **Search Index**: Fast discovery by business terms

## Use Cases

- **Self-Service Analytics**: Analysts find data without asking engineers
- **Data Mesh**: Decentralized domain ownership with central discovery
- **Compliance**: Track PII and sensitive data locations
- **Onboarding**: New team members discover available datasets
- **Data Governance**: Enforce naming conventions and standards

## Parameters


```python
# Execution parameters
execution_date = "2025-01-17"
environment = "development"
database_name = "governance_demo"
catalog_table = "data_catalog"
glossary_table = "business_glossary"
num_sample_tables = 5
enable_validation = True
enable_cleanup = False
```

## Step 1: Initialize Spark with Iceberg + Nessie


```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, TimestampType, BooleanType
from pyspark.sql.functions import (
    col, lit, current_timestamp, array, struct, explode, when,
    concat, concat_ws, collect_list, size, lower, upper, trim
)
import random
from datetime import datetime
import json
import os

# Set AWS environment variables for S3FileIO
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "admin123"

spark = SparkSession.builder \
    .appName(f"Pattern-7.4-DataCataloging-{environment}") \
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
```

## Step 2: Create Business Glossary

Define standardized business terms and definitions.


```python
# Create database
full_database = f"lakehouse.{database_name}"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_database}")
print(f"✅ Database created: {full_database}")

# Define business glossary terms
glossary_terms = [
    # Customer Domain
    ("Customer ID", "customer_id", "Unique identifier for a customer", "Customer", 
     "Primary key in customer master data", "Data Engineering Team", ["identifier", "primary_key", "customer"]),
    
    ("Customer Lifetime Value", "clv", "Total revenue expected from a customer over their lifetime", "Customer",
     "Calculated as sum of all historical purchases plus predicted future value", "Analytics Team", ["metric", "revenue", "customer"]),
    
    ("Churn Rate", "churn_rate", "Percentage of customers who stopped using the service", "Customer",
     "Measured monthly as (customers lost / total customers at start) * 100", "Analytics Team", ["metric", "kpi", "customer"]),
    
    # Product Domain
    ("SKU", "sku", "Stock Keeping Unit - unique product identifier", "Product",
     "Format: CATEGORY-SUBCATEGORY-ITEM-VARIANT (e.g., ELEC-COMP-LAP-M1)", "Product Team", ["identifier", "product"]),
    
    ("Unit Cost", "unit_cost", "Cost to acquire/produce one unit of product", "Product",
     "Includes manufacturing, shipping, and overhead allocation", "Finance Team", ["cost", "product", "financial"]),
    
    # Financial Domain
    ("Revenue", "revenue", "Total income from sales before expenses", "Financial",
     "Calculated as quantity * price for all transactions", "Finance Team", ["financial", "metric", "income"]),
    
    ("Gross Margin", "gross_margin", "Percentage of revenue remaining after cost of goods sold", "Financial",
     "Formula: ((revenue - cost) / revenue) * 100", "Finance Team", ["financial", "metric", "profitability"]),
    
    # Transaction Domain
    ("Order ID", "order_id", "Unique identifier for a customer order", "Transaction",
     "Generated at checkout, format: ORD-YYYYMMDD-NNNNNN", "Sales Team", ["identifier", "transaction"]),
    
    ("Transaction Date", "transaction_date", "Date when order was placed", "Transaction",
     "Stored in UTC timezone, format: YYYY-MM-DD", "Data Engineering Team", ["date", "transaction", "temporal"]),
    
    # Data Quality
    ("PII", "pii", "Personally Identifiable Information", "Data Classification",
     "Data that can identify an individual (email, SSN, phone, address)", "Governance Team", ["classification", "privacy", "gdpr"]),
]

glossary_schema = StructType([
    StructField("term_name", StringType(), False),
    StructField("technical_name", StringType(), False),
    StructField("definition", StringType(), False),
    StructField("domain", StringType(), False),
    StructField("business_rules", StringType(), False),
    StructField("owner", StringType(), False),
    StructField("tags", ArrayType(StringType()), False)
])

df_glossary = spark.createDataFrame(glossary_terms, glossary_schema)

full_glossary_table = f"{full_database}.{glossary_table}"
df_glossary.writeTo(full_glossary_table).using("iceberg").createOrReplace()

spark.sql(f"""
    ALTER TABLE {full_glossary_table}
    SET TBLPROPERTIES (
        'description' = 'Business glossary: standardized terms and definitions',
        'owner' = 'governance-team',
        'domain' = 'Enterprise',
        'approval_status' = 'approved',
        'version' = '1.0',
        'last_reviewed' = '{execution_date}'
    )
""")

print(f"✅ Business glossary created: {full_glossary_table}")
print(f"   Total terms: {df_glossary.count()}")
print(f"\n📚 Sample Glossary Terms:")
df_glossary.select("term_name", "technical_name", "definition", "domain", "owner").show(10, truncate=False)
```

## Step 3: Create Sample Tables with Rich Metadata

Demonstrate comprehensive metadata tagging.


```python
# Table 1: Customers (PII data)
customers_table = f"{full_database}.customers_master"
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {customers_table} (
        customer_id INT COMMENT 'Unique identifier for customer (see glossary: Customer ID)',
        email STRING COMMENT 'Customer email address (PII)',
        phone STRING COMMENT 'Customer phone number (PII)',
        first_name STRING COMMENT 'Customer first name (PII)',
        last_name STRING COMMENT 'Customer last name (PII)',
        date_of_birth DATE COMMENT 'Customer birth date (PII)',
        address STRING COMMENT 'Customer mailing address (PII)',
        city STRING COMMENT 'City',
        state STRING COMMENT 'State code',
        postal_code STRING COMMENT 'ZIP/Postal code',
        country STRING COMMENT 'Country code (ISO 3166-1 alpha-2)',
        signup_date DATE COMMENT 'Date customer signed up',
        account_status STRING COMMENT 'active, suspended, closed',
        customer_segment STRING COMMENT 'VIP, Standard, New',
        lifetime_value DOUBLE COMMENT 'Customer lifetime value (see glossary: CLV)',
        created_at TIMESTAMP COMMENT 'Record creation timestamp',
        updated_at TIMESTAMP COMMENT 'Last update timestamp'
    ) USING iceberg
""")

spark.sql(f"""
    ALTER TABLE {customers_table}
    SET TBLPROPERTIES (
        'description' = 'Customer master data - single source of truth for customer information',
        'domain' = 'Customer',
        'owner' = 'data-engineering-team',
        'steward' = 'john.smith@company.com',
        'source_system' = 'Salesforce CRM',
        'refresh_frequency' = 'daily',
        'last_refresh' = '{execution_date}',
        'sla' = '9am ET daily',
        'data_classification' = 'PII',
        'retention_period' = '7 years',
        'compliance' = 'GDPR, CCPA',
        'quality_score' = '98.5',
        'tags' = 'customer, pii, master_data, gdpr',
        'glossary_terms' = 'Customer ID, Customer Lifetime Value, PII',
        'downstream_consumers' = 'Marketing Analytics, Customer 360 Dashboard, ML Churn Model',
        'contact_for_access' = 'data-access@company.com'
    )
""")

print(f"✅ Created {customers_table} with rich metadata")

# Table 2: Orders (transactional data)
orders_table = f"{full_database}.orders_fact"
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {orders_table} (
        order_id STRING COMMENT 'Unique order identifier (see glossary: Order ID)',
        customer_id INT COMMENT 'Foreign key to customers_master',
        order_date DATE COMMENT 'Date order was placed (see glossary: Transaction Date)',
        order_status STRING COMMENT 'pending, shipped, delivered, cancelled',
        total_amount DOUBLE COMMENT 'Total order value in USD',
        discount_amount DOUBLE COMMENT 'Discount applied in USD',
        tax_amount DOUBLE COMMENT 'Tax charged in USD',
        shipping_cost DOUBLE COMMENT 'Shipping cost in USD',
        payment_method STRING COMMENT 'credit_card, paypal, bank_transfer',
        shipping_address STRING COMMENT 'Delivery address (PII)',
        created_at TIMESTAMP COMMENT 'Order creation timestamp',
        updated_at TIMESTAMP COMMENT 'Last status update timestamp'
    ) USING iceberg
    PARTITIONED BY (days(order_date))
""")

spark.sql(f"""
    ALTER TABLE {orders_table}
    SET TBLPROPERTIES (
        'description' = 'Order transaction fact table - all customer orders',
        'domain' = 'Transaction',
        'owner' = 'sales-team',
        'steward' = 'jane.doe@company.com',
        'source_system' = 'Shopify E-commerce',
        'refresh_frequency' = 'real-time (CDC)',
        'last_refresh' = '{execution_date}',
        'sla' = '15 minutes latency',
        'data_classification' = 'Confidential',
        'retention_period' = '7 years',
        'compliance' = 'SOX, PCI-DSS',
        'quality_score' = '99.2',
        'tags' = 'transaction, orders, sales, fact_table',
        'glossary_terms' = 'Order ID, Transaction Date, Revenue',
        'upstream_sources' = 'Shopify API',
        'downstream_consumers' = 'Revenue Dashboard, Sales Forecasting, Financial Reporting',
        'partition_strategy' = 'daily (order_date)',
        'contact_for_access' = 'sales-analytics@company.com'
    )
""")

print(f"✅ Created {orders_table} with rich metadata")

# Table 3: Products (reference data)
products_table = f"{full_database}.products_catalog"
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {products_table} (
        product_id INT COMMENT 'Unique product identifier',
        sku STRING COMMENT 'Stock Keeping Unit (see glossary: SKU)',
        product_name STRING COMMENT 'Product display name',
        category STRING COMMENT 'Product category (Electronics, Clothing, etc.)',
        subcategory STRING COMMENT 'Product subcategory',
        brand STRING COMMENT 'Product brand',
        price DOUBLE COMMENT 'Current selling price in USD',
        cost DOUBLE COMMENT 'Unit cost (see glossary: Unit Cost)',
        supplier_id STRING COMMENT 'Foreign key to suppliers',
        stock_quantity INT COMMENT 'Available inventory',
        active BOOLEAN COMMENT 'Is product currently active',
        created_at TIMESTAMP COMMENT 'Product creation timestamp',
        updated_at TIMESTAMP COMMENT 'Last update timestamp'
    ) USING iceberg
""")

spark.sql(f"""
    ALTER TABLE {products_table}
    SET TBLPROPERTIES (
        'description' = 'Product catalog - master data for all products',
        'domain' = 'Product',
        'owner' = 'product-team',
        'steward' = 'product-manager@company.com',
        'source_system' = 'Oracle ERP',
        'refresh_frequency' = 'hourly',
        'last_refresh' = '{execution_date}',
        'sla' = 'Every hour at :00',
        'data_classification' = 'Internal',
        'retention_period' = 'indefinite',
        'quality_score' = '97.8',
        'tags' = 'product, catalog, master_data, reference',
        'glossary_terms' = 'SKU, Unit Cost',
        'upstream_sources' = 'Oracle ERP, Supplier Portal',
        'downstream_consumers' = 'E-commerce Website, Inventory Dashboard, Pricing Engine',
        'contact_for_access' = 'product-data@company.com'
    )
""")

print(f"✅ Created {products_table} with rich metadata")

# Table 4: Customer Revenue Summary (derived/analytical)
revenue_table = f"{full_database}.customer_revenue_summary"
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {revenue_table} (
        customer_id INT COMMENT 'Foreign key to customers_master',
        customer_name STRING COMMENT 'Full customer name',
        total_orders INT COMMENT 'Count of all orders',
        total_revenue DOUBLE COMMENT 'Sum of all order amounts (see glossary: Revenue)',
        total_cost DOUBLE COMMENT 'Sum of product costs',
        gross_margin_pct DOUBLE COMMENT 'Profitability percentage (see glossary: Gross Margin)',
        first_order_date DATE COMMENT 'Date of first purchase',
        last_order_date DATE COMMENT 'Date of most recent purchase',
        days_since_last_order INT COMMENT 'Recency metric',
        avg_order_value DOUBLE COMMENT 'Average transaction size',
        customer_segment STRING COMMENT 'VIP, Loyal, At-Risk, New',
        generated_at TIMESTAMP COMMENT 'Timestamp when summary was generated'
    ) USING iceberg
""")

spark.sql(f"""
    ALTER TABLE {revenue_table}
    SET TBLPROPERTIES (
        'description' = 'Customer revenue analytics - aggregated metrics for customer analysis',
        'domain' = 'Analytics',
        'owner' = 'analytics-team',
        'steward' = 'analytics-lead@company.com',
        'source_system' = 'Derived from customers_master + orders_fact + products_catalog',
        'refresh_frequency' = 'daily',
        'last_refresh' = '{execution_date}',
        'sla' = '10am ET daily',
        'data_classification' = 'Internal',
        'retention_period' = '3 years',
        'quality_score' = '99.5',
        'tags' = 'analytics, revenue, customer, aggregated, derived',
        'glossary_terms' = 'Revenue, Gross Margin, Customer Lifetime Value',
        'upstream_sources' = 'customers_master, orders_fact, products_catalog',
        'transformation_logic' = 'GROUP BY customer with revenue/cost aggregations',
        'downstream_consumers' = 'Executive Dashboard, Customer Segmentation ML, Marketing Campaigns',
        'table_type' = 'derived',
        'contact_for_access' = 'analytics@company.com'
    )
""")

print(f"✅ Created {revenue_table} with rich metadata")

print(f"\n📊 Created {4} sample tables with comprehensive metadata")
```

## Step 4: Build Data Catalog from Metadata

Extract all metadata into a queryable catalog table.


```python
# Get all tables in database
all_tables = spark.sql(f"SHOW TABLES IN {full_database}").collect()
table_names = [row['tableName'] for row in all_tables if not row['tableName'].startswith('business_glossary')]

print(f"📊 Building catalog for {len(table_names)} tables...\n")

# Extract metadata for each table
catalog_entries = []

for table_name in table_names:
    full_table = f"{full_database}.{table_name}"
    
    # Get table properties
    try:
        props_df = spark.sql(f"SHOW TBLPROPERTIES {full_table}")
        properties = {row['key']: row['value'] for row in props_df.collect()}
        
        # Get column information
        schema_df = spark.sql(f"DESCRIBE {full_table}")
        columns = []
        for row in schema_df.collect():
            if row['col_name'] and not row['col_name'].startswith('#'):
                columns.append({
                    "name": row['col_name'],
                    "type": row['data_type'],
                    "comment": row['comment'] if row['comment'] else ""
                })
        
        # Get table statistics
        try:
            stats = spark.table(full_table)
            row_count = stats.count()
            column_count = len(stats.columns)
        except:
            row_count = 0
            column_count = len(columns)
        
        catalog_entries.append((
            database_name,
            table_name,
            full_table,
            properties.get('description', 'No description'),
            properties.get('domain', 'Unknown'),
            properties.get('owner', 'Unknown'),
            properties.get('steward', 'Unknown'),
            properties.get('source_system', 'Unknown'),
            properties.get('data_classification', 'Unclassified'),
            properties.get('tags', ''),
            properties.get('glossary_terms', ''),
            properties.get('refresh_frequency', 'Unknown'),
            properties.get('quality_score', '0'),
            row_count,
            column_count,
            json.dumps(columns),
            properties.get('upstream_sources', ''),
            properties.get('downstream_consumers', ''),
            properties.get('contact_for_access', ''),
            datetime.now()
        ))
        
        print(f"  ✅ {table_name}: {properties.get('description', 'No description')[:60]}...")
    except Exception as e:
        print(f"  ⚠️  {table_name}: Error extracting metadata - {str(e)}")

# Create catalog schema
catalog_schema = StructType([
    StructField("database_name", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("full_table_name", StringType(), False),
    StructField("description", StringType(), False),
    StructField("domain", StringType(), False),
    StructField("owner", StringType(), False),
    StructField("steward", StringType(), False),
    StructField("source_system", StringType(), False),
    StructField("data_classification", StringType(), False),
    StructField("tags", StringType(), False),
    StructField("glossary_terms", StringType(), False),
    StructField("refresh_frequency", StringType(), False),
    StructField("quality_score", StringType(), False),
    StructField("row_count", IntegerType(), False),
    StructField("column_count", IntegerType(), False),
    StructField("columns_metadata", StringType(), False),
    StructField("upstream_sources", StringType(), False),
    StructField("downstream_consumers", StringType(), False),
    StructField("contact_for_access", StringType(), False),
    StructField("catalog_updated_at", TimestampType(), False)
])

df_catalog = spark.createDataFrame(catalog_entries, catalog_schema)

full_catalog_table = f"{full_database}.{catalog_table}"
df_catalog.writeTo(full_catalog_table).using("iceberg").createOrReplace()

spark.sql(f"""
    ALTER TABLE {full_catalog_table}
    SET TBLPROPERTIES (
        'description' = 'Data catalog - centralized metadata repository for all tables',
        'owner' = 'governance-team',
        'domain' = 'Governance',
        'refresh_frequency' = 'daily',
        'purpose' = 'Data discovery and governance'
    )
""")

print(f"\n✅ Data catalog created: {full_catalog_table}")
print(f"   Total tables cataloged: {df_catalog.count()}")
print(f"\n📊 Data Catalog Summary:")
df_catalog.select(
    "table_name", "domain", "data_classification", "owner", "quality_score", "row_count"
).show(truncate=False)
```

## Step 5: Search & Discovery - Find Data by Business Terms


```python
def search_catalog(search_term, search_in=['description', 'tags', 'glossary_terms']):
    """
    Search data catalog by business terms.
    
    Args:
        search_term: Business term to search for (e.g., 'customer', 'revenue', 'pii')
        search_in: Fields to search in
    
    Returns:
        DataFrame of matching tables
    """
    
    # Build search condition
    conditions = [col(field).contains(search_term.lower()) for field in search_in]
    from functools import reduce
    from operator import or_
    
    search_condition = reduce(or_, conditions)
    
    results = spark.table(full_catalog_table).filter(search_condition)
    
    return results

# Example 1: Find all tables with customer data
print("🔍 Search 1: Find tables related to 'customer'\n")
customer_tables = search_catalog('customer')
customer_tables.select(
    "table_name", "description", "domain", "data_classification", "owner"
).show(truncate=False)

# Example 2: Find all tables with PII data
print("\n🔍 Search 2: Find tables with PII data\n")
pii_tables = search_catalog('PII')
pii_tables.select(
    "table_name", "data_classification", "compliance", "owner", "contact_for_access"
).show(truncate=False)

# Example 3: Find all tables related to revenue
print("\n🔍 Search 3: Find tables related to 'revenue'\n")
revenue_tables = search_catalog('revenue')
revenue_tables.select(
    "table_name", "description", "domain", "owner", "quality_score"
).show(truncate=False)

# Example 4: Find tables by domain
print("\n🔍 Search 4: Find all 'Transaction' domain tables\n")
transaction_tables = spark.table(full_catalog_table).filter(col("domain") == "Transaction")
transaction_tables.select(
    "table_name", "description", "source_system", "refresh_frequency", "sla"
).show(truncate=False)

# Example 5: Find high-quality tables (quality score > 95)
print("\n🔍 Search 5: Find high-quality tables (score > 95)\n")
high_quality_tables = spark.table(full_catalog_table).filter(
    col("quality_score").cast("double") > 95
)
high_quality_tables.select(
    "table_name", "quality_score", "domain", "owner", "refresh_frequency"
).show(truncate=False)
```

## Step 6: Data Classification Report (PII, Sensitive, Public)


```python
# Group tables by data classification
classification_summary = spark.table(full_catalog_table) \
    .groupBy("data_classification") \
    .agg(
        count("*").alias("table_count"),
        collect_list("table_name").alias("tables")
    ) \
    .orderBy("table_count", ascending=False)

print("📊 Data Classification Report:\n")
classification_summary.show(truncate=False)

# Detailed PII report
print("\n⚠️  PII Data Inventory (GDPR/CCPA Compliance):\n")
pii_inventory = spark.table(full_catalog_table) \
    .filter(col("data_classification") == "PII") \
    .select(
        "table_name",
        "description",
        "owner",
        "steward",
        "retention_period",
        "compliance",
        "contact_for_access"
    )

pii_inventory.show(truncate=False)

# Domain distribution
print("\n📊 Tables by Domain:\n")
domain_summary = spark.table(full_catalog_table) \
    .groupBy("domain") \
    .agg(
        count("*").alias("table_count"),
        collect_list("table_name").alias("tables")
    ) \
    .orderBy("table_count", ascending=False)

domain_summary.show(truncate=False)

# Owner responsibility report
print("\n👥 Data Ownership Report:\n")
ownership_summary = spark.table(full_catalog_table) \
    .groupBy("owner", "steward") \
    .agg(
        count("*").alias("tables_owned"),
        collect_list("table_name").alias("tables")
    ) \
    .orderBy("tables_owned", ascending=False)

ownership_summary.show(truncate=False)
```

## Step 7: Column-Level Catalog (Detailed Schema Documentation)


```python
# Extract column-level metadata
print("📊 Column-Level Catalog:\n")

# For each table, expand columns
column_catalog = []

for row in df_catalog.collect():
    table_name = row['table_name']
    columns = json.loads(row['columns_metadata'])
    
    for col_info in columns:
        # Detect PII columns
        is_pii = any(keyword in col_info['name'].lower() for keyword in 
                     ['email', 'phone', 'address', 'ssn', 'name', 'birth', 'credit'])
        
        # Detect business key columns
        is_key = col_info['name'].endswith('_id') or col_info['name'] == 'sku'
        
        # Detect metric columns
        is_metric = any(keyword in col_info['name'].lower() for keyword in 
                       ['amount', 'revenue', 'cost', 'profit', 'value', 'count', 'total'])
        
        column_catalog.append((
            table_name,
            col_info['name'],
            col_info['type'],
            col_info['comment'],
            is_pii,
            is_key,
            is_metric,
            row['domain']
        ))

column_schema = StructType([
    StructField("table_name", StringType(), False),
    StructField("column_name", StringType(), False),
    StructField("data_type", StringType(), False),
    StructField("description", StringType(), False),
    StructField("is_pii", BooleanType(), False),
    StructField("is_business_key", BooleanType(), False),
    StructField("is_metric", BooleanType(), False),
    StructField("domain", StringType(), False)
])

df_column_catalog = spark.createDataFrame(column_catalog, column_schema)

# Show PII columns
print("⚠️  PII Columns Detected:\n")
df_column_catalog.filter(col("is_pii") == True).select(
    "table_name", "column_name", "data_type", "description"
).show(20, truncate=False)

# Show business key columns
print("\n🔑 Business Key Columns:\n")
df_column_catalog.filter(col("is_business_key") == True).select(
    "table_name", "column_name", "data_type", "description"
).show(20, truncate=False)

# Show metric columns
print("\n📈 Metric Columns:\n")
df_column_catalog.filter(col("is_metric") == True).select(
    "table_name", "column_name", "data_type", "description"
).show(20, truncate=False)
```

## Step 8: Validation & Testing


```python
if enable_validation:
    print("🧪 Running validation tests...\n")
    
    # Test 1: All tables should have metadata
    catalog_count = df_catalog.count()
    assert catalog_count >= 4, f"Should have at least 4 cataloged tables, got {catalog_count}"
    print(f"✅ Test 1 PASSED: {catalog_count} tables cataloged")
    
    # Test 2: All tables should have descriptions
    no_description = df_catalog.filter(
        (col("description") == "No description") | (col("description") == "")
    ).count()
    assert no_description == 0, f"Found {no_description} tables without descriptions"
    print(f"✅ Test 2 PASSED: All tables have descriptions")
    
    # Test 3: All tables should have owners
    no_owner = df_catalog.filter(
        (col("owner") == "Unknown") | (col("owner") == "")
    ).count()
    assert no_owner == 0, f"Found {no_owner} tables without owners"
    print(f"✅ Test 3 PASSED: All tables have assigned owners")
    
    # Test 4: Search should find relevant results
    customer_results = search_catalog('customer').count()
    assert customer_results > 0, "Search for 'customer' should return results"
    print(f"✅ Test 4 PASSED: Search found {customer_results} customer-related tables")
    
    # Test 5: PII tables should be classified
    pii_count = df_catalog.filter(col("data_classification") == "PII").count()
    assert pii_count > 0, "Should have PII classified tables"
    print(f"✅ Test 5 PASSED: {pii_count} tables classified as PII")
    
    # Test 6: Glossary should have terms
    glossary_count = spark.table(full_glossary_table).count()
    assert glossary_count >= 10, f"Should have at least 10 glossary terms, got {glossary_count}"
    print(f"✅ Test 6 PASSED: Business glossary has {glossary_count} terms")
    
    # Test 7: Column catalog should detect PII
    pii_columns = df_column_catalog.filter(col("is_pii") == True).count()
    assert pii_columns > 0, "Should detect PII columns"
    print(f"✅ Test 7 PASSED: Detected {pii_columns} PII columns")
    
    test_passed = True
    print("\n✅ All validation tests PASSED!")
else:
    test_passed = True
    print("⏭️  Validation skipped")
```

## Step 9: Production Integration & Best Practices


```python
best_practices = '''
# ============================================================================
# DATA CATALOGING BEST PRACTICES
# ============================================================================

## 1. OPENMETADATA INTEGRATION

# Install OpenMetadata ingestion framework:
pip install "openmetadata-ingestion[iceberg]"

# Configure Iceberg connector (metadata.yaml):
source:
  type: iceberg
  serviceName: openlakes_iceberg
  serviceConnection:
    config:
      type: Iceberg
      catalog:
        name: lakehouse
        connection:
          uri: http://infrastructure-nessie:19120/api/v2
          warehouse: s3a://openlakes/warehouse/
      fileSystem:
        adlsOptions:
          endpoint: http://infrastructure-minio:9000
          access_key: admin
          secret_key: admin123

sink:
  type: metadata-rest
  config:
    hostPort: http://openmetadata:8585/api

# Run ingestion:
metadata ingest -c metadata.yaml

# All Iceberg tables + metadata now visible in OpenMetadata UI!

## 2. AUTOMATED CATALOG UPDATES

# Airflow DAG for daily catalog refresh:
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    "data_catalog_refresh",
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    refresh_catalog = SparkSubmitOperator(
        task_id="refresh_catalog",
        application="/notebooks/07-governance/04-data-cataloging.ipynb",
        conn_id="spark_default",
        name="catalog_refresh",
    )

## 3. SELF-SERVICE DATA DISCOVERY

# SQL interface for analysts:
CREATE VIEW data_discovery AS
SELECT 
    table_name,
    description,
    domain,
    owner,
    steward,
    data_classification,
    tags,
    contact_for_access
FROM lakehouse.governance_demo.data_catalog
ORDER BY domain, table_name;

# Analysts can query:
SELECT * FROM data_discovery WHERE tags LIKE '%revenue%';

## 4. DATA CLASSIFICATION AUTOMATION

# Use ML/regex to auto-detect PII:
def classify_column(column_name, sample_values):
    """
    Automatically classify column as PII, sensitive, or public.
    """
    
    # Check column name patterns
    pii_patterns = ['email', 'phone', 'ssn', 'address', 'name', 'dob']
    if any(pattern in column_name.lower() for pattern in pii_patterns):
        return 'PII'
    
    # Check sample data patterns (email regex, phone regex, etc.)
    import re
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if any(re.match(email_pattern, str(val)) for val in sample_values):
        return 'PII'
    
    # Financial data
    financial_keywords = ['revenue', 'cost', 'profit', 'salary', 'price']
    if any(kw in column_name.lower() for kw in financial_keywords):
        return 'Confidential'
    
    return 'Public'

## 5. METADATA GOVERNANCE POLICIES

# Enforce metadata standards:
REQUIRED_METADATA = [
    'description',      # What is this table?
    'owner',           # Who owns this data?
    'steward',         # Who maintains quality?
    'domain',          # Which business domain?
    'source_system',   # Where does data come from?
    'data_classification',  # PII, Confidential, Public?
    'refresh_frequency',    # How often updated?
    'contact_for_access',   # Who to ask for access?
]

# Pre-commit hook: Reject table creation without metadata
def validate_table_metadata(table_name):
    properties = get_table_properties(table_name)
    
    missing = []
    for required in REQUIRED_METADATA:
        if required not in properties:
            missing.append(required)
    
    if missing:
        raise ValueError(
            f"Table {table_name} missing required metadata: {missing}\n"
            f"Please add metadata via ALTER TABLE SET TBLPROPERTIES"
        )

## 6. DATA LINEAGE + CATALOG INTEGRATION

# Link catalog with lineage:
SELECT 
    c.table_name,
    c.description,
    c.owner,
    l.upstream_sources,
    l.downstream_consumers
FROM data_catalog c
LEFT JOIN lineage_events l 
    ON c.table_name = l.table_name
ORDER BY c.domain, c.table_name;

# Now you know: "What is this table AND where does it come from?"

## 7. ACCESS CONTROL + CATALOG

# Integrate with Ranger/OPA for policy enforcement:
# 
# Policy: "Only users in 'analytics-team' can access PII tables"
# 
# Ranger policy (pseudocode):
ALLOW access TO tables
WHERE data_classification = 'PII'
AND user IN (SELECT members FROM teams WHERE team_name = 'analytics-team')

# Auto-generate policies from catalog metadata!

## 8. DOCUMENTATION GENERATION

# Auto-generate data dictionary from catalog:
import pandas as pd

# Export catalog to Markdown
catalog_df = spark.table("data_catalog").toPandas()

with open("data_dictionary.md", "w") as f:
    f.write("# Data Dictionary\n\n")
    
    for domain in catalog_df['domain'].unique():
        f.write(f"\n## {domain} Domain\n\n")
        
        domain_tables = catalog_df[catalog_df['domain'] == domain]
        
        for _, row in domain_tables.iterrows():
            f.write(f"\n### {row['table_name']}\n\n")
            f.write(f"**Description**: {row['description']}\n\n")
            f.write(f"**Owner**: {row['owner']}\n\n")
            f.write(f"**Classification**: {row['data_classification']}\n\n")
            
            # Add columns
            columns = json.loads(row['columns_metadata'])
            f.write("\n**Columns**:\n\n")
            f.write("| Column | Type | Description |\n")
            f.write("|--------|------|-------------|\n")
            for col in columns:
                f.write(f"| {col['name']} | {col['type']} | {col['comment']} |\n")

print("Generated: data_dictionary.md")

## 9. SLACK NOTIFICATIONS FOR NEW DATA

# Notify teams when new datasets are available:
def notify_new_table(table_name, description, owner):
    slack_message = {
        "text": f"📊 New Dataset Available: {table_name}",
        "attachments": [
            {
                "color": "good",
                "fields": [
                    {"title": "Description", "value": description},
                    {"title": "Owner", "value": owner},
                    {"title": "Catalog", "value": f"Query: SELECT * FROM {table_name}"}
                ]
            }
        ]
    }
    
    # Post to #data-announcements channel
    slack_client.post_message(channel="#data-announcements", message=slack_message)

## 10. METRICS & MONITORING

# Track catalog health:
SELECT 
    COUNT(*) as total_tables,
    SUM(CASE WHEN description = '' THEN 1 ELSE 0 END) as missing_description,
    SUM(CASE WHEN owner = 'Unknown' THEN 1 ELSE 0 END) as missing_owner,
    AVG(CAST(quality_score AS DOUBLE)) as avg_quality_score,
    SUM(CASE WHEN data_classification = 'Unclassified' THEN 1 ELSE 0 END) as unclassified
FROM data_catalog;

# Alert if:
# - More than 10% tables missing descriptions
# - Any tables missing owners
# - Average quality score < 90

# ============================================================================
'''

print("📋 Data Cataloging Best Practices:")
print("="*80)
print(best_practices)
print("="*80)
```

## Summary

### ✅ Pattern 7.4: Data Cataloging & Discovery Complete!

This notebook demonstrated:

1. **Business Glossary**: Standardized terminology and definitions
   - 10 business terms across Customer, Product, Financial domains
   - Links technical names to business names
   - Owner and approval tracking

2. **Rich Metadata Management**: Comprehensive table properties
   - Description, domain, owner, steward
   - Source system, refresh frequency, SLA
   - Data classification, compliance, retention
   - Quality scores, tags, glossary terms
   - Upstream sources, downstream consumers

3. **Automated Catalog Generation**: Extract metadata into queryable table
   - Table-level catalog with 20+ metadata fields
   - Column-level catalog with PII detection
   - Automated refresh capability

4. **Search & Discovery**: Find data by business terms
   - Search by description, tags, glossary terms
   - Filter by domain, classification, owner
   - Quality-based discovery

5. **Data Classification**: PII, Confidential, Internal, Public
   - Automated PII detection
   - Compliance tracking (GDPR, CCPA, SOX)
   - Ownership and stewardship

6. **Column-Level Metadata**: Detailed schema documentation
   - PII column detection
   - Business key identification
   - Metric column classification

### Key Benefits:

- **Self-Service Analytics**: Analysts find data without asking engineers
- **Data Mesh**: Decentralized ownership with central discovery
- **Compliance**: Track PII and sensitive data for GDPR/CCPA
- **Onboarding**: New team members discover datasets quickly
- **Governance**: Enforce metadata standards and naming conventions

### Production Integration:

- **OpenMetadata**: Central catalog with UI and API
- **Automated Ingestion**: Daily catalog refresh via Airflow
- **Access Control**: Policy enforcement via Ranger/OPA
- **Documentation**: Auto-generated data dictionaries
- **Notifications**: Slack alerts for new datasets

### Complete Governance Suite:

- **Pattern 7.1**: Data Quality (validation & monitoring)
- **Pattern 7.2**: Schema Evolution (safe changes)
- **Pattern 7.3**: Data Lineage (track data flow)
- **Pattern 7.4**: Data Cataloging (discovery & metadata) ✅

### Next Steps:

1. Deploy OpenMetadata for UI-based catalog
2. Configure automated metadata ingestion
3. Integrate with access control systems
4. Train teams on self-service discovery


```python
# Cleanup (optional)
if enable_cleanup:
    # Drop all created tables
    for table in [customers_table, orders_table, products_table, revenue_table, 
                  full_glossary_table, full_catalog_table]:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(f"DROP DATABASE IF EXISTS {full_database}")
    print("🧹 Cleanup completed")
else:
    print("⏭️  Cleanup skipped (enable_cleanup=False)")
    print(f"\n📊 Tables created:")
    print(f"   Business Glossary:")
    print(f"     - {full_glossary_table}")
    print(f"   Sample Tables:")
    print(f"     - {customers_table} (PII)")
    print(f"     - {orders_table} (Transactional)")
    print(f"     - {products_table} (Reference)")
    print(f"     - {revenue_table} (Analytical)")
    print(f"   Data Catalog:")
    print(f"     - {full_catalog_table}")
    print(f"\n🔍 Try searching the catalog:")
    print(f"   SELECT * FROM {full_catalog_table} WHERE tags LIKE '%customer%'")
```
