# Pattern 6.2: Audit Trail & Compliance Logging

Build immutable audit trail system for compliance and security:

1. **Kafka**: User action event stream
2. **Spark Streaming**: Process and enrich audit events
3. **Iceberg**: Immutable audit log storage
4. **Trino**: Compliance queries and reports

## Architecture

```
Applications (user actions) → Kafka
                                ↓
                    Spark Streaming (foreachBatch)
                                ↓
                   Iceberg (immutable audit log)
                                ↓
                    Trino (compliance queries)
```

**Audit Trail Pattern**:
- **Immutable**: Write-once, never delete (compliance requirement)
- **Complete**: Track WHO did WHAT, WHEN, WHERE, and RESULT
- **Queryable**: SQL access for audit investigations
- **Retention**: Multi-year retention policies (GDPR, SOX, HIPAA)

**Use Cases**:
- GDPR: Track all personal data access
- SOX: Financial system audit trails
- HIPAA: Healthcare data access logs
- Security: Incident investigation and forensics
- Compliance: Regulatory reporting

**Events Tracked**:
- User authentication (login/logout)
- Data access (read/write/delete)
- Permission changes (role assignments)
- Data exports (download, transfer)
- Query execution (SQL, API calls)
- System changes (configuration updates)

**Benefits**:
- Compliance-ready (immutable log)
- Complete audit trail (all actions tracked)
- Fast queries (Iceberg metadata filtering)
- Cost-efficient (S3 storage, partitioned)

## Parameters


```python
execution_date = "2025-01-16"
environment = "development"
database_name = "demo"
kafka_topic = "audit_events"
iceberg_table = "audit_trail"
kafka_bootstrap_servers = "infrastructure-kafka.openlakes.svc.cluster.local:9092"
num_audit_events = 250
streaming_duration_seconds = 30
enable_validation = True
enable_cleanup = False
```

## Setup: Initialize Spark with Iceberg


```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, lit, expr, unix_timestamp,
    count, countDistinct, min as spark_min, max as spark_max, avg, sum as spark_sum
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, IntegerType, BooleanType
)
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
    .appName(f"Pattern-6.2-AuditTrail-{environment}") \
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

print("✅ Spark initialized for audit trail pattern")
```

## Setup: Create Immutable Audit Trail Table


```python
full_database = f"lakehouse.{database_name}"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {full_database}")

full_table = f"{full_database}.{iceberg_table}"

# Create immutable audit trail table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {full_table} (
        audit_id STRING,
        event_timestamp TIMESTAMP,
        user_id STRING,
        user_email STRING,
        action_type STRING,
        resource_type STRING,
        resource_id STRING,
        resource_name STRING,
        action_result STRING,
        ip_address STRING,
        user_agent STRING,
        session_id STRING,
        details STRING,
        ingestion_timestamp TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (days(event_timestamp), action_type)
    TBLPROPERTIES (
        'write.metadata.delete-after-commit.enabled' = 'true',
        'write.metadata.previous-versions-max' = '100',
        'write.delete.mode' = 'copy-on-write',
        'format-version' = '2',
        'comment' = 'Immutable audit trail for compliance (GDPR, SOX, HIPAA)'
    )
""")

print(f"✅ Immutable audit trail table created: {full_table}")
print("   Partitioned by: event_timestamp (days), action_type")
print("   Compliance: GDPR, SOX, HIPAA ready")
```

## Kafka Producer: Generate Audit Events

Simulate realistic audit events:
- User authentication (login/logout)
- Data access (read/write/delete)
- Permission changes
- Data exports
- Query execution


```python
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"📊 Producing {num_audit_events} audit events to Kafka...")

# Realistic audit event data
users = [
    {"id": "user_001", "email": "alice@company.com", "role": "data_analyst"},
    {"id": "user_002", "email": "bob@company.com", "role": "admin"},
    {"id": "user_003", "email": "charlie@company.com", "role": "data_engineer"},
    {"id": "user_004", "email": "diana@company.com", "role": "finance_user"},
    {"id": "user_005", "email": "eve@company.com", "role": "auditor"},
]

action_types = [
    "user_login", "user_logout",
    "data_read", "data_write", "data_delete",
    "data_export", "data_download",
    "permission_grant", "permission_revoke",
    "query_execute", "report_generate",
    "config_update", "user_create", "user_delete"
]

resource_types = [
    "database", "table", "file", "report", "user", "role", "system_config"
]

ip_addresses = [
    "192.168.1.10", "192.168.1.11", "192.168.1.12",
    "10.0.0.50", "10.0.0.51",
    "203.0.113.45", "203.0.113.46"  # Some external IPs
]

user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/17.0",
    "API Client/1.0",
    "Python/3.9 requests/2.28.0"
]

# Generate audit events with realistic patterns
base_time = datetime.utcnow() - timedelta(hours=2)
sessions = {}

for i in range(num_audit_events):
    user = random.choice(users)
    action = random.choice(action_types)
    
    # Create session if user login
    if action == "user_login":
        session_id = str(uuid.uuid4())
        sessions[user["id"]] = session_id
        result = "success" if random.random() > 0.05 else "failure"  # 5% login failures
    else:
        session_id = sessions.get(user["id"], str(uuid.uuid4()))
        result = "success" if random.random() > 0.02 else "failure"  # 2% action failures
    
    # Resource details based on action
    if action in ["data_read", "data_write", "data_delete", "query_execute"]:
        resource_type = random.choice(["database", "table"])
        resource_id = f"db_{random.randint(1, 10)}.table_{random.randint(1, 50)}"
        resource_name = f"customer_data.transactions"
    elif action in ["data_export", "data_download"]:
        resource_type = "file"
        resource_id = f"file_{random.randint(1000, 9999)}"
        resource_name = f"export_{datetime.utcnow().strftime('%Y%m%d')}.csv"
    elif action in ["permission_grant", "permission_revoke"]:
        resource_type = "role"
        resource_id = f"role_{random.choice(['analyst', 'admin', 'viewer'])}"
        resource_name = resource_id
    else:
        resource_type = random.choice(resource_types)
        resource_id = f"{resource_type}_{random.randint(1, 100)}"
        resource_name = resource_id
    
    # Details based on action
    if action == "query_execute":
        details = json.dumps({
            "query": "SELECT * FROM customer_data.transactions WHERE amount > 1000",
            "rows_returned": random.randint(10, 10000)
        })
    elif action == "data_export":
        details = json.dumps({
            "format": "CSV",
            "rows_exported": random.randint(100, 50000),
            "file_size_mb": round(random.uniform(1, 500), 2)
        })
    elif action == "permission_grant":
        details = json.dumps({
            "permission": "READ",
            "granted_to": random.choice(users)["email"]
        })
    else:
        details = json.dumps({"action": action})
    
    # Create audit event
    event_time = base_time + timedelta(seconds=i * 30)
    
    audit_event = {
        "audit_id": str(uuid.uuid4()),
        "event_timestamp": event_time.isoformat(),
        "user_id": user["id"],
        "user_email": user["email"],
        "action_type": action,
        "resource_type": resource_type,
        "resource_id": resource_id,
        "resource_name": resource_name,
        "action_result": result,
        "ip_address": random.choice(ip_addresses),
        "user_agent": random.choice(user_agents),
        "session_id": session_id,
        "details": details
    }
    
    producer.send(kafka_topic, value=audit_event)
    
    if (i + 1) % 50 == 0:
        print(f"  Produced {i+1}/{num_audit_events} events...")
        time.sleep(0.1)

producer.flush()
print(f"✅ Produced {num_audit_events} audit events to Kafka")
print(f"   Events: {', '.join(random.sample(action_types, 5))}...")
```

## Streaming: Write Audit Events to Iceberg

Process audit events and write to immutable Iceberg table.


```python
# Define audit event schema
audit_schema = StructType([
    StructField("audit_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("user_email", StringType(), True),
    StructField("action_type", StringType(), True),
    StructField("resource_type", StringType(), True),
    StructField("resource_id", StringType(), True),
    StructField("resource_name", StringType(), True),
    StructField("action_result", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("details", StringType(), True)
])

# Read from Kafka
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse audit events
df_parsed = df_stream.select(
    from_json(col("value").cast("string"), audit_schema).alias("data")
).select(
    col("data.audit_id").alias("audit_id"),
    col("data.event_timestamp").cast(TimestampType()).alias("event_timestamp"),
    col("data.user_id").alias("user_id"),
    col("data.user_email").alias("user_email"),
    col("data.action_type").alias("action_type"),
    col("data.resource_type").alias("resource_type"),
    col("data.resource_id").alias("resource_id"),
    col("data.resource_name").alias("resource_name"),
    col("data.action_result").alias("action_result"),
    col("data.ip_address").alias("ip_address"),
    col("data.user_agent").alias("user_agent"),
    col("data.session_id").alias("session_id"),
    col("data.details").alias("details"),
    current_timestamp().alias("ingestion_timestamp")
)

# Write to immutable audit trail
def write_audit_trail(batch_df, batch_id):
    if batch_df.count() == 0:
        return
    
    print(f"\n  Batch {batch_id}: Processing {batch_df.count()} audit events")
    
    # Write to Iceberg (append-only, immutable)
    batch_df.writeTo(full_table).using("iceberg").append()
    
    # Log audit statistics
    action_counts = batch_df.groupBy("action_type").count().collect()
    failure_count = batch_df.filter(col("action_result") == "failure").count()
    
    print(f"  ✅ Written to immutable audit trail")
    print(f"     Actions: {', '.join([f'{r.action_type}({r.count})' for r in action_counts[:5]])}")
    print(f"     Failures: {failure_count}")

# Start streaming query
query = df_parsed.writeStream \
    .foreachBatch(write_audit_trail) \
    .option("checkpointLocation", f"/tmp/checkpoint_audit_trail_{iceberg_table}") \
    .trigger(processingTime='5 seconds') \
    .start()

print(f"\n✅ Audit trail streaming started")
print(f"   → Target: {full_table}")
print(f"   → Mode: Append-only (immutable)")
print(f"⏳ Processing for {streaming_duration_seconds} seconds...\n")

time.sleep(streaming_duration_seconds)

query.stop()
print("\n✅ Streaming query stopped")
```

## Compliance Query 1: Data Access Audit

GDPR Article 30: Track all personal data access.


```python
# Query all data access events
df_data_access = spark.sql(f"""
    SELECT
        event_timestamp,
        user_email,
        action_type,
        resource_name,
        action_result,
        ip_address
    FROM {full_table}
    WHERE action_type IN ('data_read', 'data_write', 'data_delete', 'data_export')
    ORDER BY event_timestamp DESC
    LIMIT 20
""")

print("📊 GDPR Data Access Audit (Recent 20 Events):")
df_data_access.show(20, truncate=False)

# Summary by user
df_user_activity = spark.sql(f"""
    SELECT
        user_email,
        COUNT(*) as total_actions,
        SUM(CASE WHEN action_type = 'data_read' THEN 1 ELSE 0 END) as reads,
        SUM(CASE WHEN action_type = 'data_write' THEN 1 ELSE 0 END) as writes,
        SUM(CASE WHEN action_type = 'data_delete' THEN 1 ELSE 0 END) as deletes,
        SUM(CASE WHEN action_type = 'data_export' THEN 1 ELSE 0 END) as exports
    FROM {full_table}
    WHERE action_type IN ('data_read', 'data_write', 'data_delete', 'data_export')
    GROUP BY user_email
    ORDER BY total_actions DESC
""")

print("\n📊 Data Access Summary by User:")
df_user_activity.show(truncate=False)
```

## Compliance Query 2: Failed Access Attempts

Security audit: Track all failed access attempts.


```python
# Query failed access attempts
df_failures = spark.sql(f"""
    SELECT
        event_timestamp,
        user_email,
        action_type,
        resource_type,
        resource_name,
        ip_address,
        details
    FROM {full_table}
    WHERE action_result = 'failure'
    ORDER BY event_timestamp DESC
    LIMIT 15
""")

print("🚨 Failed Access Attempts (Security Audit):")
df_failures.show(15, truncate=False)

# Failed login attempts by IP
df_failed_logins = spark.sql(f"""
    SELECT
        ip_address,
        COUNT(*) as failed_attempts,
        COUNT(DISTINCT user_email) as unique_users,
        MIN(event_timestamp) as first_attempt,
        MAX(event_timestamp) as last_attempt
    FROM {full_table}
    WHERE action_type = 'user_login' AND action_result = 'failure'
    GROUP BY ip_address
    HAVING COUNT(*) >= 2
    ORDER BY failed_attempts DESC
""")

print("\n🚨 Suspicious Failed Login Patterns (Multiple Attempts):")
df_failed_logins.show(truncate=False)
```

## Compliance Query 3: Permission Changes Audit

SOX compliance: Track all permission grants/revocations.


```python
# Query permission changes
df_permissions = spark.sql(f"""
    SELECT
        event_timestamp,
        user_email as changed_by,
        action_type,
        resource_type,
        resource_name,
        details,
        action_result
    FROM {full_table}
    WHERE action_type IN ('permission_grant', 'permission_revoke')
    ORDER BY event_timestamp DESC
""")

print("📊 SOX Permission Changes Audit:")
df_permissions.show(truncate=False)

# Summary of permission changes by admin
df_admin_actions = spark.sql(f"""
    SELECT
        user_email,
        COUNT(*) as total_changes,
        SUM(CASE WHEN action_type = 'permission_grant' THEN 1 ELSE 0 END) as grants,
        SUM(CASE WHEN action_type = 'permission_revoke' THEN 1 ELSE 0 END) as revocations
    FROM {full_table}
    WHERE action_type IN ('permission_grant', 'permission_revoke')
    GROUP BY user_email
    ORDER BY total_changes DESC
""")

print("\n📊 Permission Changes by Administrator:")
df_admin_actions.show(truncate=False)
```

## Compliance Query 4: User Session Timeline

Reconstruct complete user session for incident investigation.


```python
# Get a sample session_id
sample_session = spark.sql(f"""
    SELECT session_id
    FROM {full_table}
    WHERE action_type = 'user_login'
    LIMIT 1
""").collect()[0][0]

# Query complete session timeline
df_session = spark.sql(f"""
    SELECT
        event_timestamp,
        user_email,
        action_type,
        resource_type,
        resource_name,
        action_result,
        ip_address
    FROM {full_table}
    WHERE session_id = '{sample_session}'
    ORDER BY event_timestamp ASC
""")

print(f"📊 Complete User Session Timeline (Session: {sample_session[:8]}...):")
df_session.show(50, truncate=False)

# Session statistics
df_session_stats = spark.sql(f"""
    SELECT
        session_id,
        user_email,
        MIN(event_timestamp) as session_start,
        MAX(event_timestamp) as session_end,
        COUNT(*) as total_actions,
        COUNT(DISTINCT action_type) as unique_action_types,
        SUM(CASE WHEN action_result = 'failure' THEN 1 ELSE 0 END) as failures
    FROM {full_table}
    WHERE session_id = '{sample_session}'
    GROUP BY session_id, user_email
""")

print("\n📊 Session Statistics:")
df_session_stats.show(truncate=False)
```

## Compliance Query 5: Data Lineage & Export Tracking

Track all data exports for compliance reporting.


```python
# Query data exports
df_exports = spark.sql(f"""
    SELECT
        event_timestamp,
        user_email,
        resource_name,
        details,
        ip_address
    FROM {full_table}
    WHERE action_type IN ('data_export', 'data_download')
    ORDER BY event_timestamp DESC
""")

print("📊 Data Export Audit (Compliance Tracking):")
df_exports.show(truncate=False)

# Export volume by user
df_export_volume = spark.sql(f"""
    SELECT
        user_email,
        COUNT(*) as total_exports,
        COUNT(DISTINCT DATE(event_timestamp)) as active_days
    FROM {full_table}
    WHERE action_type IN ('data_export', 'data_download')
    GROUP BY user_email
    ORDER BY total_exports DESC
""")

print("\n📊 Data Export Volume by User:")
df_export_volume.show(truncate=False)
```

## Advanced: Anomaly Detection

Detect unusual access patterns for security monitoring.


```python
# Detect users accessing data outside normal hours (example: outside 9am-5pm)
df_off_hours = spark.sql(f"""
    SELECT
        event_timestamp,
        user_email,
        action_type,
        resource_name,
        ip_address,
        HOUR(event_timestamp) as access_hour
    FROM {full_table}
    WHERE action_type IN ('data_read', 'data_export', 'data_delete')
      AND (HOUR(event_timestamp) < 9 OR HOUR(event_timestamp) > 17)
    ORDER BY event_timestamp DESC
""")

print("🚨 Off-Hours Data Access (Security Alert):")
df_off_hours.show(truncate=False)

# Detect users with unusual IP address changes
df_ip_changes = spark.sql(f"""
    SELECT
        user_email,
        COUNT(DISTINCT ip_address) as unique_ips,
        COUNT(*) as total_actions,
        COLLECT_SET(ip_address) as ip_addresses
    FROM {full_table}
    GROUP BY user_email
    HAVING COUNT(DISTINCT ip_address) > 2
    ORDER BY unique_ips DESC
""")

print("\n🚨 Users with Multiple IP Addresses (Potential Account Sharing):")
df_ip_changes.show(truncate=False)
```

## Production Deployment Example


```python
print("""
💡 Production Deployment: Audit Trail Pipeline

## Kubernetes SparkApplication

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: audit-trail-pipeline
  namespace: openlakes
spec:
  type: Python
  mode: cluster
  image: ghcr.io/openlakes/openlakes-core/spark-openmetadata:1.0.0
  mainApplicationFile: local:///opt/spark/apps/audit_trail.py
  
  arguments:
    - --kafka-topic=audit_events
    - --iceberg-table=lakehouse.compliance.audit_trail
    - --checkpoint-location=s3a://openlakes/checkpoints/audit_trail/
  
  sparkConf:
    "spark.sql.catalog.lakehouse": "org.apache.iceberg.spark.SparkCatalog"
    "spark.sql.catalog.lakehouse.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog"
    "spark.sql.catalog.lakehouse.uri": "http://infrastructure-nessie:19120/api/v2"
    "spark.sql.catalog.lakehouse.warehouse": "s3a://openlakes/warehouse/"
  
  driver:
    cores: 1
    memory: "2g"
    serviceAccount: spark-operator
  
  executor:
    cores: 2
    instances: 3
    memory: "4g"
  
  restartPolicy:
    type: Always
```

## Retention Policy (Compliance Requirements)

```sql
-- GDPR: 7 years retention for financial data
-- SOX: 7 years retention for financial audit trails
-- HIPAA: 6 years retention for healthcare audit logs

-- Set table retention policy
ALTER TABLE lakehouse.compliance.audit_trail
SET TBLPROPERTIES (
    'gc.enabled' = 'true',
    'write.delete.mode' = 'copy-on-write',
    'history.expire.max-snapshot-age-ms' = '220752000000'  -- 7 years
);

-- Expire old snapshots (metadata cleanup, NOT data deletion)
CALL lakehouse.system.expire_snapshots(
    table => 'compliance.audit_trail',
    older_than => TIMESTAMP '2018-01-01 00:00:00'
);
```

## MinIO Lifecycle Policy (Archive to Glacier)

```xml
<LifecycleConfiguration>
  <Rule>
    <ID>archive-old-audit-logs</ID>
    <Status>Enabled</Status>
    <Filter>
      <Prefix>warehouse/compliance/audit_trail/</Prefix>
    </Filter>
    <Transition>
      <Days>365</Days>
      <StorageClass>GLACIER</StorageClass>
    </Transition>
  </Rule>
</LifecycleConfiguration>
```

## Compliance Queries (Scheduled via Airflow)

```python
# Airflow DAG for daily compliance reports
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG('audit_compliance_reports', schedule_interval='@daily') as dag:
    
    gdpr_report = SparkSubmitOperator(
        task_id='gdpr_data_access_report',
        application='/opt/airflow/jobs/gdpr_report.py',
        conn_id='spark_k8s'
    )
    
    sox_report = SparkSubmitOperator(
        task_id='sox_permission_changes_report',
        application='/opt/airflow/jobs/sox_report.py',
        conn_id='spark_k8s'
    )
    
    security_anomaly = SparkSubmitOperator(
        task_id='security_anomaly_detection',
        application='/opt/airflow/jobs/security_anomaly.py',
        conn_id='spark_k8s'
    )
```

## Trino Query Service (Read-Only Compliance Access)

```sql
-- Analysts can query audit trail via Trino (read-only)
-- No direct write access to ensure immutability

SELECT
    event_timestamp,
    user_email,
    action_type,
    resource_name,
    action_result
FROM lakehouse.compliance.audit_trail
WHERE event_timestamp >= CURRENT_DATE - INTERVAL '30' DAY
  AND action_type = 'data_export'
ORDER BY event_timestamp DESC;
```

## Monitoring & Alerting

```python
# Alert on suspicious patterns
# - Failed login attempts > 5 from same IP
# - Data exports outside business hours
# - Permission changes by non-admins
# - Unusual data access volume

spark.sql("""
    SELECT ip_address, COUNT(*) as failures
    FROM lakehouse.compliance.audit_trail
    WHERE action_type = 'user_login'
      AND action_result = 'failure'
      AND event_timestamp >= CURRENT_TIMESTAMP - INTERVAL 1 HOUR
    GROUP BY ip_address
    HAVING COUNT(*) > 5
""").show()
```
""")
```

## Validation


```python
if enable_validation:
    # Check audit trail records
    total_events = spark.sql(f"SELECT COUNT(*) FROM {full_table}").collect()[0][0]
    assert total_events > 0, "Should have audit events in Iceberg"
    print(f"✅ Total audit events: {total_events}")
    
    # Verify action types
    action_types_count = spark.sql(f"""
        SELECT COUNT(DISTINCT action_type) FROM {full_table}
    """).collect()[0][0]
    assert action_types_count > 0, "Should have multiple action types"
    print(f"✅ Unique action types: {action_types_count}")
    
    # Verify users tracked
    users_count = spark.sql(f"""
        SELECT COUNT(DISTINCT user_email) FROM {full_table}
    """).collect()[0][0]
    assert users_count > 0, "Should have multiple users"
    print(f"✅ Users tracked: {users_count}")
    
    # Verify failures captured
    failures = spark.sql(f"""
        SELECT COUNT(*) FROM {full_table} WHERE action_result = 'failure'
    """).collect()[0][0]
    print(f"✅ Failed actions captured: {failures}")
    
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

### ✅ Pattern 6.2: Audit Trail & Compliance Logging Complete!

Demonstrated:
1. **Immutable Audit Trail**: Append-only Iceberg table for compliance
2. **Complete Tracking**: WHO, WHAT, WHEN, WHERE, RESULT
3. **Compliance Queries**: GDPR, SOX, HIPAA audit reports
4. **Security Monitoring**: Failed access, anomaly detection
5. **Data Lineage**: Export tracking and permission changes

**Compliance Framework**:

| Regulation | Requirement | Implementation |
|------------|-------------|----------------|
| **GDPR** | Article 30 - Track all personal data access | ✅ Data access audit logs |
| **SOX** | Section 404 - Internal controls audit trail | ✅ Permission change tracking |
| **HIPAA** | §164.308 - Access control audit | ✅ Healthcare data access logs |
| **PCI DSS** | Requirement 10 - Track all access to cardholder data | ✅ Complete audit trail |

**Audit Events Tracked**:
- 🔐 **Authentication**: user_login, user_logout
- 📊 **Data Access**: data_read, data_write, data_delete
- 📤 **Data Export**: data_export, data_download
- 🔑 **Permissions**: permission_grant, permission_revoke
- 🔍 **Queries**: query_execute, report_generate
- ⚙️ **System**: config_update, user_create, user_delete

**Key Features**:
- ✅ **Immutable**: Write-once, append-only (compliance requirement)
- ✅ **Complete**: All user actions tracked with context
- ✅ **Queryable**: SQL access for audit investigations
- ✅ **Partitioned**: Efficient queries by date and action type
- ✅ **Retention**: Multi-year storage (7 years for SOX/GDPR)

**Production Best Practices**:
1. **Never delete audit logs** (immutability requirement)
2. **Partition by date and action_type** (query performance)
3. **Encrypt at rest** (S3/MinIO encryption)
4. **Read-only access via Trino** (prevent tampering)
5. **Archive to Glacier** (cost optimization after 1 year)
6. **Scheduled compliance reports** (daily/weekly via Airflow)

**Security & Anomaly Detection**:
- 🚨 Failed login attempts (brute force detection)
- 🚨 Off-hours data access (unusual patterns)
- 🚨 Multiple IP addresses per user (account sharing)
- 🚨 Unusual export volumes (data exfiltration)

**When to Use This Pattern**:
- ✅ Regulatory compliance (GDPR, SOX, HIPAA, PCI DSS)
- ✅ Security audits and incident investigation
- ✅ Internal access control monitoring
- ✅ Data governance and lineage tracking

## Cleanup


```python
if enable_cleanup:
    spark.sql(f"DROP TABLE IF EXISTS {full_table}")
    print("🧹 Cleanup completed")
else:
    print("⏭️  Cleanup skipped")
```
