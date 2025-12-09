# Data Ingestion

OpenLakes Harbor provides built-in data ingestion capabilities to bring external data into your lakehouse environment. The ingestion system supports both real-time change data capture (CDC) and batch ELT pipelines.

## Overview

Each Harbor tenant environment includes:

- **Apache Kafka** - Message broker for streaming data
- **Kafka Connect** - Connector framework with Debezium for CDC
- **Meltano** - Batch ELT orchestration using Singer taps/targets
- **kafka-ui** - Web interface for managing Kafka and connectors
- **Metadata Sync** - Automatic catalog updates to OpenMetadata

## Data Flow Architecture

```
                      ┌─────────────────────────────────────────────┐
                      │           External Data Sources             │
                      │  (MySQL, PostgreSQL, MongoDB, APIs, Files)  │
                      └─────────────────────────────────────────────┘
                                           │
                   ┌───────────────────────┼───────────────────────┐
                   │                       │                       │
                   ▼                       ▼                       ▼
           ┌───────────────┐      ┌───────────────┐      ┌───────────────┐
           │  Debezium CDC │      │  Singer Taps  │      │   File Upload │
           │  (Real-time)  │      │   (Batch)     │      │    (S3/MinIO) │
           └───────────────┘      └───────────────┘      └───────────────┘
                   │                       │                       │
                   ▼                       ▼                       │
           ┌───────────────┐      ┌───────────────┐                │
           │    Kafka      │      │    Meltano    │                │
           │    Topics     │      │  Orchestrator │                │
           └───────────────┘      └───────────────┘                │
                   │                       │                       │
                   └───────────────────────┼───────────────────────┘
                                           │
                                           ▼
                               ┌───────────────────────┐
                               │    MinIO (S3 Storage) │
                               │   tenant-{id}/raw/    │
                               └───────────────────────┘
                                           │
                                           ▼
                               ┌───────────────────────┐
                               │    OpenMetadata       │
                               │   (Auto-cataloged)    │
                               └───────────────────────┘
```

## Real-time CDC with Debezium

Change Data Capture streams database changes in real-time. Supported databases:

| Database | Connector | Method |
|----------|-----------|--------|
| MySQL | `io.debezium.connector.mysql.MySqlConnector` | Binary log |
| PostgreSQL | `io.debezium.connector.postgresql.PostgresConnector` | Logical replication (pgoutput) |
| MongoDB | `io.debezium.connector.mongodb.MongoDbConnector` | Change streams |

### How CDC Works

1. **Debezium** reads the database's change log (binlog, WAL, or change streams)
2. Changes are published to **Kafka topics** with schema information
3. A **sink connector** writes data to MinIO in time-partitioned paths
4. **OpenMetadata** automatically catalogs the new data

### Storage Structure

CDC data is stored in your tenant's MinIO bucket:

```
tenant-{id}/
└── raw/
    └── cdc.{connector}.{table}/
        └── {year}/
            └── {month}/
                └── {day}/
                    └── {hour}/
                        └── {message-id}.json
```

## Batch ELT with Meltano

Meltano enables batch extraction from 500+ sources using the Singer protocol.

### Pre-installed Extractors

| Extractor | Use Case |
|-----------|----------|
| `tap-postgres` | PostgreSQL databases |
| `tap-mysql` | MySQL databases |
| `tap-csv` | CSV files |
| `tap-rest-api` | REST APIs |
| `tap-s3-csv` | CSV files from S3 |

Additional extractors can be installed via the API.

### Default Loader

| Loader | Destination |
|--------|-------------|
| `target-s3` | MinIO (tenant bucket) |

## Accessing Ingestion Services

### kafka-ui

Access the Kafka management interface at:

```
https://{tenant-id}.kafka.harbor.openlakes.io
```

From kafka-ui you can:

- View Kafka topics and messages
- Monitor consumer groups
- Create and manage Debezium connectors
- View connector status and errors

### API Endpoints

All ingestion endpoints require authentication.

#### Get Ingestion Status

```bash
GET /api/v1/tenants/{tenant-id}/ingestion
```

Returns the status of Kafka Connect, Meltano, and metadata sync services.

**Response:**
```json
{
  "kafkaConnect": {
    "available": true,
    "connectors": []
  },
  "meltano": {
    "available": true,
    "extractors": [
      {"name": "tap-postgres", "variant": "meltanolabs", "installed": false}
    ],
    "jobs": []
  },
  "metadataSync": {
    "enabled": true,
    "intervalSeconds": 60,
    "pendingIngestions": 0
  }
}
```

#### Create CDC Connector

```bash
POST /api/v1/tenants/{tenant-id}/ingestion/connectors
Content-Type: application/json

{
  "name": "my-orders-cdc",
  "type": "mysql",
  "config": {
    "database.hostname": "db.example.com",
    "database.port": "3306",
    "database.user": "replication_user",
    "database.password": "secret",
    "database.dbname": "ecommerce"
  }
}
```

**Supported types:** `mysql`, `postgres`, `mongodb`

#### Connector Actions

```bash
POST /api/v1/tenants/{tenant-id}/ingestion/connectors/{name}/{action}
```

**Actions:**

- `pause` - Pause the connector
- `resume` - Resume the connector
- `restart` - Restart the connector
- `delete` - Delete the connector

#### Run Meltano Extraction

```bash
POST /api/v1/tenants/{tenant-id}/ingestion/meltano/run
Content-Type: application/json

{
  "extractor": "tap-postgres",
  "loader": "target-s3"
}
```

#### Install Meltano Plugin

```bash
POST /api/v1/tenants/{tenant-id}/ingestion/meltano/install
Content-Type: application/json

{
  "type": "extractor",
  "name": "tap-github"
}
```

#### Test Connection

Validate database connectivity before creating a connector:

```bash
POST /api/v1/tenants/{tenant-id}/ingestion/test-connection
Content-Type: application/json

{
  "type": "postgres",
  "hostname": "db.example.com",
  "port": 5432,
  "username": "user",
  "password": "pass",
  "database": "mydb"
}
```

**Response:**
```json
{
  "success": true,
  "latencyMs": 250,
  "version": "Connection validated"
}
```

#### Register with OpenMetadata

Register a connector's data source in OpenMetadata for lineage tracking:

```bash
POST /api/v1/tenants/{tenant-id}/ingestion/connectors/{name}/register
```

## Resource Allocation

| Component | CPU Request | Memory Request | Storage |
|-----------|-------------|----------------|---------|
| Kafka | 25m | 300Mi | 5Gi |
| Kafka Connect | 50m | 256Mi | 1Gi |
| kafka-ui | 25m | 128Mi | - |
| Meltano | 25m | 128Mi | 1Gi |
| Metadata Sync | 25m | 64Mi | - |

## Example: Setting Up PostgreSQL CDC

1. **Configure source database** for logical replication:

```sql
-- On your PostgreSQL source
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_replication_slots = 4;
ALTER SYSTEM SET max_wal_senders = 4;
-- Restart PostgreSQL after these changes
```

2. **Create a replication user:**

```sql
CREATE ROLE replication_user WITH REPLICATION LOGIN PASSWORD 'secret';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO replication_user;
```

3. **Test the connection** via the API or dashboard

4. **Create the connector:**

```bash
POST /api/v1/tenants/{tenant-id}/ingestion/connectors
{
  "name": "postgres-orders",
  "type": "postgres",
  "config": {
    "database.hostname": "db.example.com",
    "database.port": "5432",
    "database.user": "replication_user",
    "database.password": "secret",
    "database.dbname": "orders",
    "table.include.list": "public.orders,public.order_items"
  }
}
```

5. **Monitor** via kafka-ui at `https://{tenant-id}.kafka.harbor.openlakes.io`

## Limitations (Trial Tier)

- Maximum 3 concurrent CDC connectors
- Maximum 10 Kafka topics
- 5Gi Kafka storage
- 1Gi Kafka Connect plugin storage
- 1Gi Meltano workspace storage

## Troubleshooting

### Connector Not Starting

1. Check connector status in kafka-ui
2. Verify source database credentials
3. Ensure network connectivity (source must be reachable from Harbor)
4. Check that required database permissions are granted

### Missing Data in MinIO

1. Verify the connector is in `RUNNING` state
2. Check Kafka topic for messages
3. Verify the S3 sink connector is configured correctly

### Meltano Extraction Fails

1. Check that the extractor is installed
2. Verify source credentials in the extractor config
3. Check Meltano logs for detailed error messages

## Related Documentation

- [Meltano & Singer](../core/components/meltano.md) - Detailed Meltano configuration
- [Debezium](../core/components/debezium.md) - Debezium CDC reference
- [Apache Kafka](../core/components/kafka.md) - Kafka configuration options
