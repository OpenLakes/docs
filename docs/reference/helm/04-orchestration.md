# openlakes-orchestration

OpenLakes orchestration layer - Apache Airflow workflow management

## Chart Info

| Property | Value |
|----------|-------|
| Version | `1.0.0` |
| App Version | `3.x` |
| Type | application |
| Release Name | `04-orchestration` |

## Components

### Apache Airflow

| Component | Description |
|-----------|-------------|
| Scheduler | Monitors DAGs and triggers task instances |
| Webserver | Web UI for DAG management and monitoring |
| Database migrations | Job to initialize/upgrade Airflow metadata |

## Configuration

Key configuration values (see `values.yaml` for full reference):

```yaml
airflow:
  # Executor configuration
  executor: KubernetesExecutor

  # Web UI settings
  webserver:
    replicas: 1
    resources:
      requests:
        memory: "512Mi"
        cpu: "250m"

  # Scheduler settings
  scheduler:
    replicas: 1
    resources:
      requests:
        memory: "512Mi"
        cpu: "250m"

  # Database connection (uses shared PostgreSQL)
  postgresql:
    enabled: false  # Uses Layer 01 PostgreSQL

  # Extra pip packages for notebook execution
  extraPipPackages:
    - "apache-airflow-providers-papermill==3.8.1"
    - "apache-airflow-providers-apache-spark==5.4.0"
    - "pyspark==3.5.4"
    - "papermill==2.6.0"
    - "trino==0.328.0"
```

## Notebook Integration

Airflow syncs notebooks from MinIO for Papermill execution:

```yaml
notebookSync:
  enabled: true
  schedule: "*/3 * * * *"  # Every 3 minutes
  source: "s3://openlakes-notebooks/published/"
  destination: "/opt/airflow/dags/published"
```

## Ingress Routes

| Route | Host | Service |
|-------|------|---------|
| airflow | `airflow.<domain>` | Webserver UI |

## Metrics

StatsD integration exports metrics to Prometheus:

```yaml
statsd:
  enabled: true
  host: monitoring-statsd-exporter
  port: 9125
```

## Dependencies

- **PostgreSQL** (Layer 01): Metadata database
- **MinIO** (Layer 01): Notebook storage
- **Redis** (Layer 01): Optional result backend
