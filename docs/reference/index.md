# Reference

Technical reference documentation for OpenLakes Core.

## Docker Images

Custom container images built for OpenLakes:

| Image | Registry | Description |
|-------|----------|-------------|
| [spark](docker/spark.md) | `ghcr.io/openlakes/core/spark` | Spark with Iceberg, Nessie, S3A, OpenLineage |
| [jupyterhub-notebook](docker/jupyterhub-notebook.md) | `ghcr.io/openlakes/core/jupyterhub-notebook` | Notebook environment with PySpark |
| [openlakes-dashboard](docker/openlakes-dashboard.md) | `ghcr.io/openlakes/core/openlakes-dashboard` | Service portal UI |

See [Docker Images](docker-images.md) for detailed documentation.

## Helm Charts

OpenLakes is organized into 8 deployment layers:

| Layer | Chart | Components |
|-------|-------|------------|
| 01 | [Infrastructure](helm/01-infrastructure.md) | PostgreSQL, MinIO, Kafka, Nessie, OpenSearch, Redis, Schema Registry, Traefik |
| 02 | [Compute](helm/02-compute.md) | Apache Spark, Trino |
| 03 | [Streaming](helm/03-streaming.md) | Reserved for future use |
| 04 | [Orchestration](helm/04-orchestration.md) | Apache Airflow |
| 05 | [Analytics](helm/05-analytics.md) | Apache Superset, JupyterHub |
| 06 | [Ingestion](helm/06-ingestion.md) | Meltano, Debezium |
| 07 | [Catalog](helm/07-catalog.md) | OpenMetadata |
| 08 | [Monitoring](helm/08-monitoring.md) | Prometheus, Grafana, Alertmanager, Loki |

See [Helm Charts](helm-charts.md) for detailed documentation.

## CLI Tools

- [deploy-openlakes.sh](cli.md#deploy-openlakessh) - Main deployment script
- [mark_notebook_ready.py](cli.md#mark_notebook_readypy) - Notebook publishing helper
- [configure-storage.sh](cli.md#configure-storagesh) - Storage configuration wizard

## Configuration

- [core-config.yaml](../getting-started/core-config.md) - Central configuration file
- [Environment Variables](configuration.md) - Runtime configuration
- [Secrets Management](configuration.md#secrets) - Credential handling

## Component Versions

Current versions deployed by OpenLakes Core 1.0.0:

| Component | Version | Image |
|-----------|---------|-------|
| Apache Spark | 4.1.0-preview3 | `ghcr.io/openlakes/core/spark:1.0.0` |
| Trino | 478 | `trinodb/trino:478` |
| Apache Kafka | 4.0.1 | `apache/kafka:4.0.1` |
| PostgreSQL | 15 | `postgres:15` |
| MinIO | latest | `minio/minio:latest` |
| Apache Airflow | 3.x | `apache/airflow:3.x` |
| Apache Superset | 4.1.1 | `apache/superset:4.1.1` |
| JupyterHub | 4.3.1 | `quay.io/jupyterhub/k8s-hub:4.3.1` |
| OpenMetadata | 1.10.7 | `docker.getcollate.io/openmetadata/server:1.10.7` |
| Project Nessie | 0.77.1 | `ghcr.io/projectnessie/nessie:0.77.1` |
| Debezium | 3.0.0.Final | `debezium/connect:3.0.0.Final` |
| Meltano | 4.0.6 | `meltano/meltano:v4.0.6` |
| Schema Registry | 2.6.13.Final | `quay.io/apicurio/apicurio-registry-kafkasql:2.6.13.Final` |
| OpenSearch | 2.11.0 | `opensearchproject/opensearch:2.11.0` |
| Redis | 7 | `redis:7-alpine` |
| Prometheus | 3.1.0 | `quay.io/prometheus/prometheus:v3.1.0` |
| Alertmanager | 0.28.0 | `quay.io/prometheus/alertmanager:v0.28.0` |
| Loki | 3.3.1 | `grafana/loki:3.x` |
