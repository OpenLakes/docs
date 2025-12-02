# Core Platform

OpenLakes Core provides the control plane for the entire lakehouse. It installs a consistent stack of open-source services, wired together with sane defaults so you can focus on data pipelines instead of infrastructure plumbing.

## Layered Architecture

| Layer | Description | Key Components |
|-------|-------------|----------------|
| **01 – Infrastructure** | Databases, storage, messaging, ingress | PostgreSQL 15, MinIO, Kafka 4.0, Nessie 0.77, OpenSearch 2.11, Redis 7, Apicurio Registry 2.6, Traefik |
| **02 – Compute** | Query and processing engines | Spark 4.1, Trino 478 |
| **03 – Streaming** | Reserved for future streaming engines | Spark Structured Streaming available in Layer 02 |
| **04 – Orchestration** | Workflow management | Airflow 3.x (KubernetesExecutor) |
| **05 – Analytics** | BI and notebooks | Superset 4.1, JupyterHub 4.3 |
| **06 – Ingestion** | Data connectors and CDC | Meltano 4.0, Debezium 3.0 |
| **07 – Catalog** | Metadata and governance | OpenMetadata 1.10 |
| **08 – Monitoring** | Observability stack | Prometheus 3.1, Grafana, Alertmanager, Loki 3.3 |

## Components

| Component | Description |
|-----------|-------------|
| [Apache Spark](components/spark.md) | Batch and streaming compute engine using the custom OpenLakes image with Iceberg/Nessie/S3A integrations. |
| [Trino](components/trino.md) | Interactive SQL engine pre-wired to the Nessie/Iceberg catalog and exposed via `trino.<domain>`. |
| [MinIO](components/minio.md) | Distributed S3-compatible storage across the hot-tier hostPaths; feeds Spark, Trino, Meltano/Singer, and Debezium. |
| [Apache Kafka & Schema Registry](components/kafka.md) | KRaft-based Kafka broker plus Apicurio Registry for CDC streams and connector workloads. |
| [Project Nessie](components/nessie.md) | REST catalog that stores Iceberg metadata in PostgreSQL and coordinates table versions. |
| [PostgreSQL & Redis](components/postgres-redis.md) | Shared metadata stores used by Airflow, Superset, Meltano, and OpenMetadata. |
| [OpenMetadata](components/openmetadata.md) | Catalog/governance platform backed by PostgreSQL + OpenSearch with built-in ingestion Airflow jobs. |
| [JupyterHub](components/jupyterhub.md) | Multi-user notebooks with KubernetesSpawner and MinIO-backed example sync. |
| [Apache Airflow](components/airflow.md) | Orchestration engine running with the KubernetesExecutor and StatsD instrumentation. |
| [Apache Superset](components/superset.md) | BI dashboarding layer configured to use Trino as its default SQL engine. |
| [Meltano & Singer](components/meltano.md) | ELT platform that uses Singer taps/targets and the shared PostgreSQL/MinIO services. |
| [Debezium](components/debezium.md) | Change Data Capture Connect cluster publishing to Kafka topics. |
| [OpenSearch](components/opensearch.md) | Search/index backend for OpenMetadata and log examples. |
| [Prometheus, Grafana, Alertmanager, StatsD, Loki](components/monitoring.md) | Observability stack deployed in Layer 08. |
| [Traefik](components/traefik.md) | Ingress controller providing TLS termination and routing for all `*.openlakes.dev` endpoints. |
| [Longhorn](components/longhorn.md) | Distributed block storage powering stateful workloads and PVCs. |

## Guides

- [Architecture](architecture.md) – How all components interact, from ingestion through analytics.
- [Storage Architecture](storage.md) – HostPath tiers, Longhorn, and MinIO layout.
- [Deployment Guide](deployment.md) – Deep dive into the `deploy-openlakes.sh` features.
- [Developer Guide](guide.md) – Contributing changes and building custom images.
