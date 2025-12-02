# Architecture

OpenLakes Core wires together storage, compute, catalog, orchestration, ingestion, and monitoring components so each service knows how to talk to the others out of the box. This section explains how the pieces fit.

## Storage & networking

- **Longhorn** provides replicated block storage for stateful workloads (PostgreSQL, Redis, Kafka, OpenSearch, Superset metadata, etc.).
- **MinIO** runs in distributed mode across the hot-tier mount points defined in `core-config.yaml`. Spark, Trino, Meltano/Singer, and Debezium all write to the `openlakes` bucket. Optional `/cold-nas` mounts enable lifecycle tiering.
- **Traefik** exposes every UI (`*.openlakes.dev`) and terminates TLS (either via Cloudflare DNS-01 or user-provided certificates).
- **Project Nessie** stores Iceberg table metadata in PostgreSQL and exposes a REST catalog consumed by Spark and Trino.

## Compute plane

- **Apache Spark** uses the custom `ghcr.io/openlakes/core/spark` image with Iceberg/S3A dependencies preinstalled. Executors write event logs to `s3a://openlakes/spark-history/` and can leverage Alluxio when multi-node caching is enabled.
- **Trino** runs the `trinodb/trino:478` image with an Iceberg catalog configured for Nessie/MinIO, plus built-in catalogs for PostgreSQL and MinIO object storage. Both engines are exposed through Traefik (`spark.<domain>`, `trino.<domain>`).

## Data services & catalog

- **Apache Kafka** (single KRaft broker) captures CDC streams from Debezium and is paired with the Apicurio Registry for Avro/JSON/Protobuf compatibility.
- **Meltano & Singer** orchestrate batch connectors. Both Meltano and Debezium use the shared PostgreSQL + MinIO credentials from `core-config.yaml`.
- **OpenMetadata** is backed by PostgreSQL and OpenSearch. Layer 07 also deploys the OpenMetadata ingestion Airflow deployment so you can schedule metadata syncs. Spark includes OpenLineage hooks for lineage capture.

## Orchestration & analytics

- **Apache Airflow** (Layer 04) runs with the KubernetesExecutor and a StatsD sidecar that feeds metrics to Prometheus/Grafana. A CronJob mirrors the MinIO `published/` prefix into `/opt/airflow/dags/published` so Papermill DAGs always execute the latest versioned notebooks.
- **JupyterHub** (Layer 05) provides multi-user notebooks using KubernetesSpawner. The shared `analytics-jupyter-examples` PVC is populated by mirroring the MinIO `shared/` prefix, and the `mark_notebook_ready.py` helper uploads author-approved notebooks back into the bucket for Airflow to consume.
- **Apache Superset** connects to Trino by default and can reach MinIO-hosted Iceberg tables without manual configuration.

## Monitoring & logging

- **kube-prometheus-stack** (Prometheus, Alertmanager, Grafana, node-exporter) scrapes every service, including Traefik, Spark, Trino, Kafka, Airflow, and the controllers in the `monitoring-statsd-exporter`.
- **Loki** is deployed in single-binary mode with persistent storage so you can centralize component logs.
- Dashboards included with the repo cover Airflow orchestration, Spark executors, Trino queries, Kafka broker health, MinIO throughput, and more.

## Data flow summary

1. **Ingest** data with Meltano/Singer (batch to MinIO `raw/`) or Debezium (CDC into Kafka). Use Kafka for low-latency streaming jobs and MinIO for durable raw history so you get the best of both worlds.
2. **Process** workloads by developing Spark batch or structured streaming jobs inside JupyterHub notebooks. Once the logic is validated (writing Iceberg tables managed by Nessie), run `scripts/mark_notebook_ready.py` to version the notebook in MinIO and let Airflow automatically discover it as a Papermill DAG.
3. **Catalog** datasets via OpenMetadata so downstream users discover assets and manage governance.
4. **Analyze** data through JupyterHub (ad hoc notebooks) and Superset (dashboards), both of which connect to Trino/Spark-backed tables.
5. **Observe** everything via Prometheus/Grafana dashboards and Loki logs to keep the platform healthy.

This architecture keeps each layer composable: storage and ingress are isolated in Layer 01, compute engines in Layer 02, orchestration and analytics in Layers 04–05, ingestion in Layer 06, cataloging in Layer 07, and monitoring in Layer 08.
