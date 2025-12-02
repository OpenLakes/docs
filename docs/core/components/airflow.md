# Apache Airflow

## Overview

Airflow powers orchestration in Layer 04. OpenLakes configures it with the KubernetesExecutor so DAGs run as pods and inherit the same credentials as the rest of the stack.

## Configuration in OpenLakes

- **Image/version:** `apache/airflow:3.1.2`.
- **Executor:** KubernetesExecutor with a pod template that includes Papermill and the libraries required for Spark/Trino connectivity.
- **Storage:** Logs and metadata are stored in the shared PostgreSQL database (`airflow` schema). A Longhorn PVC stores deployment-specific files, and a CronJob mirrors the MinIO `published/` prefix into `/opt/airflow/dags/published` so notebook-based DAGs stay in sync.
- **Ingress:** The web UI is available at `https://airflow.<domain>`.
- **Metrics:** StatsD integration points to the monitoring namespace so Prometheus/Grafana dashboards can visualize DAG timings, queue depth, etc.
- **Notebooks:** Airflow reads Papermill-ready notebooks from `/opt/airflow/dags/published`, which is populated by mirroring the MinIO notebooks bucket. Promoting a notebook from JupyterHub automatically makes it runnable in Airflow.

## Integration points

- Airflow connections for Spark, Trino, and MinIO are pre-created via the helper scripts in Layer 04.
- The OpenMetadata ingestion deployment (Layer 07) runs its own Airflow instance but shares the same base image and configuration patterns.
- StatsD metrics feed the monitoring stack, and Airflow logs can be collected via Loki if desired.

## License

Apache Airflow is licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
