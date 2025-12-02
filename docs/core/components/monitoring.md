# Monitoring Stack

## Overview

Layer 08 installs a turnkey observability suite so you can monitor every OpenLakes component out of the box.

## What's included

- **kube-prometheus-stack:** Prometheus, Alertmanager, Grafana, and node-exporter.
- **StatsD exporter:** Collects Airflow metrics and exposes them to Prometheus.
- **Loki:** Centralized log storage (single binary) with persistent storage on Longhorn.
- **Grafana dashboards:** Preloaded dashboards for Airflow, Spark, Trino, Kafka, MinIO, and cluster health.

## Configuration in OpenLakes

- Services deploy into the `openlakes` namespace. Storage classes default to the global Longhorn setting.
- Grafana credentials come from `core-config.yaml` (`credentials.grafana.password`).
- Loki ships with a single replica and stores chunks in a Longhorn PVC.

## Integration points

- Airflow emits StatsD metrics to `monitoring-statsd-exporter`.
- Prometheus scrapes Traefik, Spark, Trino, Kafka, MinIO, OpenMetadata, etc.
- Grafana dashboards provide ready-made views for each layer.
- Loki can ingest application logs (Spark history server, Meltano, Debezium) for troubleshooting.

## License

- kube-prometheus-stack is licensed under the [Apache License 2.0](https://github.com/prometheus-operator/kube-prometheus/blob/main/LICENSE).
- Grafana is licensed under the [AGPL v3](https://grafana.com/licensing/).
- Loki is licensed under the [AGPL v3](https://github.com/grafana/loki/blob/main/LICENSE).
