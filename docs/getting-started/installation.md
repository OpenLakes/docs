# Installation Script (`deploy-openlakes.sh`)

OpenLakes Core ships with `deploy-openlakes.sh`, an idempotent wrapper around Helm that deploys every layer in order. This page summarizes how to use it effectively.

## Basic usage

```bash
./deploy-openlakes.sh
```

The script automatically:
- Detects your OS (macOS, Linux, WSL).
- Determines single-node vs multi-node mode.
- Applies `core-config.yaml` overrides and storage tiers.
- Installs Helm charts for layers 01 through 08, waiting for critical components (Postgres, MinIO, Trino, Spark, Superset, etc.) to become ready.

## Common flags

| Flag | Description |
|------|-------------|
| `--namespace <ns>` | Deploy into a custom namespace (default: `openlakes`). |
| `--timeout <dur>` | Max time for Helm operations (default: `5m`). |
| `--skip-throughput-check` | Skip iperf3 preflight on multi-node clusters (useful when you already validated networking). |
| `--dry-run` | Show planned actions without applying them. |

Environment variables can be used instead:
```bash
NAMESPACE=dev TIMEOUT=15m ./deploy-openlakes.sh
DRY_RUN=true ./deploy-openlakes.sh
```

Run `./deploy-openlakes.sh --help` for the full list of options.

## Workflow checklist

1. **Set kube context** to the cluster you’re targeting (e.g., `rancher-desktop` or `default` for RKE2).
2. **Edit `core-config.yaml`** to match your storage/domain requirements.
3. **Ensure prerequisites** (requirements, Longhorn, Traefik, directories) are in place.
4. **Run the script**. If it fails midway, fix the issue and rerun—the process is idempotent.
5. **Verify releases/pods** once complete:
   ```bash
   helm list -n openlakes
   kubectl get pods -n openlakes
   ```

## What the script deploys

| Layer | Components |
|-------|------------|
| 01 – Infrastructure | Traefik ingress (optional), distributed MinIO, PostgreSQL (shared database), Redis, Apache Kafka (KRaft), **Apicurio Schema Registry**, Project Nessie (Iceberg catalog), OpenSearch, infrastructure dashboard |
| 02 – Compute | Apache Spark (master, workers, history server) with S3A + Alluxio hooks, Trino 478 configured for the Nessie/Iceberg catalog |
| 03 – Streaming | Reserved for future engines (Spark Structured Streaming already available in Layer 02) |
| 04 – Orchestration | Apache Airflow (KubernetesExecutor) with StatsD integration and a MinIO-synced Papermill notebook registry |
| 05 – Analytics & Notebooks | Apache Superset and JupyterHub (including local example sync into the shared PVC) |
| 06 – Ingestion | Meltano UI + Singer pipelines (configured for shared PostgreSQL/MinIO) and Debezium Connect backed by infrastructure PostgreSQL/Kafka/MinIO |
| 07 – Catalog | OpenMetadata server plus Airflow-based ingestion workflows (cataloging services, bots, and connectors) |
| 08 – Monitoring | kube-prometheus-stack (Prometheus, Alertmanager, Grafana, node-exporter), StatsD exporter, and Loki for log aggregation |

Refer to the [Deployment Guide](../core/deployment.md) for detailed features (readiness checks, output format, troubleshooting) and advanced customization.

## Troubleshooting tips

- **Script errors:** Re-run the script after fixing the underlying issue; Helm will upgrade/install as needed.
- **Pods stuck pending:** Check `kubectl describe pod` for PVC or scheduling issues (often storage class misconfiguration).
- **Multi-node networking warnings:** The script warns if throughput < 1 Gbps or if Cilium isn’t detected. Address this before heavy workloads.
- **Longhorn prerequisites:** The script ensures the `longhorn` StorageClass exists on multi-node clusters; install Longhorn before running.

For deeper debugging, consult the logs of the affected component (e.g., `kubectl logs deployment/analytics-superset -n openlakes`) or the RKE2 service (`journalctl -u rke2-server -f`).
