# OpenLakes Deployment Guide

## Quick Start

The `deploy-openlakes.sh` script provides an automated, idempotent way to deploy all OpenLakes layers to your Kubernetes cluster.

### Prerequisites

- `kubectl` configured and connected to your Kubernetes cluster
- `helm` (version 3.x)
- Sufficient cluster resources (minimum: 10 CPU cores, 18 GB RAM)
- **Networking (multi-node)**: Use a high-throughput CNI (Cilium recommended for RKE2) so inter-node bandwidth is >=1 Gbps. The deploy script will measure throughput during preflight and warn if it detects <1 Gbps.
- **Cluster flavor**: The script auto-detects single-node vs multi-node:
  - Single-node (e.g., Rancher Desktop): Skips the storage wizard, runs MinIO in standalone mode, and defaults to `local-path`.
  - Multi-node: Runs the storage wizard (unless skipped), keeps distributed MinIO/Longhorn paths, and enforces NFS client tooling on all nodes.

### Basic Usage

```bash
# Deploy all layers with default settings
./deploy-openlakes.sh

# Preview what would be deployed (dry-run)
./deploy-openlakes.sh --dry-run

# Deploy to a custom namespace
./deploy-openlakes.sh --namespace production

# Set custom timeout for Helm operations
./deploy-openlakes.sh --timeout 15m

# Show help
./deploy-openlakes.sh --help
```

### Environment Variables

You can also configure the deployment using environment variables:

```bash
# Deploy to 'dev' namespace with 15-minute timeout
NAMESPACE=dev TIMEOUT=15m ./deploy-openlakes.sh

# Dry run using environment variable
DRY_RUN=true ./deploy-openlakes.sh
```

## Features

### Idempotency

The script uses `helm upgrade --install`, making it safe to run multiple times:

- **First run**: Installs all layers
- **Subsequent runs**: Upgrades existing releases or installs missing ones
- **Partial failures**: Re-running will pick up where it left off

### Cross-Platform Compatibility

The script automatically detects your operating system and adapts:

- **macOS** (Darwin) - Full support
- **Linux** - Full support
- **WSL** - Full support (detected as Linux)

### Resource Readiness Checks

The script waits for critical services to be ready before proceeding:

**Layer 01 (Infrastructure)**:

- PostgreSQL StatefulSet
- Kafka StatefulSet
- MinIO StatefulSet

**Layer 02 (Compute)**:

- Trino Deployment
- Spark Worker StatefulSet

**Layer 05 (Analytics)**:

- Superset Deployment
- JupyterHub

**Layer 07 (Catalog)**:

- OpenMetadata server

### Progress Reporting

The script provides detailed, color-coded output:

- **Info**: General information and progress updates
- **Success**: Completed operations
- **Warning**: Non-critical issues (continues execution)
- **Error**: Critical failures (stops execution)

### Deployment Status

After successful deployment, the script displays:

1. **Helm Releases**: All deployed releases with versions
2. **Pod Status**: Running pods with their readiness state
3. **Ingress Routes**: Service URLs via Traefik
4. **Capacity Summary**: Remaining CPU/memory for workloads

## Deployment Layers

The script deploys 8 layers in sequence:

| Layer | Name | Release Name | Components |
|-------|------|--------------|------------|
| 01 | Infrastructure | `01-infrastructure` | PostgreSQL 15, MinIO, Kafka 4.0, Nessie, OpenSearch, Redis, Schema Registry (Apicurio), Traefik, Dashboard |
| 02 | Compute | `02-compute` | Spark 4.1 (master/worker/history), Trino 478 |
| 03 | Streaming | `03-streaming` | Reserved for future streaming engines |
| 04 | Orchestration | `04-orchestration` | Airflow 3.x (scheduler/webserver) |
| 05 | Analytics | `05-analytics` | Superset 4.1, JupyterHub 4.3 |
| 06 | Ingestion | `ingestion` | Meltano 4.0, Debezium 3.0 |
| 07 | Catalog | `catalog` | OpenMetadata 1.10 (server + ingestion) |
| 08 | Monitoring | `monitoring` | Prometheus 3.1, Grafana, Alertmanager, Loki 3.3, Promtail |

## Service Access

### Via Traefik Ingress (Recommended)

All services are accessible via hostname-based routing through Traefik:

| Service | URL | Default Credentials |
|---------|-----|---------------------|
| Dashboard | `https://dashboard.openlakes.dev` | - |
| MinIO Console | `https://minio.openlakes.dev` | admin / admin123 |
| Nessie | `https://nessie.openlakes.dev` | - |
| Schema Registry | `https://schema-registry.openlakes.dev` | - |
| Spark Master | `https://spark.openlakes.dev` | - |
| Spark History | `https://spark-history.openlakes.dev` | - |
| Trino | `https://trino.openlakes.dev` | - |
| Airflow | `https://airflow.openlakes.dev` | admin / admin123 |
| JupyterHub | `https://jupyter.openlakes.dev` | admin / openlakes |
| Superset | `https://superset.openlakes.dev` | admin / admin123 |
| Meltano | `https://meltano.openlakes.dev` | - |
| Debezium | `https://debezium.openlakes.dev` | - |
| OpenMetadata | `https://metadata.openlakes.dev` | admin@open-metadata.org / admin |
| Grafana | `https://grafana.openlakes.dev` | admin / admin123 |
| Prometheus | `https://prometheus.openlakes.dev` | - |
| Alertmanager | `https://alertmanager.openlakes.dev` | - |
| Traefik Dashboard | `https://traefik.openlakes.dev` | - |

**Note**: Replace `openlakes.dev` with your configured domain from `core-config.yaml`.

### DNS Configuration

For the ingress routes to work, configure DNS:

**Option 1: Cloudflare DNS-01 (Recommended for production)**

Configure Let's Encrypt certificates via Traefik's Cloudflare integration.

**Option 2: Local /etc/hosts**

Add entries pointing to your cluster node IP:

```bash
# Add to /etc/hosts (replace IP with your node)
192.168.1.10  dashboard.openlakes.dev minio.openlakes.dev nessie.openlakes.dev
192.168.1.10  spark.openlakes.dev trino.openlakes.dev airflow.openlakes.dev
192.168.1.10  jupyter.openlakes.dev superset.openlakes.dev metadata.openlakes.dev
192.168.1.10  grafana.openlakes.dev prometheus.openlakes.dev
```

## Troubleshooting

### Script fails with "Cannot connect to Kubernetes cluster"

Verify your Kubernetes cluster is running and accessible:

```bash
kubectl cluster-info
```

### Script fails with "helm not found"

Install Helm:

```bash
# macOS
brew install helm

# Linux
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
```

### Deployment hangs waiting for a service

The script will wait up to 5 minutes (300 seconds) for each critical service. If a service doesn't become ready:

1. Check pod logs: `kubectl logs -n openlakes <pod-name>`
2. Check events: `kubectl get events -n openlakes --sort-by='.lastTimestamp'`
3. The script will continue anyway after timeout

### Layer 06 (Ingestion) fails

Layer 06 deploys Meltano (Singer) plus Debezium:

1. Confirm the Meltano PVC is bound: `kubectl get pvc -n openlakes | grep meltano`
2. Tail Meltano logs to watch plugin installs: `kubectl logs -n openlakes deployment/ingestion-meltano -f`
3. Verify Debezium is healthy: `kubectl get pods -n openlakes | grep debezium`

### Pods stuck in Pending

Usually indicates storage issues:

```bash
# Check PVC status
kubectl get pvc -n openlakes

# Check PV availability
kubectl get pv

# Describe the pending pod
kubectl describe pod <pod-name> -n openlakes
```

### Re-running after partial failure

The script is idempotent. Simply re-run it:

```bash
./deploy-openlakes.sh
```

Helm will:

- Upgrade existing releases
- Install missing releases
- Skip completed deployments

## Advanced Usage

### Custom Values Files

To customize a layer's configuration:

1. Edit the layer's `values.yaml`:
   ```bash
   vim layers/01-infrastructure/values.yaml
   ```

2. Re-run the deployment:
   ```bash
   ./deploy-openlakes.sh
   ```

Helm will apply your changes as an upgrade.

### Deploying Individual Layers

To deploy a single layer manually:

```bash
# Example: Deploy only the catalog layer
helm upgrade --install catalog ./layers/07-catalog \
  --namespace openlakes \
  --timeout 10m \
  --wait
```

### Uninstalling

To remove all OpenLakes components:

```bash
# Uninstall all releases (reverse order)
helm uninstall -n openlakes monitoring
helm uninstall -n openlakes catalog
helm uninstall -n openlakes ingestion
helm uninstall -n openlakes 05-analytics
helm uninstall -n openlakes 04-orchestration
helm uninstall -n openlakes 03-streaming
helm uninstall -n openlakes 02-compute
helm uninstall -n openlakes 01-infrastructure

# Delete the namespace
kubectl delete namespace openlakes
```

## Performance Tuning

### Resource Limits

Each layer's `values.yaml` defines resource limits. Adjust based on your cluster capacity:

```yaml
resources:
  limits:
    cpu: "2"
    memory: "4Gi"
```

### Timeout Adjustment

For slower clusters, increase the timeout:

```bash
./deploy-openlakes.sh --timeout 20m
```

## Known Issues

### OpenMetadata Ingestion Restarts

The OpenMetadata ingestion pod may restart several times during initial setup while waiting for the server to become ready. This is expected behavior.

### Meltano First-Boot Plugin Install

The Meltano pod installs Singer plugins the first time it starts. During this phase the pod may restart once if the installation exceeds the readiness timeout. Watch the logs (`kubectl logs -n openlakes deployment/ingestion-meltano -f`) until you see `meltano install` complete; subsequent restarts will be fast because the PVC caches the virtual environment.

## Support

For issues or questions:

1. Check pod logs: `kubectl logs -n openlakes <pod-name>`
2. Check events: `kubectl get events -n openlakes`
3. Review Helm release status: `helm list -n openlakes`
4. Open an issue at: https://github.com/OpenLakes/openlakes-core/issues
