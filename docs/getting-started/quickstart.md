# Quick Start

This quick start walks you through the minimum steps to deploy OpenLakes Core on either Rancher Desktop (single-node) or RKE2 (multi-node).

## 1. Prepare your cluster

- **Single-node (macOS/Apple Silicon):** Enable Kubernetes in Rancher Desktop and select the `rancher-desktop` context.
- **Multi-node (RKE2):** Install RKE2 with the manifests described in [Multi-Node (RKE2)](rke2.md), ensure Longhorn is healthy, and switch `kubectl` to the RKE2 context. When you edit files under `/var/lib/rancher/rke2/server/manifests/`, watch `journalctl -u rke2-server -f` for helm-controller messages so you know the manifests applied.

Verify connectivity:
```bash
kubectl cluster-info
```

## 2. Clone the repository

```bash
git clone https://github.com/OpenLakes/openlakes-core.git
cd openlakes-core/core
```

## 3. Review `core-config.yaml`

- Update credentials, storage tiers, and domains (see [core-config guide](core-config.md)).
- For single-node deployments, set `cluster.mode: single` (or leave `auto`) and keep `singleNodeStorageClass: local-path`.
- For multi-node, define every `/minio*`, `/alluxio`, `/longhorn`, and `/cold-nas` path under `storage.tiers`.
- Adjust `observability` storage classes/limits and `components.populateExamples` if you do not want JupyterHub to sync local notebooks.
- Leave the Schema Registry and Meltano toggles enabled—OpenMetadata relies on them for schema discovery.

## 4. Confirm prerequisites

- `kubectl` and `helm` installed and pointing to your cluster.
- Cluster resources meet the [requirements](prerequisites.md).
- (RKE2) Traefik/Longhorn manifests placed under `/var/lib/rancher/rke2/server/manifests/`.

## 5. Run the deployment script

```bash
./deploy-openlakes.sh --skip-throughput-check
```

Helpful options:
```bash
# Dry run
DRY_RUN=true ./deploy-openlakes.sh

# Set namespace/timeout
NAMESPACE=dev TIMEOUT=10m ./deploy-openlakes.sh
```

See [Installation Script](installation.md) for full flag references.

## 6. Verify the deployment

```bash
kubectl get pods -n openlakes
```

You should see pods for infrastructure, compute, orchestration, analytics, ingestion, and catalog layers.

## 7. Access the services

Configure DNS or `/etc/hosts` entries, then access services via their ingress URLs:

| Service | URL | Credentials |
|---------|-----|-------------|
| Dashboard | `https://dashboard.<domain>` | - |
| MinIO | `https://minio.<domain>` | admin / admin123 |
| JupyterHub | `https://jupyter.<domain>` | admin / openlakes |
| Superset | `https://superset.<domain>` | admin / admin123 |
| Airflow | `https://airflow.<domain>` | admin / admin123 |
| OpenMetadata | `https://metadata.<domain>` | admin@open-metadata.org / admin |
| Grafana | `https://grafana.<domain>` | admin / admin123 |
| Trino | `https://trino.<domain>` | - |
| Spark | `https://spark.<domain>` | - |

Replace `<domain>` with your configured domain (default: `openlakes.dev`).

## 8. First steps after deployment

1. **Open the Dashboard** at `https://dashboard.<domain>` to see all available services
2. **Log into JupyterHub** and run an example notebook from `/home/jovyan/examples`
3. **Explore Superset** to create your first dashboard using Trino as the data source
4. **Check Grafana** for cluster health and component metrics

## Next steps

- Follow platform-specific guides for [Rancher Desktop](rancher-desktop.md) or [RKE2](rke2.md) for detailed tuning.
- Explore the [Core Platform Reference](../core/index.md) for architecture, storage, and component-specific docs.
- Try the [Tutorials](../tutorials/index.md) for hands-on data pipeline examples.
