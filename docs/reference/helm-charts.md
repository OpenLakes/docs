# Helm Charts

OpenLakes Core is organized into 8 Helm chart layers, deployed sequentially to build the complete data platform.

## Chart Overview

| Layer | Chart | Version | Description |
|-------|-------|---------|-------------|
| 01 | [openlakes-infrastructure](helm/01-infrastructure.md) | 1.0.0 | Databases, storage, messaging, ingress |
| 02 | [openlakes-compute](helm/02-compute.md) | 1.0.0 | Spark and Trino query engines |
| 03 | [openlakes-streaming](helm/03-streaming.md) | 1.0.0 | Reserved for streaming engines |
| 04 | [openlakes-orchestration](helm/04-orchestration.md) | 1.0.0 | Apache Airflow orchestration |
| 05 | [openlakes-analytics](helm/05-analytics.md) | 1.0.0 | Superset and JupyterHub |
| 06 | [ingestion](helm/06-ingestion.md) | 1.0.0 | Meltano and Debezium |
| 07 | [openlakes-catalog](helm/07-catalog.md) | 1.0.0 | OpenMetadata server and ingestion |
| 08 | [openlakes-monitoring](helm/08-monitoring.md) | 1.0.0 | Prometheus, Grafana, Loki stack |

## Deployment Order

Charts must be deployed in order due to dependencies:

```
01-infrastructure
       ↓
02-compute ←──────────────────┐
       ↓                      │
03-streaming                  │
       ↓                      │
04-orchestration ─────────────┤ (depends on PostgreSQL, MinIO)
       ↓                      │
05-analytics ─────────────────┤
       ↓                      │
06-ingestion ─────────────────┤
       ↓                      │
07-catalog ───────────────────┘
       ↓
08-monitoring
```

## Configuration

All charts read from `core-config.yaml` for centralized configuration:

```yaml
# Example: Override storage class for all charts
cluster:
  storageClass: longhorn

# Example: Configure credentials once
credentials:
  minio:
    rootUser: admin
    rootPassword: admin123
  postgres:
    user: openlakes
    password: openlakes123
```

## Environment-Specific Values

Each chart supports four environments via values file merging:

| Environment | Values Files | Use Case |
|-------------|--------------|----------|
| Production | `values.yaml` | Stable releases, version-pinned images |
| Local | `values.yaml` + `values-local.yaml` | Rancher Desktop, `imagePullPolicy: Never` |
| Dev | `values.yaml` + `values-dev.yaml` | Development builds, GHCR dev tags |
| Staging | `values.yaml` + `values-staging.yaml` | QA testing, release candidates |

Deploy with environment:

```bash
./deploy-openlakes.sh --env local
./deploy-openlakes.sh --env dev
./deploy-openlakes.sh --env staging
./deploy-openlakes.sh  # production (default)
```

## Manual Chart Deployment

To deploy individual charts:

```bash
# Deploy infrastructure layer
helm upgrade --install 01-infrastructure ./layers/01-infrastructure \
  --namespace openlakes \
  --create-namespace \
  --timeout 10m \
  --wait

# Deploy with environment overrides
helm upgrade --install 02-compute ./layers/02-compute \
  --namespace openlakes \
  -f ./layers/02-compute/values.yaml \
  -f ./layers/02-compute/values-local.yaml \
  --timeout 10m \
  --wait
```

## Chart Dependencies

### External Dependencies

| Chart | Dependency | Version | Repository |
|-------|------------|---------|------------|
| 01-infrastructure | traefik | 33.2.1 | traefik.github.io/charts |
| 01-infrastructure | alluxio | 0.6.54 | bundled |
| 05-analytics | jupyterhub | 4.3.1 | jupyterhub.github.io/helm-chart |
| 08-monitoring | kube-prometheus-stack | 68.2.2 | prometheus-community |
| 08-monitoring | loki | 3.3.1 | grafana.github.io/helm-charts |

### Internal Dependencies

All charts depend on Layer 01 infrastructure services:

- **PostgreSQL**: Used by Airflow, Superset, Meltano, OpenMetadata, Nessie
- **MinIO**: Used by Spark, Trino, Meltano, Debezium, JupyterHub
- **Kafka**: Used by Debezium, Spark Streaming
- **Redis**: Used by Superset caching

## Uninstalling

Remove all OpenLakes components:

```bash
# Uninstall in reverse order
helm uninstall -n openlakes monitoring
helm uninstall -n openlakes catalog
helm uninstall -n openlakes ingestion
helm uninstall -n openlakes 05-analytics
helm uninstall -n openlakes 04-orchestration
helm uninstall -n openlakes 03-streaming
helm uninstall -n openlakes 02-compute
helm uninstall -n openlakes 01-infrastructure

# Delete namespace and PVCs
kubectl delete namespace openlakes
```

## Detailed Chart Documentation

- [Layer 01: Infrastructure](helm/01-infrastructure.md) - PostgreSQL, MinIO, Kafka, Nessie, OpenSearch, Redis, Schema Registry, Traefik
- [Layer 02: Compute](helm/02-compute.md) - Apache Spark, Trino
- [Layer 03: Streaming](helm/03-streaming.md) - Reserved
- [Layer 04: Orchestration](helm/04-orchestration.md) - Apache Airflow
- [Layer 05: Analytics](helm/05-analytics.md) - Apache Superset, JupyterHub
- [Layer 06: Ingestion](helm/06-ingestion.md) - Meltano, Debezium
- [Layer 07: Catalog](helm/07-catalog.md) - OpenMetadata
- [Layer 08: Monitoring](helm/08-monitoring.md) - Prometheus, Grafana, Loki
