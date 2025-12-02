# Configuration

This page documents configuration options for OpenLakes Core.

## core-config.yaml

The central configuration file that drives all deployments. See [core-config.yaml reference](../getting-started/core-config.md) for the complete schema.

## Default Credentials {#credentials}

OpenLakes ships with development credentials for quick deployment. **Change these for production use.**

| Service | Username | Password | Configuration Key |
|---------|----------|----------|-------------------|
| MinIO | admin | admin123 | `credentials.minio.rootUser/rootPassword` |
| PostgreSQL | openlakes | openlakes123 | `credentials.postgres.user/password` |
| Superset | admin | admin123 | `credentials.superset.user/password` |
| Airflow | admin | admin123 | `credentials.airflow.user/password` |
| Grafana | admin | admin123 | `credentials.grafana.password` |
| OpenMetadata | admin@open-metadata.org | admin | `credentials.openmetadata.user/password` |
| JupyterHub | admin | openlakes | Configured in Layer 05 values |

## Secrets Management {#secrets}

For production deployments, use Kubernetes secrets instead of inline credentials:

### Creating Secrets

```bash
# Create MinIO secret
kubectl create secret generic openlakes-minio \
  --from-literal=rootUser=your-user \
  --from-literal=rootPassword=your-secure-password \
  -n openlakes

# Create PostgreSQL secret
kubectl create secret generic openlakes-postgres \
  --from-literal=user=your-user \
  --from-literal=password=your-secure-password \
  --from-literal=postgresPassword=your-admin-password \
  -n openlakes
```

### Referencing Secrets

Update `core-config.yaml` to reference secrets:

```yaml
credentials:
  minio:
    secretName: openlakes-minio  # Use secret instead of inline values
    rootUser: ""                  # Ignored when secretName is set
    rootPassword: ""
  postgres:
    secretName: openlakes-postgres
    user: ""
    password: ""
```

### Rotating Credentials

1. Update the Kubernetes secret
2. Re-run `./deploy-openlakes.sh`
3. Helm automatically rolls dependent pods

## Environment Variables {#environment}

The deployment script accepts these environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `NAMESPACE` | `openlakes` | Target Kubernetes namespace |
| `TIMEOUT` | `5m` | Helm operation timeout |
| `DRY_RUN` | `false` | Preview without deploying |
| `ENV` | `production` | Environment: local, dev, staging, production |
| `KUBECONFIG` | `~/.kube/config` | Kubernetes config file |

## Configuration Overrides {#overrides}

Settings in `core-config.yaml` always override layer-specific `values.yaml` files. Use this to customize without editing charts:

```yaml
# core-config.yaml

# Override storage class globally
cluster:
  storageClass: my-custom-storage

# Override networking
networking:
  domain: mycompany.com
  ingressClass: nginx

# Toggle components
components:
  schemaRegistry: true
  alluxio: false
  populateExamples: true
```

## Resource Profiles

The deployment script auto-detects cluster capacity and applies appropriate resource profiles:

| Profile | Target | CPU | Memory |
|---------|--------|-----|--------|
| `compact-vm` | Apple Silicon VMs | 10+ cores | 18+ GB |
| `small` | Single-node Linux | 10+ cores | 18+ GB |
| `full` | Multi-node RKE2 | 24+ cores | 128+ GB |

Force a specific profile:

```yaml
# core-config.yaml
resourceProfile: small  # Override auto-detection
```

## Storage Configuration

### Tiers

```yaml
storage:
  tiers:
    hot:
      - node: worker-1
        paths: ["/minio1", "/minio2"]
    cache:
      - node: worker-1
        paths: ["/alluxio"]
    longhorn:
      - node: worker-1
        paths: ["/longhorn"]
    cold:
      - node: worker-1
        paths: ["/cold-nas"]
```

### MinIO Buckets

```yaml
storage:
  minio:
    precreateBuckets: true
    buckets:
      main: openlakes
      sparkHistoryPrefix: spark-history/
```

## Observability Settings

```yaml
observability:
  loki:
    limits:
      maxStreamsPerUser: 20000
      retentionPeriod: 168h  # 7 days
    storageClass: ""  # Use default

  prometheus:
    storageClass: ""

  grafana:
    storageClass: ""
```

## Networking

```yaml
networking:
  domain: openlakes.dev
  ingressClass: traefik
  enableIngressRoutes: true
  useNodePortFallback: false

ingress:
  enabled: true
  tls: false  # Set to true for production with certificates
```
