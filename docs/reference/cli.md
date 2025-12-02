# CLI Reference

## deploy-openlakes.sh

The main deployment script for OpenLakes Core. This idempotent script deploys all layers to your Kubernetes cluster.

### Synopsis

```bash
./deploy-openlakes.sh [OPTIONS]
```

### Options

| Option | Environment Variable | Description |
|--------|---------------------|-------------|
| `--dry-run` | `DRY_RUN=true` | Preview what would be deployed without making changes |
| `--namespace <name>` | `NAMESPACE=<name>` | Deploy to a custom namespace (default: `openlakes`) |
| `--timeout <duration>` | `TIMEOUT=<duration>` | Helm operation timeout (default: `5m`) |
| `--env <environment>` | `ENV=<environment>` | Target environment: `local`, `dev`, `staging`, `production` |
| `--skip-throughput-check` | - | Skip network throughput validation |
| `--help` | - | Display help message |

### Examples

```bash
# Deploy all layers with default settings
./deploy-openlakes.sh

# Preview deployment (dry-run)
./deploy-openlakes.sh --dry-run
# OR
DRY_RUN=true ./deploy-openlakes.sh

# Deploy to a custom namespace with extended timeout
./deploy-openlakes.sh --namespace production --timeout 15m
# OR
NAMESPACE=production TIMEOUT=15m ./deploy-openlakes.sh

# Deploy for local development (Rancher Desktop)
./deploy-openlakes.sh --env local

# Skip network throughput check
./deploy-openlakes.sh --skip-throughput-check
```

### Deployment Layers

The script deploys 8 layers in sequence:

| Layer | Chart Name | Release Name | Components |
|-------|------------|--------------|------------|
| 01 | openlakes-infrastructure | `01-infrastructure` | PostgreSQL, MinIO, Kafka, Nessie, OpenSearch, Redis, Schema Registry, Traefik, Dashboard |
| 02 | openlakes-compute | `02-compute` | Spark (master/worker/history), Trino |
| 03 | openlakes-streaming | `03-streaming` | Reserved for future streaming engines |
| 04 | openlakes-orchestration | `04-orchestration` | Airflow (scheduler/webserver) |
| 05 | openlakes-analytics | `05-analytics` | Superset, JupyterHub |
| 06 | ingestion | `ingestion` | Meltano, Debezium |
| 07 | openlakes-catalog | `catalog` | OpenMetadata server, OpenMetadata ingestion |
| 08 | openlakes-monitoring | `monitoring` | Prometheus, Grafana, Alertmanager, Loki, Promtail |

### Resource Readiness Checks

The script waits for critical services before proceeding:

- **Layer 01**: PostgreSQL, Kafka, MinIO StatefulSets
- **Layer 02**: Trino Deployment, Spark Worker StatefulSet
- **Layer 04**: Airflow scheduler/webserver
- **Layer 05**: Superset, JupyterHub
- **Layer 07**: OpenMetadata server

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error (missing prerequisites, cluster connection failed) |
| 2 | Resource validation failed (insufficient CPU/memory) |

### Cluster Detection

The script automatically detects your cluster configuration:

- **Single-node** (e.g., Rancher Desktop): Uses `local-path` storage, standalone MinIO
- **Multi-node** (e.g., RKE2): Uses Longhorn storage, distributed MinIO with erasure coding

### Output

After successful deployment, the script displays:

1. **Helm Releases**: All deployed releases with versions
2. **Pod Status**: Running pods with their readiness state
3. **Ingress Routes**: Service URLs via Traefik
4. **Capacity Summary**: Remaining CPU/memory for workloads

---

## mark_notebook_ready.py

Helper script for publishing Jupyter notebooks to Airflow via Papermill.

### Synopsis

```bash
python scripts/mark_notebook_ready.py [OPTIONS]
```

### Options

| Option | Description |
|--------|-------------|
| `--notebook <path>` | Path to the notebook file (required) |
| `--dag-id <id>` | Airflow DAG identifier (required) |
| `--schedule <cron>` | Cron schedule expression (required) |
| `--output-notebook <path>` | Output path for executed notebook |
| `--param <key>=<value>` | Papermill parameters (can be repeated) |
| `--disable` | Disable/pause the scheduled job |

### Example

```bash
python scripts/mark_notebook_ready.py \
  --notebook examples/01-batch-etl/01-basic-etl.ipynb \
  --dag-id noaa_batch_etl \
  --schedule "0 2 * * *" \
  --output-notebook /opt/openlakes/artifacts/noaa_batch_etl-{{ ds }}.ipynb \
  --param environment=production \
  --param execution_date={{ ds }}
```

### Workflow

1. Adds Papermill metadata to the notebook
2. Uploads notebook to MinIO `published/<dag_id>/` prefix
3. Airflow CronJob mirrors the prefix to `/opt/airflow/dags/published`
4. The `notebook_registry.py` DAG discovers and schedules execution

---

## configure-storage.sh

Interactive storage configuration wizard for multi-node deployments.

### Synopsis

```bash
./configure-storage.sh
```

### Workflow

1. **Detect node storage**: Queries all nodes for available capacity
2. **Configure hot tier**: Allocate NVMe storage for Alluxio caching
3. **Configure warm tier**: Set up distributed MinIO with erasure coding
4. **Configure cold tier**: Select archive storage (local, NFS, S3)
5. **Generate config**: Creates `/tmp/openlakes-storage-config.yaml`

### Integration

```bash
# Run wizard and deploy with generated config
./configure-storage.sh
./deploy-openlakes.sh --storage-config /tmp/openlakes-storage-config.yaml
```

See [Storage Architecture](../core/storage.md) for detailed configuration options.
