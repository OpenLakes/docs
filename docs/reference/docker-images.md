# Docker Images

OpenLakes builds and maintains custom Docker images for components requiring specialized dependencies.

## Image Registry

All OpenLakes images are published to GitHub Container Registry (GHCR):

```
ghcr.io/openlakes/core/<image-name>:<tag>
```

## Available Images

### spark

Custom Apache Spark image with lakehouse dependencies.

| Property | Value |
|----------|-------|
| Base | `apache/spark:4.1.0-preview3` |
| Registry | `ghcr.io/openlakes/core/spark` |
| Current Tag | `1.0.0` |

**Included Dependencies:**

- Apache Iceberg 1.8.0
- Nessie Client 0.77.1
- Hadoop AWS 3.4.1
- AWS SDK v2 Bundle 2.29.27
- OpenLineage Spark Agent 1.40.1
- OpenMetadata Transporter 1.35
- Alluxio Client 2.9.3

**Usage:**

```yaml
# In Helm values
spark:
  image:
    repository: ghcr.io/openlakes/core/spark
    tag: "1.0.0"
```

See [Spark Image Details](docker/spark.md) for build instructions.

---

### jupyterhub-notebook

Custom notebook environment for JupyterHub users.

| Property | Value |
|----------|-------|
| Base | `jupyter/base-notebook` |
| Registry | `ghcr.io/openlakes/core/jupyterhub-notebook` |
| Current Tag | `1.0.0` |

**Included Libraries:**

- PySpark 3.5.4
- Pandas, NumPy, scikit-learn
- Matplotlib, Plotly, Seaborn
- MinIO client
- Iceberg libraries
- Trino client

**Usage:**

```yaml
# In Helm values
jupyterhub:
  singleuser:
    image:
      name: ghcr.io/openlakes/core/jupyterhub-notebook
      tag: "1.0.0"
```

See [JupyterHub Notebook Details](docker/jupyterhub-notebook.md) for build instructions.

---

### openlakes-dashboard

React-based service portal for OpenLakes.

| Property | Value |
|----------|-------|
| Base | `node:18-alpine` (build) / `nginx:alpine` (runtime) |
| Registry | `ghcr.io/openlakes/core/openlakes-dashboard` |
| Current Tag | `1.3.0` |

**Features:**

- Material UI interface
- Service status cards
- Links to all component UIs
- Dark/light theme support
- Responsive design

**Usage:**

```yaml
# In Helm values
dashboard:
  image:
    repository: ghcr.io/openlakes/core/openlakes-dashboard
    tag: "1.3.0"
```

See [Dashboard Details](docker/openlakes-dashboard.md) for build instructions.

---

## Building Images

All images include a `build.sh` script supporting four environments:

```bash
cd docker/<image-name>

# Local build (Rancher Desktop)
./build.sh

# Dev build (push to GHCR dev tag)
BUILD_ENV=dev ./build.sh

# Staging build
BUILD_ENV=staging VERSION=1.0.0 ./build.sh

# Production build (via CI/CD)
BUILD_ENV=production VERSION=1.0.0 ./build.sh
```

## Image Tags

| Tag | Description | Pull Policy |
|-----|-------------|-------------|
| `local` | Local development builds | `Never` |
| `dev` | Collaborative development | `Always` |
| `staging` | Release candidates | `Always` |
| `1.0.0` | Production releases | `IfNotPresent` |
| `latest` | Latest production release | `Always` |

## OCI Labels

All images include standard OCI labels for traceability:

```dockerfile
LABEL org.opencontainers.image.title="..."
LABEL org.opencontainers.image.description="..."
LABEL org.opencontainers.image.version="${VERSION}"
LABEL org.opencontainers.image.source="https://github.com/openlakes-io/openlakes-core"
LABEL org.opencontainers.image.vendor="OpenLakes"
LABEL org.opencontainers.image.licenses="Apache-2.0"
```
