# openlakes-analytics

OpenLakes analytics layer - Superset and JupyterHub

## Chart Info

| Property | Value |
|----------|-------|
| Version | `1.0.0` |
| App Version | `N/A` |
| Type | application |
| Release Name | `05-analytics` |

## Components

### Apache Superset

| Component | Description |
|-----------|-------------|
| Superset | BI and visualization platform |
| Image | `apache/superset:4.1.1` |

### JupyterHub

| Component | Description |
|-----------|-------------|
| Hub | Notebook server |
| Proxy | HTTP proxy for notebook routing |
| User Scheduler | Kubernetes scheduler for user pods |
| Image Puller | Pre-pulls notebook images |

## Configuration

### Superset

```yaml
superset:
  enabled: true

  image:
    repository: apache/superset
    tag: "4.1.1"

  # Database connection (uses shared PostgreSQL)
  postgresql:
    enabled: false

  # Redis for caching (uses shared Redis)
  redis:
    enabled: false

  # Default database connection
  defaultDatabase:
    type: trino
    host: compute-trino
    port: 8080
    database: lakehouse
```

### JupyterHub

```yaml
jupyterhub:
  enabled: true

  hub:
    image:
      name: quay.io/jupyterhub/k8s-hub
      tag: "4.3.1"

    # Authentication (dummy for demos)
    config:
      Authenticator:
        admin_users:
          - admin
        allowed_users:
          - admin
      DummyAuthenticator:
        password: openlakes

  singleuser:
    image:
      name: ghcr.io/openlakes/core/jupyterhub-notebook
      tag: "1.0.0"

    storage:
      dynamic:
        storageClass: longhorn
        pvcNameTemplate: "jupyter-{username}"
        capacity: 10Gi
```

## Notebook Sync

Example notebooks are synced from MinIO:

```yaml
notebookSync:
  enabled: true
  schedule: "*/2 * * * *"  # Every 2 minutes
  source: "s3://openlakes-notebooks/shared/"
  destination: "/home/jovyan/examples"
```

## Ingress Routes

| Route | Host | Service |
|-------|------|---------|
| superset | `superset.<domain>` | Superset UI |
| jupyter | `jupyter.<domain>` | JupyterHub UI |

## Default Credentials

| Service | Username | Password |
|---------|----------|----------|
| Superset | admin | admin123 |
| JupyterHub | admin | openlakes |

## Dependencies

- **PostgreSQL** (Layer 01): Superset metadata
- **Redis** (Layer 01): Superset caching
- **Trino** (Layer 02): Default SQL engine for Superset
- **MinIO** (Layer 01): Notebook storage
- **Longhorn** (Layer 01): User notebook PVCs
