# Getting Started

OpenLakes Core is the complete data lakehouse platform you deploy into your Kubernetes cluster. This guide walks you through requirements, configuration, and deployment.

## Resource profiles & minimum footprints

`deploy-openlakes.sh` inspects the cluster and chooses an appropriate resource profile. Profiles determine how aggressively components are sized and how much capacity remains for user workloads. Anything below the hard floors fails fast rather than leaving you with a stuck control plane.

| Profile | Target platform(s) | Minimum CPU / RAM | Notes |
|---------|--------------------|-------------------|-------|
| `compact-vm` | Apple Silicon + virtualization (Rancher Desktop / Lima) | **10 vCPU / 18 GB RAM** | Hypervisor overhead leaves no headroom below this mark. The script exits if a VM advertises fewer resources. |
| `small` | Single-node Linux (bare metal or virtualization with direct storage) | **10 vCPU / 18 GB RAM** (24–32 GB recommended) | Uses the built-in `local-path` provisioner, skips Longhorn, and leaves headroom for notebooks, Spark, and Meltano jobs. |
| `full` | Homogeneous multi-node RKE2 + Longhorn | ≥24 cores / ≥128 GB aggregate (for example 3×8-core/32 GB) | Every worker must expose identical `/minio*`, `/alluxio`, `/longhorn`, `/cold-nas` mount points. Heterogeneous clusters are not supported. |

- **VM-only guardrail:** When running on virtualization the script enforces the 10 core / 18 GB floor even if Kubernetes reports lower numbers.
- **Headroom report:** At the end of each run the script prints how much CPU/RAM remains available for ETL, analytics queries, and user workloads.

## Deployment choices

- **Single-node (Rancher Desktop on Apple Silicon):** Targets the `rancher-desktop` context, uses `local-path` storage, and avoids Longhorn entirely. Ideal for demos and day-to-day development.
- **Multi-node (RKE2 on bare metal):** Requires homogeneous nodes and Longhorn. HostPath tiers (`/minio*`, `/alluxio`, `/longhorn`, `/cold-nas`) are read from `core-config.yaml`. Traefik terminates TLS via Cloudflare DNS-01 or falls back to HTTP/`/etc/hosts` if you prefer to stay offline.

Platform guides:
- [Single-Node (Rancher Desktop)](rancher-desktop.md)
- [Multi-Node (RKE2)](rke2.md)

## Storage tiering strategy

`core-config.yaml` tells the deploy script where every tier lives:

- **Hot tier (MinIO):** Map each node to NVMe/SSD paths such as `/minio1`, `/minio2`, `/minio3`, `/minio4`. The script creates hostPath-backed PVCs for each path and warns if capacities differ by more than ~5 %.
- **Cache tier (Alluxio):** Optional `/alluxio` mount. Enable Alluxio only when this directory exists and is fast.
- **Longhorn tier:** `/longhorn` mount per worker where Longhorn stores its data path. Required for every multi-node deployment.
- **Cold tier:** Optional `/cold-nas` mount for lifecycle tiering to NAS/NFS. If configured, MinIO lifecycle policies and the hot→cold→hot rehydration jobs are created automatically.

Ensure these directories exist, are empty, and remain mounted before running the script. If you wipe NVMe drives, rerun `deploy-openlakes.sh` and the PVCs will reattach (as long as `storage.longhorn.reclaimPolicy` remains `Retain`).

## Editing `core-config.yaml`

Treat `core-config.yaml` as the single source of truth. The script reads it on every run and applies overrides accordingly:

- **`cluster`** – namespace, mode (`auto|single|multi`), and which StorageClass to use per mode.
- **`networking`** – base domain, ingress class (`traefik`), whether to emit Traefik CRDs, and a NodePort fallback switch if you want to skip ingress.
- **`storage.tiers`** – the mount matrix described above.
- **`components`** – toggles optional services. Leave Schema Registry enabled (OpenMetadata requires it). Alluxio and `populateExamples` are optional. When `populateExamples` is true the deployment seeds the MinIO notebooks bucket from your clone and the platform mirrors it into JupyterHub/Airflow automatically.
- **`observability`** – surfaces every Loki/Prometheus/Grafana limit plus dedicated storage classes so you do not have to edit Helm values directly.
- **`meltano`, `spark`, `ingress`** – capture S3 credentials, Spark endpoints, and ingress preferences in one place.
- **`timeouts` / `resourceProfile`** – tune Helm timeouts and force a particular profile if you do not want auto-detection.

Example skeleton (replace credentials before deploying):

```yaml
cluster:
  mode: auto
  namespace: openlakes
  storageClass: longhorn
  singleNodeStorageClass: local-path

networking:
  domain: example.com
  ingressClass: traefik
  enableIngressRoutes: true

credentials:
  minio:
    rootUser: admin
    rootPassword: changeme
  postgres:
    user: openlakes
    password: changeme
    postgresPassword: supersecret
  superset:
    user: admin
    password: changeme
  airflow:
    user: admin
    password: changeme
  grafana:
    password: changeme
  openmetadata:
    user: admin@example.com
    password: changeme

storage:
  minio:
    precreateBuckets: true
    buckets:
      main: openlakes
      sparkHistoryPrefix: spark-history/
  tiers:
    hot:
      - node: worker-a
        paths: ["/minio1", "/minio2"]
      - node: worker-b
        paths: ["/minio3", "/minio4"]
    cache:
      - node: worker-a
        paths: ["/alluxio"]
    longhorn:
      - node: worker-a
        paths: ["/longhorn"]
      - node: worker-b
        paths: ["/longhorn"]
    cold:
      - node: worker-a
        paths: ["/cold-nas"]
  longhorn:
    enabled: true
    reclaimPolicy: Retain
```

## Networking & DNS options

- **Cloudflare DNS-01 (recommended):** Supply a token in the RKE2 Traefik HelmChartConfig so Let’s Encrypt issues certificates for `*.your-domain`. Whenever you edit files inside `/var/lib/rancher/rke2/server/manifests/`, follow `journalctl -u rke2-server -f` to confirm the helm controller reconciles the change (look for `DesiredSet` log lines).
- **No external DNS?** Add `/etc/hosts` entries pointing to your node IPs and access over HTTP. You can also provide your own self-signed certificates and reference them from Traefik values.

## What comes next

1. Validate that you meet the [Requirements & Profiles](prerequisites.md).
2. Follow the detailed instructions for your platform: [Rancher Desktop](rancher-desktop.md) or [RKE2](rke2.md).
3. Review the [installation script guide](installation.md) and [core-config breakdown](core-config.md) to understand every toggle.
4. Run `./deploy-openlakes.sh` from the `core` repository and watch the output for capacity summaries and ingress information.
5. Once Core is healthy, explore the services via the Dashboard, run notebooks in JupyterHub, and start building data pipelines with Airflow.
