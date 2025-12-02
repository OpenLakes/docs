# Understanding `core-config.yaml`

`deploy-openlakes.sh` reads `core-config.yaml` every time it runs. Treat it as the authoritative source for cluster strategy, networking, storage tiers, and optional components. This page walks through each section so you know what to edit before deploying.

## `cluster`

```yaml
cluster:
  mode: auto            # auto|single|multi
  namespace: openlakes  # Kubernetes namespace for all releases
  storageClass: longhorn
  singleNodeStorageClass: local-path
```

- **mode** – `auto` examines node count. Override to `single` (Rancher Desktop) or `multi` (RKE2) if detection ever fails.
- **namespace** – every Helm release lands here. Change only if you intend to run multiple isolated environments.
- **storageClass** – default PVC class for multi-node deployments (Longhorn). `singleNodeStorageClass` is used whenever `mode=single`.

## `networking`

```yaml
networking:
  domain: openlakes.dev
  ingressClass: traefik
  enableIngressRoutes: true
  useNodePortFallback: false
```

- **domain** – base DNS suffix (`minio.<domain>`, `meltano.<domain>`, etc.). Point this at your own domain when not using `openlakes.dev`.
- **ingressClass** – Traefik by default. Keep it in sync with the ingress controller installed on your cluster.
- **enableIngressRoutes** – `true` emits Traefik CRDs (`IngressRoute`, `Middleware`). Set to `false` if you want to manage vanilla Kubernetes `Ingress` resources yourself.
- **useNodePortFallback** – enables NodePort services for every UI if you plan to bypass ingress entirely.

## `credentials`

Store non-production credentials here. Each block has a `secretName` override if you want to supply real secrets out-of-band:

- **MinIO** – `rootUser` / `rootPassword`.
- **PostgreSQL** – shared DB user plus the `postgres` superuser password.
- **Superset, Airflow, Grafana, OpenMetadata** – default admin logins.

Never commit production values—use environment-specific files or Kubernetes secrets instead.

## `storage`

```yaml
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
    cold:
      - node: worker-a
        paths: ["/cold-nas"]
    longhorn:
      - node: worker-a
        paths: ["/longhorn"]
      - node: worker-b
        paths: ["/longhorn"]
  longhorn:
    enabled: true
    reclaimPolicy: Retain
```

- **MinIO buckets** – names for the bootstrap job (hot bucket + Spark history prefix).
- **Tiers** – maps each node to its hostPath mount points. The deployment script creates PVCs for every entry. Keep capacities within ~5 % across nodes for the hot tier.
  - **hot** – NVMe/SSD paths for distributed MinIO.
  - **cache** – `/alluxio` mounts (only used when Alluxio is enabled).
  - **cold** – optional NAS path used for MinIO tiering/rehydration.
  - **longhorn** – mount where Longhorn stores its engine data.
- **longhorn.reclaimPolicy** – `Retain` keeps volumes around if you delete the namespace; switch to `Delete` if you want automatic cleanup (not recommended for production).

## `components`

```yaml
components:
  schemaRegistry: true
  alluxio: false
  gpuMonitoring: auto   # auto|true|false
  populateExamples: true
```

- **schemaRegistry** – must stay true. OpenMetadata depends on the Apicurio registry.
- **alluxio** – enable only if `/alluxio` mounts exist.
- **gpuMonitoring** – automatically enables NVIDIA/DCGM exporters when GPUs are detected; override to `false` if you do not want extra DaemonSets.
- **populateExamples** – when true, the deployment script seeds the MinIO notebooks bucket with the `examples/` directory from the repo (no external Git jobs). JupyterHub mounts the shared bucket read-only and Airflow mirrors it into `/opt/airflow/dags/published`. Set to `false` if you want an empty notebooks bucket.

## `notebooks`

```yaml
notebooks:
  bucket: openlakes-notebooks
  prefixes:
    shared: shared
    workspace: workspace
    published: published
    artifacts: artifacts
  sync:
    downloadSchedule: "*/2 * * * *"
    uploadSchedule: "*/5 * * * *"
    airflowSchedule: "*/3 * * * *"
  versioning: true
```

- **bucket** – dedicated MinIO bucket that stores every notebook revision. The infrastructure layer enables S3 versioning automatically.
- **prefixes** – logical folders for different lifecycles.
  - `shared` – curated notebooks that appear read-only in every JupyterHub user’s `/home/jovyan/examples`.
  - `workspace` – per-user notebooks uploaded via the `mark_notebook_ready.py` helper (uses the `JUPYTERHUB_USER` as part of the key).
  - `published` – notebooks that Airflow runs via Papermill. The CronJob in Layer 04 mirrors this prefix into `/opt/airflow/dags/published`.
  - `artifacts` – reserved for Papermill outputs if you want to collect result notebooks in object storage.
- **sync** – Cron expressions for the built-in MinIO mirror jobs:
  - `downloadSchedule` controls how often the analytics layer mirrors `shared/` from MinIO into the `analytics-jupyter-examples` PVC.
  - `uploadSchedule` is available if you later add a bidirectional sync job (defaults to every 5 minutes, but the current release uses direct uploads via the publishing helper).
  - `airflowSchedule` controls how often Airflow refreshes its `/opt/airflow/dags/published` folder from MinIO.
- **versioning** – when `true`, the infrastructure bootstrap job enables MinIO object versioning so every publish retains history.

## `observability`

Expose every Loki/Prometheus/Grafana knob in one place:

```yaml
observability:
  loki:
    limits:
      maxStreamsPerUser: 20000
      retentionPeriod: 168h
    storageClass: longhorn-fast
  promtail:
    inotify:
      maxUserInstances: 8192
      maxUserWatches: 524288
  grafana:
    storageClass: longhorn-slow
  prometheus:
    storageClass: longhorn-slow
```

Adjust these values instead of editing Helm charts directly. It keeps resource policies under source control.

## `spark`, `meltano`, `ingress`

- **`spark`** – S3 endpoint/credentials for Spark history and Iceberg jobs.
- **`meltano`** – optional overrides for the Meltano/Singer UI (bucket/prefix, feature toggles). Leave blank to use the defaults derived from the MinIO config.
- **`ingress`** – set `enabled: false` if you want the script to skip Traefik `IngressRoute` objects entirely.

## `timeouts` & `resourceProfile`

```yaml
timeouts:
  helm: 5m
  probes:
    profile: slow  # slow|medium|fast

resourceProfile: ""     # compact-vm|compact|small|full
```

- **timeouts.helm** – overrides the Helm wait timeout for each release.
- **timeouts.probes.profile** – coarse knob that adjusts readiness/liveness probes for sensitive services (Superset, Trino, Meltano, Apicurio, etc.).
- **resourceProfile** – leave empty to let the script auto-detect. Override only if you want to pin a specific profile regardless of available CPU/RAM.

---

**Tip:** Keep a sanitized copy of `core-config.yaml` in source control and inject real credentials via environment-specific overlays or Kubernetes secrets. The deploy script reuses this file across reruns, so consistent configuration makes redeployments deterministic.
