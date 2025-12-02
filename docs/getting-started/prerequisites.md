# Requirements & Profiles

Run through this checklist before launching the deployment script. Meeting these requirements upfront avoids half-configured clusters and makes reruns idempotent.

## Minimum resources

- **Apple Silicon (virtualized: Rancher Desktop / Lima)** – **10 vCPU / 18 GB RAM** minimum (`compact-vm` profile). Hypervisor overhead makes anything smaller unstable. The script refuses to run if a VM advertises fewer resources.
- **Linux single-node (bare metal or non-virtualized)** – **10 vCPU / 18 GB RAM** minimum (`small` profile). Provision 24–32 GB for better Spark/Meltano headroom.
- **Homogeneous multi-node (RKE2 + Longhorn)** – 3+ identical nodes. Target ≥24 cores & ≥128 GB aggregate (`full` profile). Mixing different CPU/RAM/disk sizes is not supported.

> **Hard stop:** VM scenarios that fall below 10 cores or 18 GB immediately exit rather than attempting a partial deployment.

## Platform & tooling

- Kubernetes v1.25+ (Rancher Desktop or RKE2).
- `kubectl` pointing at the correct context (`rancher-desktop` or your RKE2 context such as `default`).
- `helm` v3.10+.
- Cluster-admin rights to create namespaces, CRDs, RBAC, StorageClasses, PVs, and PVCs.

## Networking & ingress

- Traefik is the supported ingress class. For public TLS, configure Cloudflare DNS-01 tokens (see the RKE2 manifest examples). Prefer watching `journalctl -u rke2-server -f` whenever you edit `/var/lib/rancher/rke2/server/manifests/*.yaml` so you can confirm the `helm-controller` reconciliation.
- If you do **not** want Cloudflare/TLS: add `/etc/hosts` entries mapping `*.your-domain` to your node IPs and access services over HTTP or self-signed certificates.
- RKE2 nodes should have NetworkManager disabled (or the relevant interfaces marked unmanaged) and `multipathd` disabled/blacklisted so Longhorn can claim NVMe devices reliably.

## Storage layout

- **Single-node (Rancher Desktop):** Uses the built-in `local-path` provisioner—no Longhorn required.
- **Multi-node (RKE2):** Each worker must surface identical mount points:
  - `/minio1`, `/minio2`, `/minio3`, `/minio4` (hot MinIO shards; keep sizes within ~5 %)
  - `/alluxio` (optional cache tier)
  - `/longhorn` (Longhorn data path)
  - `/cold-nas` (optional NAS mount for cold tier)
- Update `storage.tiers` inside `core-config.yaml` to reflect these mount points before running the script. The bootstrap jobs verify capacities and fail if discrepancies exceed 5 %.

## Optional but recommended

- GPUs (automatically detected; GPU monitoring toggles live under `observability`).
- External DNS providers other than Cloudflare if you want to integrate a different ACME workflow.
- Dedicated storage classes for observability (Loki / Prometheus) if you do not want them on the default Longhorn class.

Once everything here is satisfied, move on to the platform-specific guides or the [Quick Start](quickstart.md) to launch the deployment.
