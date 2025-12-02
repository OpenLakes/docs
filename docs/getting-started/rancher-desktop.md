# Single-Node (Rancher Desktop, Apple Silicon)

Use this path for a local single-node cluster on macOS/Apple Silicon (Rancher Desktop/Lima). It targets the `rancher-desktop` kube context, uses `local-path` storage, and avoids Longhorn.

## Minimum footprint
- 10 vCPU, 18 GB RAM (compact-vm). More memory (24–32 GB) improves stability.
- Kubernetes v1.25+ with Traefik enabled (default in Rancher Desktop).

## Steps
1. Enable Kubernetes in Rancher Desktop (Kubernetes v1.25+ is bundled) and select the `rancher-desktop` context: `kubectl config use-context rancher-desktop`. The built-in `local-path` provisioner is sufficient—no Longhorn installation needed.
2. Clone the repo and review `core-config.yaml` (see [core-config breakdown](core-config.md)). Key items:
   - `cluster.mode: single`
   - `cluster.singleNodeStorageClass: local-path`
   - `networking.ingressClass: traefik`
   - `components.schemaRegistry: true` (Apicurio is required by OpenMetadata)
   - Storage tiers are ignored on single-node; the `local-path` provisioner handles PVCs.
3. If you want DNS+TLS, point `*.openlakes.dev` (or your domain) at the host IP and configure Cloudflare DNS-01 in Traefik. Otherwise add `/etc/hosts` entries and access via HTTP or your own self-signed certificates.
4. Run the deploy script from the `core` repo (full deployment checklist):
   ```bash
   git clone https://github.com/openlakes-io/openlakes-core.git
   cd openlakes-core/core
   ./deploy-openlakes.sh --skip-throughput-check
   ```
   The script will auto-detect single-node mode, use `local-path`, and skip Longhorn/iperf.
5. Verify pods: `kubectl -n openlakes get pods`. IngressRoutes are created for `*.openlakes.dev` (or your domain).

## Access without Cloudflare
- Add `/etc/hosts` entries for `minio.<your-domain>`, `superset.<your-domain>`, `airflow.<your-domain>`, `jupyter.<your-domain>`, `meltano.<your-domain>`, `schema-registry.<your-domain>`, etc. pointing to the host IP.
- Use HTTP or self-signed certs; Traefik can serve HTTP only if TLS is not desired.
