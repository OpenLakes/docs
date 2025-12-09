# Multi-Node (RKE2)

Use this path for a homogeneous multi-node cluster on bare metal with RKE2 and Longhorn. **Mixing different CPU/memory/disk sizes is not supported.**

## Minimum footprint (homogeneous)
- 3+ nodes with the same CPU/RAM/disk layout.
- Aim for ≥24 cores and ≥128 GB aggregate for the full profile (e.g., 3×8 vCPU/32 GB+). The script still enforces a hard floor of 10 cores / 18 GB per node.
- Underlying disks per node sized within ~5% of each other.

## Required node mounts
Configure each node with the same mount points:
- **Hot MinIO object storage:** `/minio1`, `/minio2` on node A; `/minio3`, `/minio4` on node B. Each path backs a hostPath PV created by the deploy script based on `storage.tiers.hot`.
- **Cache:** `/alluxio`
- **Block storage for PVCs:** `/longhorn`
- **Optional cold tier/NAS:** `/cold-nas`

Longhorn disks are defined at `/longhorn` and must be consistent. MinIO paths must exist and be empty before deployment.

## RKE2 config (per node)
Example `/etc/rancher/rke2/config.yaml` (primary server shown). Worker nodes include a `server: https://<primary>:9345` line and omit the taints/labels unless you also want them control-plane-only.
```yaml
token: "<your-secure-token>"  # Generate with: openssl rand -hex 32

# primary-server is the initial server (no "server:" line here)
tls-san:
  - "192.168.1.10"        # primary server IP
  - "192.168.1.11"        # worker-1 IP
  - "192.168.1.12"        # worker-2 IP
  - "k8s.example.com"     # hostname you will use for API/kubectl

ingress-controller: traefik

# CNI + dual-stack CIDRs (must match on all servers)
cni: cilium
cluster-cidr: "10.42.0.0/16,fd00:10:42::/56"
service-cidr: "10.43.0.0/16,fd00:10:43::/112"

# Make primary server control-plane-only for workloads
node-label:
  - "openlakes.io/role=control-only"
node-taint:
  - "role=control-only:NoSchedule"
```

## Manifests (placed in `/var/lib/rancher/rke2/server/manifests/`)

### Traefik (helmchartconfig)
`/var/lib/rancher/rke2/server/manifests/traefik.yaml`
```yaml
apiVersion: helm.cattle.io/v1
kind: HelmChartConfig
metadata:
  name: rke2-traefik
  namespace: kube-system
spec:
  valuesContent: |-
    nodeSelector:
      kubernetes.io/hostname: primary-server  # Replace with your control-plane node hostname
    tolerations:
    - key: role
      operator: Equal
      value: control-only
      effect: NoSchedule

    deployment:
      kind: Deployment
      replicas: 1
      hostNetwork: true
      dnsPolicy: ClusterFirstWithHostNet

    daemonset:
      enabled: false

    service:
      type: ClusterIP

    ports:
      web:
        port: 80
        exposedPort: 80
        hostPort: 80
      websecure:
        port: 443
        exposedPort: 443
        hostPort: 443
        tls:
          enabled: true
          certResolver: letsencrypt
      traefik:
        port: 9000
        exposedPort: 9000

    additionalArguments:
    - --certificatesresolvers.letsencrypt.acme.dnschallenge=true
    - --certificatesresolvers.letsencrypt.acme.dnschallenge.provider=cloudflare
    - --certificatesresolvers.letsencrypt.acme.email=aleksarias@gmail.com
    - --certificatesresolvers.letsencrypt.acme.storage=/data/acme.json

    env:
    - name: CF_API_TOKEN
      value: "<CLOUDFLARE_API_TOKEN>"
    - name: CLOUDFLARE_DNS_API_TOKEN
      value: "<CLOUDFLARE_API_TOKEN>"
    - name: CLOUDFLARE_ZONE_API_TOKEN
      value: "<CLOUDFLARE_ZONE_TOKEN>"

    persistence:
      enabled: false

    podSecurityContext:
      runAsNonRoot: true
      runAsUser: 65532
      runAsGroup: 65532
      fsGroup: 65532
      fsGroupChangePolicy: "OnRootMismatch"

    securityContext:
      readOnlyRootFilesystem: false
      allowPrivilegeEscalation: false
      capabilities:
        drop: []
        add:
        - NET_BIND_SERVICE

    volumePermissions:
      enabled: false
```

### Traefik middleware
`/var/lib/rancher/rke2/server/manifests/traefik-middleware.yaml`
```yaml
apiVersion: traefik.io/v1alpha1
kind: Middleware
metadata:
  name: redirect-https
  namespace: openlakes
spec:
  redirectScheme:
    scheme: https
    permanent: true
```

> Replace the Cloudflare tokens with your own. After saving the file, run `journalctl -u rke2-server -f` and look for `helm-controller` logs referencing `rke2-traefik` to confirm the config applied.

### Longhorn (prerequisite)
`/var/lib/rancher/rke2/server/manifests/longhorn.yaml`
```yaml
apiVersion: helm.cattle.io/v1
kind: HelmChart
metadata:
  name: longhorn
  namespace: kube-system
spec:
  repo: https://charts.longhorn.io
  chart: longhorn
  version: 1.10.1
  targetNamespace: longhorn-system
  createNamespace: true
  valuesContent: |
    persistence:
      defaultClassReplicaCount: 2

    defaultSettings:
      defaultDataPath: "/longhorn"
      createDefaultDiskLabeledNodes: true
      defaultReplicaCount: 2
```

## Cluster prep
- Disable NetworkManager or ensure it does not manage your data interfaces:
  ```bash
  sudo systemctl disable --now NetworkManager
  ```
  (or configure `unmanaged-devices` for the NICs Longhorn/Cilium will use).
- Disable `multipathd` or blacklist the NVMe devices Longhorn will mount:
  ```bash
  sudo systemctl disable --now multipathd
  # or edit /etc/multipath.conf and add:
  blacklist {
    device {
      devnode "^nvme"
    }
  }
  ```
- Create and mount `/minio*`, `/alluxio`, `/longhorn`, `/cold-nas` on each node; ensure they are empty before deployment.
- Label/taint control-plane-only nodes as needed (e.g., primary-server with `role=control-only:NoSchedule`).
- Whenever you edit files under `/var/lib/rancher/rke2/server/manifests/`, stream `journalctl -u rke2-server -f` and wait for a line similar to `DesiredSet - Replace Wait ... helm-install-rke2-traefik` to verify the change reconciled.

## core-config.yaml (key fields)
- `cluster.mode: auto|single|multi` and `cluster.namespace`.
- `cluster.storageClass` (multi-node: `longhorn`); `singleNodeStorageClass` (`local-path`).
- `networking.domain`, `ingressClass` (Traefik), `enableIngressRoutes`.
- `storage.tiers`: set the host paths listed above for hot/cold/cache/longhorn.
- `components.schemaRegistry: true` (required for OpenMetadata), `populateExamples`, `alluxio` (optional).
- `observability.loki/prometheus/grafana`: storage classes and limits.

Review and adjust `core-config.yaml` before running the deploy script. See [core-config.yaml breakdown](core-config.md) for details.

## Deployment steps
1. Install RKE2 with the config above and place the Traefik/Longhorn manifests under `/var/lib/rancher/rke2/server/manifests/` **before starting rke2-server** (or restart the service after editing). Watch the RKE2 journal (`journalctl -u rke2-server -f`) to confirm each HelmChart is applied.
2. Verify the `longhorn` StorageClass exists and Traefik is running on the control-plane node.
3. Set your kube context to the RKE2 cluster (e.g., `default`).
4. From the `core` repo, run (deployment checklist):
   ```bash
   git clone https://github.com/OpenLakes/openlakes-core.git
   cd openlakes-core/core
   ./deploy-openlakes.sh --skip-throughput-check
   ```
   The script will detect multi-node mode, require the `longhorn` StorageClass, and create hostPath PVs for MinIO based on `core-config.yaml`.
5. Verify pods: `kubectl -n openlakes get pods`. IngressRoutes for `*.openlakes.dev` (or your domain) will be created.

## DNS and TLS options
- **Cloudflare (recommended for public HTTPS):** Point `*.openlakes.dev` (or your domain) at the ingress node IP (e.g., primary-server), and let Traefik issue ACME DNS-01 certs using the tokens in the manifest. Replace the placeholder tokens with your own.
- **Bring-your-own TLS / no Cloudflare:** Remove the ACME arguments/env from the Traefik manifest and:
  1. Create a Kubernetes secret with your certificate (self-signed or trusted):
     ```bash
     kubectl create secret tls openlakes-tls \
       --cert=path/to/fullchain.pem \
       --key=path/to/privkey.pem \
       -n openlakes
     ```
  2. Update the Traefik middleware/IngressRoute to reference the secret via `tls.secretName`.
  3. Update each `IngressRoute` to remove the `certResolver` and add `tls:\n  secretName: openlakes-tls`.
  4. Optionally add `/etc/hosts` entries pointing to the ingress node if you don’t have public DNS, or keep HTTP-only by omitting the TLS block entirely.
    defaultSettings:
      defaultDataPath: "/longhorn"
      createDefaultDiskLabeledNodes: true
      defaultReplicaCount: 2
```

If you do not want to use ACME/Cloudflare, remove the `additionalArguments`/`env` sections above and configure Traefik with your own TLS secret (see “DNS and TLS options” below).

> **Note:** RKE2 watches the manifests directory. After editing a manifest, RKE2 picks up the change automatically (watch `journalctl -u rke2-server -f` for lines such as `handler helm-controller-chart-registration: DesiredSet - Replace Wait ... helm-install-rke2-traefik`).

### Example worker config

`/etc/rancher/rke2/config.yaml` on worker nodes:
```yaml
server: https://192.168.1.10:9345
token: "<same token>"

cni: cilium
cluster-cidr: "10.42.0.0/16,fd00:10:42::/56"
service-cidr: "10.43.0.0/16,fd00:10:43::/112"

# Workers should not carry the control-only label/taint
node-label:
  - "openlakes.io/role=worker"
```
