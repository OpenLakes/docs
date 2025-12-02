# openlakes-infrastructure

OpenLakes infrastructure layer - databases, storage, and messaging


## Chart Info

| Property | Value |
|----------|-------|
| Version | `1.0.0` |
| App Version | `N/A` |
| Type | application |

## Dependencies

| Chart | Version | Repository |
|-------|---------|------------|
| traefik | `33.2.1` | https://traefik.github.io/charts |
| alluxio | `0.6.54` | file://./charts/alluxio |

## Configuration

Key configuration values (see `values.yaml` for full reference):

```yaml
global:
  networking:
    type: ClusterIP  # Services use ClusterIP, exposed via Ingress resources
    ingressClass: traefik
    domain: openlakes.dev  # All services accessible via *.openlakes.dev
    enableIngressRoutes: true  # Create Traefik IngressRoutes for hostname routing
  storageClass: longhorn  # Default to Longhorn for distributed, replicated storage
  namespace: openlakes
  minioStorageClass: ""  # Empty so StatefulSet binds to pre-created hostPath PVs

# ═══════════════════════════════════════════════════════════════
# Longhorn - Distributed Block Storage
# ═══════════════════════════════════════════════════════════════
# Provides replicated persistent storage across all cluster nodes
# Enables stateful services (PostgreSQL, etc.) to run on any node with data accessibility
#
# Storage allocation: 150GB per node reserved for Longhorn
# Replication: Data replicated across nodes for high availability
# Excludes: Raspberry Pi nodes (insufficient resources)
longhorn:
  enabled: true

  # Node selection - exclude Raspberry Pi nodes from ALL components
  longhornManager:
    nodeSelector:
      kubernetes.io/arch: amd64  # Only run on x86_64 nodes (exclude ARM/RPi)

  longhornDriver:
    nodeSelector:
      kubernetes.io/arch: amd64

  longhornUI:
    nodeSelector:
      kubernetes.io/arch: amd64

  # Exclude RPi from CSI plugin DaemonSet
  csi:
    attacherReplicaCount: 2
    provisionerReplicaCount: 2
    resizerReplicaCount: 2
    snapshotterReplicaCount: 2
    nodeSelector:
      kubernetes.io/arch: amd64

  # Exclude RPi from engine image DaemonSet
  engineImage:
    nodeSelector:
      kubernetes.io/arch: amd64

  # Default settings
# ... (truncated)
```