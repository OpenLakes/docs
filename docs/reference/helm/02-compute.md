# openlakes-compute

OpenLakes compute layer - query engines (Trino, StarRocks)


## Chart Info

| Property | Value |
|----------|-------|
| Version | `1.0.0` |
| App Version | `N/A` |
| Type | application |

## Configuration

Key configuration values (see `values.yaml` for full reference):

```yaml
global:
  networking:
    type: ClusterIP  # Service type (ClusterIP + Ingress for external access via Traefik)
    ingressClass: traefik
    domain: openlakes.dev
    enableIngressRoutes: true  # Create Traefik IngressRoutes for hostname routing
  storageClass: longhorn
  namespace: openlakes

trino:
  enabled: true
  fullnameOverride: compute-trino
  image:
    repository: trinodb/trino
    tag: "478"
  coordinator:
    serviceType: ClusterIP  # Changed from NodePort - access via trino.openlakes.dev
  # Ingress routing: trino.openlakes.dev → Trino UI
#    limits:

starrocks:
  enabled: false
  fullnameOverride: compute-starrocks
  fe:
    image:
      repository: starrocks/fe-ubuntu
      tag: "3.5-latest"
    replicas: 1
    serviceType: ClusterIP  # Changed from NodePort - access via starrocks.openlakes.dev
  # Ingress routing: starrocks.openlakes.dev → StarRocks FE UI
#      limits:
    persistence:
      size: 10Gi
  be:
    image:
      repository: starrocks/be-ubuntu
      tag: "3.5-latest"
    replicas: 1
#      limits:
    persistence:
      size: 50Gi

# Apache Spark Configuration
spark:
  enabled: true
  fullnameOverride: compute-spark

  # Image configuration
  # OpenLakes Core uses custom Spark image with Iceberg, Nessie, S3A, and OpenMetadata support
  image:
# ... (truncated)
```