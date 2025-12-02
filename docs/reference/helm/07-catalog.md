# openlakes-catalog

OpenLakes Catalog Management Layer - OpenMetadata


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

openmetadata:
  # OpenMetadata is deployed as part of this layer chart
  # No separate Helm chart installation required
  enabled: true
  fullnameOverride: catalog-openmetadata

  service:
    type: ClusterIP  # Changed from NodePort - access via metadata.openlakes.dev
  # Ingress routing: metadata.openlakes.dev → OpenMetadata UI

  # Image Configuration
  image:
    repository: docker.getcollate.io/openmetadata/server
    tag: "1.10.7"
    pullPolicy: IfNotPresent

  resources:
    requests:
      cpu: "1"
      memory: 2Gi
    limits:
      cpu: "2"
      memory: 4Gi

  # Database Configuration (using existing OpenLakes PostgreSQL)
  database:
    host: infrastructure-postgres
    port: 5432
    databaseName: openmetadata
    username: openlakes
    password: openlakes123
    driverClass: org.postgresql.Driver
    dbScheme: postgresql
    dbParams: ""

  # OpenSearch/Elasticsearch Configuration (using OpenLakes OpenSearch)
  elasticsearch:
    enabled: true
    host: infrastructure-opensearch
    port: 9200
    scheme: http
# ... (truncated)
```