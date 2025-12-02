# ingestion

OpenLakes ingestion layer – Meltano/Singer pipelines plus Debezium CDC.

## Chart Info

| Property | Value |
|----------|-------|
| Version | `1.0.0` |
| Type | application |

## Dependencies

None. The chart renders native Kubernetes manifests for Meltano, Debezium, and their PVCs/IngressRoutes.

## Key values

```yaml
global:
  networking:
    ingressClass: traefik
    domain: openlakes.dev
    enableIngressRoutes: true
  storageClass: longhorn
  namespace: openlakes
  minio:
    endpoint: http://infrastructure-minio:9000
    rootUser: admin
    rootPassword: admin123

meltano:
  enabled: true
  fullnameOverride: ingestion-meltano
  image:
    repository: meltano/meltano
    tag: v4.0.6
  service:
    port: 5000
  persistence:
    size: 10Gi         # PVC that caches plugins/virtualenvs
    storageClass: ""   # inherits from global.storageClass when empty
  s3:
    bucket: openlakes
    prefix: meltano/
  bootstrap:
    enabled: true
    meltanoYml: |-
      version: 1
      default_environment: dev
      project_id: openlakes
      environments:
        - name: dev
      plugins:
        extractors:
          - name: tap-postgres
            pip_url: tap-postgres
        loaders:
          - name: target-s3
            pip_url: git+https://github.com/transferwise/pipelinewise-target-s3.git
    requirementsTxt: |-
      tap-postgres==0.14.3
      pipelinewise-target-s3==1.1.3

debezium:
  enabled: true
  fullnameOverride: ingestion-debezium
  image:
    repository: debezium/connect
    tag: 3.0.0.Final
  kafka:
    host: infrastructure-kafka
    port: 9092
  service:
    type: ClusterIP
```

## Highlights

- **Meltano bootstrap ConfigMap** – seeds `meltano.yml` and `requirements.txt`, then executes `meltano install` before launching the UI. The PVC ensures plugin installs persist across restarts.
- **Ingress** – Traefik `IngressRoute` objects expose `meltano.<domain>` and `debezium.<domain>` over HTTP/HTTPS.
- **Shared credentials** – Meltano inherits MinIO credentials directly from `global.minio`, keeping secrets centralized in `core-config.yaml`.
- **Debezium** – Runs a single Connect pod with built-in wait-for-Kafka init container.

See `layers/06-ingestion/values.yaml` for every available override.
