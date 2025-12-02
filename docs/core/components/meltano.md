# Meltano & Singer

## Overview

Layer 06 ships Meltano’s open-source UI backed by Singer taps/targets. OpenLakes Core embeds a curated `meltano.yml` plus a bootstrap ConfigMap that installs Singer plugins, provisions a PVC for virtualenv caches, and exposes the web UI behind Traefik (`meltano.<domain>`). All pipelines reuse the shared PostgreSQL/MinIO services from Layer 01.

## Configuration in OpenLakes

- **Image/version:** `meltano/meltano:v4.0.6` by default. The chart renders a single Deployment with a writable PVC so plugin installs persist across restarts.
- **Bootstrap content:** The `start.sh` script copies `meltano.yml` and `requirements.txt` from the ConfigMap on first boot, installs Singer plugins, and runs `meltano ui`.
- **Storage:** Uses the same MinIO credentials defined under `credentials.minio`. Buckets/prefixes can be overridden via the `meltano.s3` block in `core-config.yaml`.
- **Ingress:** A Traefik `IngressRoute` exposes the UI on `meltano.<domain>` (HTTP + HTTPS when `enableIngressRoutes` is true).
- **PVC:** `meltano.persistence.size` controls the per-cluster cache. Default is `10Gi` using the cluster’s storage class (`local-path` or `longhorn`).

## Integration points

- Singer taps can read from databases, APIs, or Kafka topics and land curated datasets into MinIO (`s3a://<bucket>/<prefix>`). The sample project demonstrates `tap-postgres` → `target-s3`.
- Meltano pairs well with Debezium: CDC events stream into Kafka/MinIO while Singer ELT jobs handle batch sources.
- OpenMetadata can register Meltano pipeline services for RBAC and documentation.
- Meltano can auto-generate Singer jobs when new systems are onboarded.

## License

Meltano and the Singer plugins referenced in `meltano.yml` are published under permissive licenses (primarily MIT/Apache 2.0). This aligns with OpenLakes’ requirement to avoid SaaS-restricted licenses.
