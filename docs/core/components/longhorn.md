# Longhorn

## Overview

Longhorn provides replicated block storage for stateful services. OpenLakes Core expects Longhorn to be installed before running the deployment script on multi-node clusters.

## Configuration in OpenLakes

- **Data path:** `/longhorn` on each node (configure via `storage.tiers.longhorn` in `core-config.yaml`).
- **Replica count:** Defaults to 2 replicas so volumes survive a single node failure.
- **Node labels:** Only nodes labeled with `node.longhorn.io/create-default-disk=true` will get default disks. The provided RKE2 manifest demonstrates how to set this up.
- **Ingress/UI:** An optional Traefik ingress exposes the Longhorn dashboard at `https://longhorn.<domain>`.

## Integration points

- PVCs for PostgreSQL, Redis, Kafka, OpenSearch, Superset, OpenMetadata, etc. all use the Longhorn StorageClass.
- The deployment script patches Longhorn node resources to add disks automatically for each `storage.tiers.longhorn` entry.
- When running single-node (Rancher Desktop), Longhorn is not required; the script falls back to `local-path`.

## License

Longhorn is licensed under the [Apache License 2.0](https://github.com/longhorn/longhorn/blob/master/LICENSE).
