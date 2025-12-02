# MinIO

## Overview

MinIO provides S3-compatible object storage for all data lake workloads. OpenLakes Core deploys MinIO in distributed mode with erasure coding so it can tolerate drive or pod failures.

## Configuration in OpenLakes

- **Topology:** Four MinIO pods are scheduled across the hosts listed under `storage.tiers.hot` in `core-config.yaml`. Each path (e.g., `/minio1`, `/minio2`, …) is converted into a hostPath PV and bound to the StatefulSet.
- **Credentials:** The root user/password values are sourced from `core-config.yaml` (`credentials.minio.*`). Buckets for `openlakes`, the Spark history prefix, and the dedicated `notebooks` bucket (with versioning enabled) are created automatically.
- **Cold tier:** `/cold-nas` is mounted if present so you can set up lifecycle rules or future tiering jobs.
- **Ingress:** The console/API are published through Traefik at `https://minio.<domain>`.

## Integration points

- Spark and Trino use MinIO for both data and the Iceberg warehouse (through Nessie).
- Meltano/Singer and Debezium write datasets and state into dedicated prefixes.
- Superset, Airflow, and other services store artifacts in MinIO when remote logging is enabled.

## License

MinIO is licensed under the [GNU AGPL v3](https://min.io/license).
