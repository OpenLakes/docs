# Project Nessie

## Overview

Nessie serves as the Iceberg catalog in Layer 01. It stores table metadata in PostgreSQL and exposes a REST API so Spark and Trino can share the same versioned tables.

## Configuration in OpenLakes

- **Image/version:** `ghcr.io/projectnessie/nessie:latest`.
- **State store:** Uses the shared PostgreSQL instance (`nessie` database) with credentials defined in `layers/01-infrastructure/values.yaml`.
- **Service exposure:** ClusterIP service `infrastructure-nessie:19120` plus Traefik ingress at `https://nessie.<domain>`.
- **Readiness:** An init container waits for PostgreSQL before starting Nessie.

## Integration points

- Spark and Trino point to `http://infrastructure-nessie:19120/api/v2` as their Iceberg catalog.
- Meltano/Singer and Debezium pipelines write data into MinIO/Iceberg tables that are registered via Nessie.
- OpenMetadata can catalog Nessie-backed tables for lineage.

## License

Project Nessie is licensed under the [Apache License 2.0](https://github.com/projectnessie/nessie/blob/main/LICENSE).
