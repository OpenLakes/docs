# Trino

## Overview

Trino is the interactive SQL engine in Layer 02. OpenLakes Core deploys Trino 478 (`trinodb/trino:478`) with a coordinator and worker(s) reachable at `https://trino.<domain>`.

## Configuration in OpenLakes

- **Catalogs:** By default, Trino is configured with an Iceberg catalog that talks to Project Nessie over HTTP and stores table data inside MinIO (`s3://openlakes/warehouse`). Additional catalogs for PostgreSQL and MinIO object storage are generated automatically.
- **Security:** UI access is proxied through Traefik. Authentication is open (demo friendly) but can be tightened by editing `layers/02-compute/values.yaml`.
- **Resources:** Coordinator and worker resources scale based on the detected resource profile. All pods use the global `storageClass` (Longhorn) for any local state.

## Integration points

- Shares catalogs with Spark through Nessie, so tables created by Spark show up instantly in Trino and vice versa.
- Superset uses Trino as its default SQL engine, so dashboards work without extra configuration.
- OpenMetadata can register Trino as a database service to capture metadata and lineage.

## License

Trino is licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
