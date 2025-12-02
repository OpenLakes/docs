# OpenMetadata

## Overview

OpenMetadata powers the catalog, glossary, lineage, and governance features in Layer 07. Users access the UI at `https://metadata.<domain>`.

## Configuration in OpenLakes

- **Image/version:** `docker.getcollate.io/openmetadata/server:1.10.7`.
- **Backing stores:** PostgreSQL (`infrastructure-postgres`) stores metadata, while OpenSearch holds search indexes. Both are provisioned in Layer 01.
- **Ingestion:** An Airflow deployment (`catalog-openmetadata-ingestion`) is installed alongside the server so you can schedule metadata syncs without extra setup.
- **Authentication:** Basic auth with self-signup is enabled by default. Admin credentials are defined in `layers/07-catalog/values.yaml`.
- **Pipeline client:** The built-in Airflow REST client is configured, so lineage and metadata ingestion jobs can be triggered from the OpenMetadata UI.

## Integration points

- Spark and Trino services can be registered manually via the UI to capture lineage and metadata.
- Meltano/Singer and Debezium sources map to data products documented in the catalog.
- Nessie + Iceberg table metadata can be synchronized so downstream users discover tables and dashboards.

## License

OpenMetadata is licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
