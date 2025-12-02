# Apache Spark

## Overview

OpenLakes Core deploys Apache Spark as part of Layer 02 (compute). It ships a custom image (`ghcr.io/openlakes/core/spark:1.0.0`) with Iceberg, Nessie, S3A, and common lakehouse dependencies preinstalled so you can run ETL or interactive jobs immediately.

## Configuration in OpenLakes

- **Deployment model:** Spark master + worker StatefulSets exposed through ClusterIP services with Traefik ingress (`spark.<domain>`, `spark-worker.<domain>`). A history server is enabled by default.
- **Storage:** Event logs are written to `s3a://openlakes/spark-history/` in MinIO. Credentials are pulled from `core-config.yaml` (`credentials.minio.*`).
- **Iceberg/Nessie:** The image includes Iceberg + Nessie client libraries. Spark connects to the Nessie REST catalog hosted in Layer 01 so tables are versioned and shared with Trino.
- **Alluxio:** When the storage wizard detects multi-node deployments, Spark is automatically configured to use Alluxio as a caching tier. Single-node clusters keep the integration disabled.
- **Lineage:** The Spark image includes OpenLineage agent and OpenMetadata transporter for automatic lineage capture to OpenMetadata.

## Integration points

- Reads and writes Iceberg tables stored in MinIO via Nessie.
- Uses Kafka for structured streaming sources/sinks if required.
- Metrics and executor stats are scraped by the kube-prometheus stack, and the history server is reachable at `spark-history.<domain>`.

## License

Apache Spark is licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
