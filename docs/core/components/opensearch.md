# OpenSearch

## Overview

OpenSearch is deployed in Layer 01 to support search and analytics use cases (OpenMetadata’s backend, log demos, and future observability workloads).

## Configuration in OpenLakes

- **Image/version:** `opensearchproject/opensearch:2.11.0`.
- **Mode:** Single-node cluster (discovery type `single-node`) with security disabled for simplicity.
- **Storage:** Longhorn PVC sized via `opensearch.storageSize` (default 10 GiB).
- **Java opts & resources:** Tuned via `deploy-openlakes.sh` based on the resource profile.
- **Ingress:** ClusterIP service (`infrastructure-opensearch`) plus Traefik routing as needed.

## Integration points

- OpenMetadata uses OpenSearch for indexing metadata and powering the search UI.
- Example notebooks demonstrate writing log events to OpenSearch for analytics.
- Future log ingestion pipelines (Loki → OpenSearch) can reuse the same endpoint.

## License

OpenSearch is licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
