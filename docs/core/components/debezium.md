# Debezium

## Overview

Debezium provides change data capture (CDC) capabilities in Layer 06. OpenLakes deploys a single Debezium Connect pod that reads from upstream databases and writes to Kafka topics.

## Configuration in OpenLakes

- **Image/version:** `debezium/connect:3.0.0.Final`.
- **Kafka connection:** Points at the infrastructure Kafka broker (`infrastructure-kafka:9092`).
- **Group ID & REST API:** Configured via `layers/06-ingestion/values.yaml` and exposed internally as `ingestion-debezium`.
- **Ingress:** Exposed through Traefik at `https://debezium.<domain>` so you can manage connectors via REST.
- **Storage:** Uses Longhorn PVCs for connector offsets/configs; credentials for source databases come from the shared secrets defined in `core-config.yaml`.

## Integration points

- CDC events land in Kafka topics that Meltano/Singer, Spark Structured Streaming, or Trino can consume.
- Schema evolution is handled by the Apicurio Registry deployed in Layer 01.
- Downstream jobs (Spark, Trino) can read CDC outputs from MinIO when you configure sink connectors to push Iceberg-friendly data.

## License

Debezium is licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
