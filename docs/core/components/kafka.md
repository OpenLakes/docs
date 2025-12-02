# Apache Kafka

## Overview

Layer 01 installs a single-node Apache Kafka cluster (KRaft mode) paired with the Apicurio Registry (KafkaSQL storage). This broker powers Debezium change streams, Meltano/Singer pipelines, and any streaming demos you run on the platform.

## Configuration in OpenLakes

- **Image/version:** `apache/kafka:4.0.1` running in combined controller/broker mode (KRaft). The Schema Registry uses `quay.io/apicurio/apicurio-registry-kafkasql:2.6.13.Final`.
- **Storage:** Kafka data volumes are provisioned by Longhorn (`kafka.persistence.storageClass`). fsGroup is set to `1000` to handle Longhorn permissions automatically.
- **Networking:** Services are exposed as ClusterIP inside the cluster (`infrastructure-kafka:9092`). The Apicurio compatibility API is published via Traefik (`schema-registry.<domain>`).
- **Configuration:** The `_schemas` topic is managed by Apicurio’s KafkaSQL storage. Compatibility mode (`BACKWARD` by default) and TLS/auth settings can be tuned in `layers/01-infrastructure/values.yaml`.

## Integration points

- Debezium connectors publish CDC events to Kafka topics.
- Meltano/Singer pipelines and Spark Structured Streaming jobs can both read from and write to Kafka using the shared bootstrap servers.
- Schema management is centralized, so Spark/Trino/Meltano jobs can fetch Avro/JSON/Protobuf schemas programmatically via Apicurio.

## License

Apache Kafka and Apicurio Registry are licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
