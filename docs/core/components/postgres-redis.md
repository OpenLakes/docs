# PostgreSQL & Redis

## Overview

Layer 01 provisions shared PostgreSQL and Redis instances that back almost every OpenLakes service (Airflow, Superset, Meltano, OpenMetadata, Nessie, etc.). Deploying once avoids running separate databases per chart.

## Configuration in OpenLakes

- **PostgreSQL:** StatefulSet using the Bitnami chart (`postgresql:16`) with a Longhorn PVC. Default credentials are defined in `core-config.yaml` (`credentials.postgres.*`).
- **Redis:** Deployed via Bitnami with persistence on Longhorn. Used primarily by Airflow for Celery/Kubernetes executor state and Superset session storage.
- **Access:** Both services are exposed as ClusterIP inside the cluster (`infrastructure-postgres`, `infrastructure-redis`).

## Integration points

- Nessie, Meltano, Superset, Airflow, OpenMetadata, and Debezium all reuse the shared PostgreSQL database, each with its own schema.
- Redis backs Airflow’s task queues and Superset’s caching/session store.

## License

- PostgreSQL is licensed under the [PostgreSQL License](https://www.postgresql.org/about/licence/).
- Redis (server) is licensed under the [Redis Source Available License](https://redis.io/legal/rsalv2/); the Bitnami Helm chart is Apache-2.0.
