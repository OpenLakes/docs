# Apache Superset

## Overview

Superset delivers the analytics dashboarding layer in Layer 05. It is automatically configured to use Trino as its default SQL engine, so you can explore Iceberg tables immediately after deployment.

## Configuration in OpenLakes

- **Image/version:** `apache/superset:4.1.1`.
- **Database:** Uses the shared PostgreSQL instance (`superset` database) for metadata.
- **Auth:** Default admin credentials (`admin/admin123`) are set via values; change them in `core-config.yaml` for production.
- **Ingress:** Exposed at `https://superset.<domain>`.
- **Storage:** A Longhorn PVC stores Superset artifacts (dashboards, uploads). Remote logging can be pointed at MinIO if desired.

## Integration points

- Superset ships with a Trino database connection that targets `compute-trino`; no manual setup required.
- Dashboards can query data stored in MinIO/Iceberg via Trino, or connect to other services you register.
- Authentication is tied to the Superset config only; if you integrate SSO, update the Helm values accordingly.

## License

Apache Superset is licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
