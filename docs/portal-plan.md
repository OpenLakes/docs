# OpenLakes Portal Plan

Purpose-built console that exposes the entire OpenLakes lakehouse through a single ingress endpoint. Engineers authenticate once (SSO → Console) and receive scoped tokens for downstream services (Meltano, Airflow, dbt, Trino, OpenMetadata, Superset, MinIO, StarRocks). Everything else remains ClusterIP/private.

## Guiding Principles
- **Workflow-first** – encode common data engineering journeys (ingest → model → govern → serve) instead of linking to raw UIs.
- **Least privilege** – Console brokers auth via Vault/Kubernetes ServiceAccounts; downstream UIs never leave the cluster unauthenticated.
- **Observability baked in** – Helm release status, Prometheus/Grafana, lineage/quality surfaced contextually.
- **Extensible** – plugin model (Backstage-style) so new OSS components slot in quickly.

## Core Personas & Journeys

| Persona | Need | Portal experience |
|---------|------|-------------------|
| Data Engineer | Land new source, build tables, publish to BI | Guided ingestion wizard → dbt workspace → SQL workbench → publish + test |
| Analytics Engineer | Iterate dbt models, document lineage, promote to StarRocks | dbt IDE + run orchestrator (Airflow task) + OpenMetadata integration |
| Platform Admin | Monitor cluster, upgrade Helm release, manage secrets | Ops dashboard, Helm state view, secret rotation, audit log |
| BI Analyst | Discover curated datasets, launch Superset dashboards | Catalog search + one-click Superset with SSO |

### End-to-end workflow
1. **Discover data** – Search OpenMetadata inside Console (lineage, quality, owners).
2. **Ingest** – Guided Meltano/Singer connection builder: source creds from Vault, choose MinIO bucket + Iceberg namespace, auto-generate Airflow DAG for scheduling.
3. **Model (dbt)** – Console hosts dbt repo browser + IDE. Engineers edit models, run `dbt build` via:
   - on-demand runs (REST call to Airflow DAG that launches dbt container),
   - scheduled runs (dbt DAG, now provisioned by Helm chart),
   - CI integrations (GitHub actions, surfaced in Console).
4. **Validate** – dbt tests + Great Expectations results surfaced; integration tests run via Helm hook.
5. **Serve/BI** – Publish dataset to Superset or StarRocks (Realtime SKU) with templated connectors.
6. **Operate** – Dashboard shows Helm release health, pod status, metrics, alerts, Helm SQL state, upgrade CTA.

## Portal Modules

1. **Home / Health**
   - Release version, Helm test status, component availability, recent alerts.
   - Quick actions: “New ingestion,” “New dbt model,” “Open SQL Workbench,” “Launch Superset.”
2. **Pipelines**
   - Card per pipeline (Meltano connection + Airflow orchestration). Pause/resume, inspect logs, trigger backfill.
3. **dbt Studio**
   - Git-backed file browser + editor (monaco). Inline lint/test (`dbt build` via Airflow API).
   - Run history table (status, runtime, artifact location). Surfacing docs site.
4. **Lakehouse Explorer**
   - MinIO/Iceberg tree, sample previews via Trino, schema diff, retention policies.
   - “Promote to StarRocks” action (Realtime SKU) to materialize serving marts.
5. **Observability**
   - Embedded Grafana dashboards, log viewer (Loki/Elastic), SLA monitor.
6. **Governance**
   - Embedded OpenMetadata views: dataset details, lineage graph, owners, quality assertions, dbt metadata ingestion results.
7. **Admin**
   - User/role management, secret rotation, Helm upgrade orchestration (`helm repo update && helm upgrade` automation), image mirror status, air-gapped bundle builder.

## Technical Architecture
- **Frontend**: React/Next.js or Backstage foundation.
- **Backend**: Go or FastAPI service running in-cluster with RBAC to Kubernetes API, Vault, Helm SQL state DB, component APIs.
- **Auth**: OIDC (AzureAD, Okta). Console exchanges tokens for service-specific cookies (e.g., Superset, Airflow) via service accounts.
- **Plugins/Integrations**:
  - Meltano REST API
  - Airflow REST API (dbt DAG control, DAG/Pipeline status)
  - Trino REST/JDBC for SQL IDE + sampling
  - MinIO S3 API for storage browsing
  - OpenMetadata GraphQL/REST
  - Superset FAB security for SSO tokens
  - Vault (secrets), Helm SQL driver (release mgmt), Prometheus/Grafana (metrics)

## dbt Integration (new)
- dbt is first-class modeling workflow:
  - Helm chart provisions a managed dbt runner: Airflow DAG (`openlakes_dbt_run`) uses KubernetesPodOperator with `ghcr.io/dbt-labs/dbt-trino` image, cloning a configurable Git repo (default `dbt-labs/jaffle_shop`).
  - Profiles stored in ConfigMap with Trino target; KubernetesPodOperator mounts it at runtime.
  - Console exposes:
    - Run Now / schedule editing,
    - log streaming + artifacts download (stored in MinIO),
    - button to sync docs/tests back into OpenMetadata via ingestion pipeline.
  - Engineers can connect their own repos by updating portal settings (or values override), with secret storage handled by Vault integration.

## Portal Implementation Roadmap
1. **Foundations**
   - Scaffold Next.js (or Backstage) app with OAuth, RBAC, base layout.
   - Build backend service with connectors to Meltano, Airflow, Trino, OpenMetadata, Helm.
2. **Workflow MVP**
   - Ingestion wizard (Meltano + Airflow) with secret vault integration.
   - SQL Workbench hitting Trino (duckdb fallback for demos).
   - dbt run control panel (list runs, trigger Airflow DAG, surface logs).
3. **Lakehouse Explorer & Governance**
   - Integrate OpenMetadata browsing, lineage visualization.
   - MinIO/Iceberg explorer with sample queries.
4. **Observability & Ops**
   - Display Helm release info, integration test results, cluster pod health.
   - Build upgrade automation (toggle to run installer in upgrade mode).
5. **Serve / Realtime Add-ons**
   - StarRocks provisioning UI, Superset data-source templating, caching controls.
6. **Hardening**
   - Audit logging, multi-cluster targeting, air-gapped workflow support, theming, training modules integration.

Deliverables from this step:
- Updated Helm chart provisions dbt Airflow DAG & ConfigMaps.
- Portal plan (this document) capturing workflows with dbt as core modeling layer.
