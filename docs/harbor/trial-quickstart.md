# Harbor Trial Quick Start

Get started with OpenLakes Harbor in minutes. No Kubernetes or infrastructure setup required.

## 1. Sign Up

Visit [harbor.openlakes.io](https://harbor.openlakes.io) and create your account.

- Use your work email for team collaboration
- No credit card required for the 30-day trial

## 2. Access Your Environment

Once provisioned, you'll receive access to your dedicated OpenLakes environment with all components ready:

| Service | What it does |
|---------|--------------|
| **Dashboard** | Central hub showing all available services |
| **JupyterHub** | Collaborative notebooks with Spark kernels |
| **Superset** | Self-service dashboards and SQL exploration |
| **Airflow** | Workflow orchestration and scheduling |
| **OpenMetadata** | Data catalog, lineage, and governance |
| **Trino** | Interactive SQL queries across all data |
| **MinIO** | S3-compatible object storage |
| **Grafana** | Platform monitoring and metrics |

## 3. Explore the Platform

### Run Your First Notebook

1. Open **JupyterHub** from the dashboard
2. Navigate to `/home/jovyan/examples`
3. Open any example notebook to see Spark and Iceberg in action

### Query Data with Trino

1. Open **Superset** from the dashboard
2. Go to **SQL Lab**
3. Select the Trino connection
4. Run queries against the sample datasets

### Create a Dashboard

1. In Superset, explore the pre-configured datasets
2. Create charts using the drag-and-drop interface
3. Combine charts into dashboards

### Schedule a Pipeline

1. Open **Airflow** from the dashboard
2. Browse the example DAGs
3. Enable a DAG to see scheduled execution

## 4. Bring Your Own Data

### Upload Files to MinIO

1. Open **MinIO** from the dashboard
2. Navigate to the `openlakes` bucket
3. Upload CSV, Parquet, or JSON files

### Connect External Sources

Use **Meltano** to pull data from external systems:

- Databases (PostgreSQL, MySQL, etc.)
- SaaS applications (Salesforce, HubSpot, etc.)
- APIs and files

See the [Batch ETL tutorials](../tutorials/batch-etl.md) for step-by-step guides.

## Trial Limitations

The free trial is designed for evaluation and learning:

- **30-day duration** - Environment expires after 30 days
- **No uptime guarantees** - Trial environments may experience occasional restarts
- **No data loss protection** - Back up important work externally

!!! warning "Important"
    Trial environments do not include uptime or data loss guarantees. Do not use for production workloads. Export any important data before your trial expires.

## Upgrade to Harbor Pro

After evaluating the platform, upgrade to Harbor Pro (coming Q1 2026) for:

- 99% uptime SLA
- Data loss protection
- 24/7 availability

Contact [sales@openlakes.io](mailto:sales@openlakes.io) to discuss your requirements.

## Next Steps

- [Explore the platform architecture](../core/index.md)
- [Follow the tutorials](../tutorials/index.md)
- [Compare Harbor vs self-hosted Core](vs-self-hosted.md)
