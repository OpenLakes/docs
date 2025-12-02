# Tutorials

Hands-on tutorials map to each phase of the OpenLakes Core data-flow.

## 1. Ingest
Use Meltano/Singer (batch) or Debezium (CDC) to deliver raw data into MinIO buckets and Kafka topics. The examples below call out freely available public sources so you can demonstrate scale:
- **NOAA Climate Data** – bulk CSV downloads of weather readings (great for Meltano taps).
- **NYC TLC Trip Records** – large Parquet/CSV datasets for historical ETL.
- **USGS Earthquake GeoJSON feed** – live streaming events perfect for Spark Structured Streaming.
- **OpenAQ Air Quality API** – sensor data to enrich streaming joins.

### Batch sources → MinIO (`01-batch-etl-*`)
- [01-basic-etl](01-batch-etl-01-basic-etl.md) – foundational Meltano-style ETL. *(Example source: NOAA daily temperature CSVs)*
- [02-elt-trino](01-batch-etl-02-elt-trino.md) – push raw data to Iceberg, transform with Trino. *(Example source: NYC TLC Parquet dumps)*
- [03-multi-source-consolidation](01-batch-etl-03-multi-source-consolidation.md) – merge multiple upstream systems. *(Example sources: OpenFlights airports + airlines CSVs)*
- [04-scd2-historical-tracking](01-batch-etl-04-scd2-historical-tracking.md) – maintain slowly changing dimensions. *(Example source: World Bank country indicators)*
- [05-scd1-current-state](01-batch-etl-05-scd1-current-state.md) – current-state snapshots. *(Example source: OpenAQ latest measurements)*
- [06-batch-aggregations](01-batch-etl-06-batch-aggregations.md) – large-scale aggregations to curated zones. *(Example source: GDELT events aggregates)*

### Streaming & CDC (`02-streaming-*`)
- [01-kafka-spark-streaming](02-streaming-01-kafka-spark-streaming.md) – Spark reads Kafka in real time. *(Example source: USGS earthquake GeoJSON feed)*
- [02-spark-continuous-processing](02-streaming-02-spark-continuous-processing.md) – low-latency continuous mode. *(Example source: OpenAQ live sensors)*
- [03-cdc-postgres-to-iceberg](02-streaming-03-cdc-postgres-to-iceberg.md) – Debezium CDC straight into Iceberg. *(Example source: Pagila/PostgreSQL sample DB)*
- [04-multi-stream-join](02-streaming-04-multi-stream-join.md) – join multiple Kafka topics. *(Example source: Combine USGS earthquakes + NOAA weather alerts)*
- [05-stream-enrichment](02-streaming-05-stream-enrichment.md) – enrich streaming events in Spark. *(Example source: Join GDELT news events with OpenAQ readings)*
- [06-windowed-aggregations](02-streaming-06-windowed-aggregations.md) – sliding window metrics. *(Example source: GitHub Archive hourly events)*
- [07-exactly-once-spark](02-streaming-07-exactly-once-spark.md) – exactly-once semantics. *(Example source: NOAA streaming updates)*
- [08-session-window-analytics](02-streaming-08-session-window-analytics.md) – sessionization on streams. *(Example source: Clickstream feed from OpenFlights route logs)*

### Log & event ingestion (`06-log-event-*`)
- [01-log-dual-sink](06-log-event-01-log-dual-sink.md) – write application logs to Kafka and MinIO. *(Example source: Synthetic application logs or GitHub Actions logs)*
- [02-audit-trail](06-log-event-02-audit-trail.md) – immutable audit trails. *(Example source: Open metadata changes, GitHub issue events)*
- [03-clickstream](06-log-event-03-clickstream.md) – ingest clickstream events for analytics. *(Example source: GDELT or simulated web events)*

## 2. Process
Develop Spark workloads in JupyterHub, codify hybrid architectures, and blend multiple engines.

### Lambda & streaming blueprints
- [03-lambda-01-speed-batch-layers](03-lambda-01-speed-batch-layers.md) – canonical Lambda architecture. *(Demonstrate with NOAA + USGS feeds processed together)*
- [03-lambda-02-kappa-stream-only](03-lambda-02-kappa-stream-only.md) – Kappa-only streaming flows. *(Drive with USGS or OpenAQ continuous streams)*

### Federated processing (`04-federated-*`)
- [01-operational-analytical](04-federated-01-operational-analytical.md) – join OLTP + analytical systems. *(Example: Pagila/PostgreSQL + NYC TLC analytics)*
- [02-multi-engine-iceberg](04-federated-02-multi-engine-iceberg.md) – coordinate Spark, Trino, Flink on Iceberg. *(Example: Combine NOAA climate + OpenAQ air quality layers)*
- [03-cross-database](04-federated-03-cross-database.md) – cross-database queries & consolidation. *(Example: Blend World Bank indicators with OpenFlights routes)*

## 3. Catalog
Register services, datasets, lineage, and quality signals in OpenMetadata. Use the same public sources (NOAA, NYC TLC, USGS, etc.) so readers can trace lineage from ingestion through cataloging.

- [07-governance-01-data-quality](07-governance-01-data-quality.md) – automated quality checks.
- [07-governance-02-schema-evolution](07-governance-02-schema-evolution.md) – document schema changes.
- [07-governance-03-data-lineage](07-governance-03-data-lineage.md) – visualize producer/consumer chains.
- [07-governance-04-data-cataloging](07-governance-04-data-cataloging.md) – onboard data products with ownership and tags.

## 4. Analyze
Explore data via notebooks, run collaborative experiments, and publish dashboards built on those open datasets (e.g., NYC taxi revenue heat maps, NOAA anomaly charts, USGS earthquake alerting).

- [05-analytics-bi-01-superset-self-service](05-analytics-bi-01-superset-self-service.md) – empower analysts with curated Superset datasets.
- [05-analytics-bi-02-jupyter-collaborative-notebooks](05-analytics-bi-02-jupyter-collaborative-notebooks.md) – collaborative notebook workflows.
- [05-analytics-bi-03-scheduled-reports](05-analytics-bi-03-scheduled-reports.md) – automated reporting pipelines.
- [05-analytics-bi-04-reverse-etl](05-analytics-bi-04-reverse-etl.md) – operationalize BI outputs via reverse ETL.
- [Analytics & BI overview](analytics.md) – contextual entry point for the whole analytics toolchain.

## 5. Observe
Monitor the platform, benchmark storage tiers, and keep Iceberg tables healthy as those ingest pipelines scale up with public data volumes.

### Storage performance (`06-storage-performance-*`)
- [01-iceberg-direct-minio](06-storage-performance-01-iceberg-direct-minio.md) – baseline Iceberg on MinIO.
- [02-iceberg-alluxio-cached](06-storage-performance-02-iceberg-alluxio-cached.md) – add Alluxio caching.
- [03-performance-comparison](06-storage-performance-03-performance-comparison.md) – compare hot tiers.
- [04-alluxio-minio-benchmark](06-storage-performance-04-alluxio-minio-benchmark.md) – benchmark caching tiers.

### Housekeeping & maintenance (`08-maintenance-*`)
- [01-table-compaction](08-maintenance-01-table-compaction.md) – compaction strategies.
- [02-snapshot-expiration](08-maintenance-02-snapshot-expiration.md) – manage Iceberg snapshots.
- [03-partition-evolution](08-maintenance-03-partition-evolution.md) – evolve partition schemes safely.
- [04-orphan-file-cleanup](08-maintenance-04-orphan-file-cleanup.md) – purge orphaned data files.

Each tutorial follows the same structure:

Each tutorial follows the same structure:

1. **Overview** – What you’ll accomplish and why it matters.
2. **Prerequisites** – Required services, credentials, or datasets.
3. **Step-by-step instructions** – Detailed walkthrough with screenshots or CLI snippets.
4. **Code samples** – Copy/paste Spark notebooks, Meltano configs, or SQL.
5. **Next steps** – Suggested follow-ups to extend the pattern.

## Publishing notebooks to Airflow via Papermill
OpenLakes now includes an automated notebook registry so users can promote notebooks from JupyterHub into scheduled Airflow jobs.

1. **Author notebooks** on the shared PVC (default `/home/jovyan/examples`). When you’re ready to schedule one, run the helper from inside JupyterHub (the container already exposes the required `OPENLAKES_*` environment variables so it can talk to MinIO):
   ```bash
   python scripts/mark_notebook_ready.py \
     --notebook examples/01-batch-etl/01-basic-etl.ipynb \
     --dag-id noaa_batch_etl \
     --schedule "0 2 * * *" \
     --output-notebook /opt/openlakes/artifacts/noaa_batch_etl-{{ ds }}.ipynb \
     --param environment=production --param execution_date={{ ds }}
   ```
   Toggle `--disable` to pause or remove the job later.
2. **The helper uploads the notebook to MinIO** under both `workspace/<user>/` (for personal history) and `published/<dag_id>/`. Airflow’s CronJob mirrors the published prefix into `/opt/airflow/dags/published`, and `examples/dags/notebook_registry.py` scans that folder for notebooks containing a `papermill_job` metadata block with `"enabled": true`.
3. **Monitor executions** in Airflow. Each notebook run logs its Papermill output notebook under `/opt/openlakes/artifacts` so you can download notebook results with embedded plots.

This workflow keeps authoring inside JupyterHub while ensuring every scheduled notebook is visible, auditable, and restartable from the Airflow UI.
