# Batch ETL

The Batch ETL series (tutorials `01-basic-etl` through `01-batch-etl-06-batch-aggregations`) now pulls live NOAA precipitation data to highlight scalable ingestion on OpenLakes Core.

## Open data source
- NOAA hourly precipitation sample: `https://www1.ncdc.noaa.gov/pub/data/cdo/samples/PRECIP_HLY_sample_csv.csv`
- Downloaded automatically in `examples/01-batch-etl/01-basic-etl.ipynb`.

## Workflow
1. **Orchestration** – Airflow runs the notebook via the PapermillOperator so executions are fully observable.
2. **Extraction** – The notebook downloads the NOAA CSV, reshapes it into the generic schema (`product_id`, `product_name`, `category`, `price`, `quantity`).
3. **Transformation** – Spark aggregates the normalized dataset and writes Iceberg tables backed by MinIO.
4. **Validation** – Trino queries confirm counts and aggregated metrics.

## Airflow integration
`examples/dags/open_data_ingestion_dag.py` contains the `noaa_batch_etl` Papermill task:

```python
noaa_batch_etl = PapermillOperator(
    task_id="noaa_batch_etl",
    input_nb="/opt/openlakes/examples/01-batch-etl/01-basic-etl.ipynb",
    output_nb="/opt/openlakes/artifacts/noaa_batch_etl-{{ ds }}.ipynb",
    parameters={"execution_date": "{{ ds }}", "environment": "production"},
)
```

Run `helm list -n openlakes` → `kubectl port-forward deployment/orchestration-airflow-web ...` to open the Airflow UI and show stakeholders how notebooks, Spark jobs, and Iceberg tables are orchestrated and monitored end-to-end.
