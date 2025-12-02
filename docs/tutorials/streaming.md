# Streaming

The streaming tutorials now ingest real USGS earthquake events, orchestrated entirely by Airflow for full visibility across the OpenLakes stack.

## Open data source
- USGS real-time earthquake GeoJSON feed: `https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson`

## Pipeline
1. **Airflow DAG** – `examples/dags/open_data_ingestion_dag.py` triggers a Bash task (`usgs_feed_to_kafka`) that runs `examples/02-streaming/usgs_to_kafka.py`. This script continuously polls the feed and pushes events into a Kafka topic (`usgs_quakes`).
2. **Structured streaming** – The DAG then launches `examples/02-streaming/usgs_stream_processor.py` via `SparkSubmitOperator`. The job reads the Kafka topic, parses GeoJSON payloads, and writes events to an Iceberg table `lakehouse.streaming.usgs_quakes`.
3. **Notebook updates** – `examples/02-streaming/01-kafka-spark-streaming.ipynb` now uses the same USGS feed when demonstrating how Spark Structured Streaming consumes Kafka topics.

## Airflow snippet
```python
usgs_feed_to_kafka = BashOperator(
    task_id="usgs_feed_to_kafka",
    bash_command="python /opt/openlakes/examples/02-streaming/usgs_to_kafka.py --topic usgs_quakes --duration 180",
)

usgs_stream_processor = SparkSubmitOperator(
    task_id="usgs_stream_processor",
    application="/opt/openlakes/examples/02-streaming/usgs_stream_processor.py",
    conn_id="spark_default",
    name="usgs-stream-processor",
)
```

Because Airflow is orchestrating both ingestion and processing, operators can monitor the streaming workload, restart jobs, and audit checkpoints directly from the Airflow UI and the broader OpenLakes monitoring stack (Prometheus/Grafana + Loki).
