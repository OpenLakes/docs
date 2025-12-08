# JupyterHub

## Overview

JupyterHub gives analysts and engineers a browser-based notebook environment. It lives in Layer 05 together with Superset and is exposed at `https://jupyter.<domain>` (default credentials: admin / openlakes).

## Configuration in OpenLakes

- **Image:** Uses the community `quay.io/jupyterhub/k8s-hub:4.3.1` chart with KubernetesSpawner.
- **Authentication:** Dummy authenticator (`admin` / `openlakes`) is enabled for demos. Swap in an authenticator of your choice by editing `layers/05-analytics/values.yaml`.
- **Storage:** User homes are provisioned dynamically on Longhorn. A shared PVC (`analytics-jupyter-examples`) is mounted read-only at `/home/jovyan/examples` and is refreshed by mirroring the MinIO `shared/` prefix.
- **Notebook publishing:** When `components.populateExamples` is true the deployment script seeds the MinIO `shared/` prefix from the local repo. To promote a notebook to Airflow, run `scripts/mark_notebook_ready.py`, which updates the Papermill metadata and uploads the notebook to the MinIO `published/` prefix.
- **Network policy:** Disabled so the hub can talk to the Kubernetes API (needed for spawning notebooks).

## Integration points

- Spark kernels can connect to the `compute-spark` service and use the same S3 credentials as batch jobs.
- Trino connections can be configured using the pre-created example notebooks or the Superset credentials.
- Airflow CronJobs in Layer 04 mirror the MinIO `published/` prefix to `/opt/airflow/dags/published`, so every notebook promoted from JupyterHub shows up as a Papermill DAG automatically.

## License

JupyterHub is licensed under the [BSD 3-Clause License](https://github.com/jupyterhub/jupyterhub/blob/main/LICENSE).
