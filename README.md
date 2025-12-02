# OpenLakes Documentation

This repository hosts the OpenLakes documentation site (MkDocs + Material theme). It contains all markdown content, the MkDocs configuration, and helper scripts that sync docs from other OpenLakes projects.

## Local Development

Using [uv](https://docs.astral.sh/uv/) (recommended):

```bash
uv run python scripts/sync-docs.py  # optional: pull docs from sibling repos
uv run mkdocs serve
```

Or with pip:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-docs.txt
python scripts/sync-docs.py
mkdocs serve
```

Docs are available at <http://localhost:8000> once the server starts.

## Deployment

Pushes to `main` trigger `.github/workflows/docs.yml`, which:

1. Optionally syncs docs from the `core` and `pier` repos.
2. Builds the MkDocs site (`mkdocs build --strict`).
3. Publishes to GitHub Pages (configure the repo’s Pages settings to use GitHub Actions and set the custom domain to `docs.openlakes.io`).

## Repository Layout

```
docs/                # Markdown + assets
mkdocs.yml           # MkDocs configuration
scripts/sync-docs.py # Imports content from sibling projects
```
