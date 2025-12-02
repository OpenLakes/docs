#!/usr/bin/env python3
"""
OpenLakes Documentation Sync Script

Syncs documentation from core and pier projects into the docs site.
Supports:
- Copying markdown files
- Converting Jupyter notebooks to markdown
- Extracting OCI labels from Dockerfiles
- Generating Helm chart documentation
"""

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional

# Paths
SCRIPT_DIR = Path(__file__).parent
SITE_ROOT = SCRIPT_DIR.parent
DOCS_DIR = SITE_ROOT / "docs"
PROJECTS_ROOT = SITE_ROOT.parent  # One directory up

CORE_DIR = PROJECTS_ROOT / "core"
PIER_DIR = PROJECTS_ROOT / "pier"


def ensure_dir(path: Path) -> None:
    """Create directory if it doesn't exist."""
    path.mkdir(parents=True, exist_ok=True)


def copy_markdown(src: Path, dest: Path, title_override: Optional[str] = None) -> None:
    """Copy a markdown file, optionally modifying the title."""
    content = src.read_text()

    # If title override provided, replace or add frontmatter
    if title_override:
        if content.startswith("---"):
            # Has frontmatter, try to update title
            end = content.find("---", 3)
            if end != -1:
                frontmatter = content[3:end]
                if "title:" in frontmatter:
                    frontmatter = re.sub(r"title:.*", f"title: {title_override}", frontmatter)
                else:
                    frontmatter = f"title: {title_override}\n" + frontmatter
                content = f"---{frontmatter}---{content[end+3:]}"
        else:
            # No frontmatter, add it
            content = f"---\ntitle: {title_override}\n---\n\n{content}"

    ensure_dir(dest.parent)
    dest.write_text(content)
    print(f"  Copied: {src.name} -> {dest}")


def convert_notebook(src: Path, dest: Path) -> bool:
    """Convert a Jupyter notebook to markdown."""
    try:
        result = subprocess.run(
            [
                sys.executable, "-m", "nbconvert",
                "--to", "markdown",
                "--output-dir", str(dest.parent),
                "--output", dest.stem,
                str(src)
            ],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            print(f"  Converted: {src.name} -> {dest.name}")
            return True
        else:
            print(f"  Warning: Failed to convert {src.name}: {result.stderr}")
            return False
    except Exception as e:
        print(f"  Warning: Failed to convert {src.name}: {e}")
        return False


def extract_dockerfile_labels(dockerfile_path: Path) -> dict:
    """Extract OCI labels from a Dockerfile."""
    labels = {}
    content = dockerfile_path.read_text()

    # Match LABEL directives
    label_pattern = re.compile(
        r'LABEL\s+(?:org\.opencontainers\.image\.)?(\w+)\s*=\s*["\']?([^"\'\\n]+)["\']?',
        re.MULTILINE
    )

    for match in label_pattern.finditer(content):
        key = match.group(1)
        value = match.group(2).strip()
        labels[key] = value

    return labels


def generate_docker_image_doc(image_dir: Path, output_path: Path) -> None:
    """Generate documentation for a Docker image."""
    dockerfile = image_dir / "Dockerfile"
    readme = image_dir / "README.md"

    if not dockerfile.exists():
        return

    labels = extract_dockerfile_labels(dockerfile)
    image_name = image_dir.name

    content_parts = [f"# {labels.get('title', image_name)}\n"]

    if labels.get("description"):
        content_parts.append(f"{labels['description']}\n")

    content_parts.append("\n## Image Details\n")
    content_parts.append("| Property | Value |")
    content_parts.append("|----------|-------|")

    if labels.get("version"):
        content_parts.append(f"| Version | `{labels['version']}` |")
    if labels.get("vendor"):
        content_parts.append(f"| Vendor | {labels['vendor']} |")
    if labels.get("source"):
        content_parts.append(f"| Source | [{labels['source']}]({labels['source']}) |")
    if labels.get("licenses"):
        content_parts.append(f"| License | {labels['licenses']} |")

    content_parts.append("")

    # If README exists, append its content
    if readme.exists():
        readme_content = readme.read_text()
        # Skip the first heading if present (we already have one)
        if readme_content.startswith("#"):
            first_newline = readme_content.find("\n")
            if first_newline != -1:
                readme_content = readme_content[first_newline+1:].lstrip()
        content_parts.append("\n## Overview\n")
        content_parts.append(readme_content)

    ensure_dir(output_path.parent)
    output_path.write_text("\n".join(content_parts))
    print(f"  Generated: {output_path.name}")


def generate_helm_chart_doc(chart_dir: Path, output_path: Path) -> None:
    """Generate documentation for a Helm chart."""
    chart_yaml = chart_dir / "Chart.yaml"
    values_yaml = chart_dir / "values.yaml"

    if not chart_yaml.exists():
        return

    try:
        import yaml
        chart_meta = yaml.safe_load(chart_yaml.read_text())
    except Exception as e:
        print(f"  Warning: Failed to parse {chart_yaml}: {e}")
        return

    chart_name = chart_meta.get("name", chart_dir.name)

    content_parts = [f"# {chart_name}\n"]

    if chart_meta.get("description"):
        content_parts.append(f"{chart_meta['description']}\n")

    content_parts.append("\n## Chart Info\n")
    content_parts.append("| Property | Value |")
    content_parts.append("|----------|-------|")
    content_parts.append(f"| Version | `{chart_meta.get('version', 'N/A')}` |")
    content_parts.append(f"| App Version | `{chart_meta.get('appVersion', 'N/A')}` |")
    content_parts.append(f"| Type | {chart_meta.get('type', 'application')} |")

    # Dependencies
    deps = chart_meta.get("dependencies", [])
    if deps:
        content_parts.append("\n## Dependencies\n")
        content_parts.append("| Chart | Version | Repository |")
        content_parts.append("|-------|---------|------------|")
        for dep in deps:
            content_parts.append(
                f"| {dep.get('name', 'N/A')} | "
                f"`{dep.get('version', 'N/A')}` | "
                f"{dep.get('repository', 'N/A')} |"
            )

    # Values preview
    if values_yaml.exists():
        content_parts.append("\n## Configuration\n")
        content_parts.append("Key configuration values (see `values.yaml` for full reference):\n")
        content_parts.append("```yaml")
        # Only show first 50 lines of values
        values_content = values_yaml.read_text()
        lines = values_content.split("\n")[:50]
        content_parts.append("\n".join(lines))
        if len(values_content.split("\n")) > 50:
            content_parts.append("# ... (truncated)")
        content_parts.append("```")

    ensure_dir(output_path.parent)
    output_path.write_text("\n".join(content_parts))
    print(f"  Generated: {output_path.name}")


def sync_core_docs() -> None:
    """Sync documentation from the core project."""
    print("\n=== Syncing Core Documentation ===")

    if not CORE_DIR.exists():
        print(f"  Warning: Core directory not found at {CORE_DIR}")
        return

    core_docs = DOCS_DIR / "core"
    ensure_dir(core_docs)

    # Copy main documentation files
    doc_mappings = [
        (CORE_DIR / "CLAUDE.md", core_docs / "guide.md", "Developer Guide"),
        (CORE_DIR / "docs" / "deployment-guide.md", core_docs / "deployment.md", None),
        (CORE_DIR / "docs" / "STORAGE-ARCHITECTURE.md", core_docs / "storage.md", "Storage Architecture"),
        (CORE_DIR / "docs" / "INGRESS-SETUP.md", core_docs / "ingress.md", "Ingress Setup"),
        (CORE_DIR / "docs" / "SCHEMA-REGISTRY-GUIDE.md", core_docs / "schema-registry.md", "Schema Registry"),
    ]

    for src, dest, title in doc_mappings:
        if src.exists():
            copy_markdown(src, dest, title)

    # Generate Docker image docs
    print("\n  Generating Docker image documentation...")
    docker_dir = CORE_DIR / "docker"
    docker_docs = DOCS_DIR / "reference" / "docker"
    ensure_dir(docker_docs)

    if docker_dir.exists():
        for image_dir in docker_dir.iterdir():
            if image_dir.is_dir() and (image_dir / "Dockerfile").exists():
                generate_docker_image_doc(
                    image_dir,
                    docker_docs / f"{image_dir.name}.md"
                )

    # Convert example notebooks
    print("\n  Converting example notebooks...")
    examples_dir = CORE_DIR / "examples"
    tutorials_dir = DOCS_DIR / "tutorials"
    ensure_dir(tutorials_dir)

    if examples_dir.exists():
        for example_dir in sorted(examples_dir.iterdir()):
            if example_dir.is_dir():
                # Find notebooks in example directory
                for notebook in example_dir.glob("*.ipynb"):
                    output_name = f"{example_dir.name}-{notebook.stem}.md"
                    convert_notebook(notebook, tutorials_dir / output_name)

    # Generate Helm chart docs
    print("\n  Generating Helm chart documentation...")
    layers_dir = CORE_DIR / "layers"
    helm_docs = DOCS_DIR / "reference" / "helm"
    ensure_dir(helm_docs)

    if layers_dir.exists():
        for layer_dir in sorted(layers_dir.iterdir()):
            if layer_dir.is_dir() and (layer_dir / "Chart.yaml").exists():
                generate_helm_chart_doc(
                    layer_dir,
                    helm_docs / f"{layer_dir.name}.md"
                )


def sync_pier_docs() -> None:
    """Sync documentation from the pier project."""
    print("\n=== Syncing Pier Documentation ===")

    if not PIER_DIR.exists():
        print(f"  Warning: Pier directory not found at {PIER_DIR}")
        return

    pier_docs = DOCS_DIR / "pier"
    ensure_dir(pier_docs)

    # Copy main documentation files
    doc_mappings = [
        (PIER_DIR / "README.md", pier_docs / "index.md", "Pier Overview"),
        (PIER_DIR / "README_LLM.md", pier_docs / "llm-inference.md", "LLM Inference"),
        (PIER_DIR / "architecture" / "medallion-setup" / "README.md", pier_docs / "medallion.md", "Medallion Architecture"),
    ]

    for src, dest, title in doc_mappings:
        if src.exists():
            copy_markdown(src, dest, title)

    # Generate Docker image docs for pier
    docker_dir = PIER_DIR / "docker"
    docker_docs = DOCS_DIR / "reference" / "docker"
    ensure_dir(docker_docs)

    if docker_dir.exists():
        for image_dir in docker_dir.iterdir():
            if image_dir.is_dir() and (image_dir / "Dockerfile").exists():
                generate_docker_image_doc(
                    image_dir,
                    docker_docs / f"pier-{image_dir.name}.md"
                )


def create_index_files() -> None:
    """Create index files for documentation sections."""
    print("\n=== Creating Index Files ===")

    # Main index
    index_content = """# OpenLakes Documentation

Welcome to the OpenLakes documentation. OpenLakes is an open-source lakehouse platform
that deploys a production-ready data infrastructure in minutes.

## Quick Links

<div class="grid cards" markdown>

-   :material-rocket-launch:{ .lg .middle } **Getting Started**

    ---

    Get up and running with OpenLakes in minutes

    [:octicons-arrow-right-24: Quick Start](getting-started/quickstart.md)

-   :material-cube-outline:{ .lg .middle } **Core Platform**

    ---

    Learn about the core components and architecture

    [:octicons-arrow-right-24: Core Docs](core/index.md)

-   :material-book-open-page-variant:{ .lg .middle } **Tutorials**

    ---

    Step-by-step guides for common workflows

    [:octicons-arrow-right-24: Tutorials](tutorials/index.md)

-   :material-cog:{ .lg .middle } **Reference**

    ---

    API docs, configuration, and CLI reference

    [:octicons-arrow-right-24: Reference](reference/index.md)

</div>

## What is OpenLakes?

OpenLakes is a complete data lakehouse platform built on proven open-source foundations:

- **Apache Iceberg** - Open table format for huge analytic datasets
- **Apache Spark** - Unified analytics engine for large-scale data processing
- **Trino** - Fast distributed SQL query engine
- **MinIO** - High-performance S3-compatible object storage
- **OpenMetadata** - Open standard for metadata, data discovery, and governance
- **Apache Kafka** - Distributed event streaming platform

## Key Features

- **One-click deployment** - Deploy to Kubernetes with a single command
- **100% Open Source** - Apache 2.0 licensed, no vendor lock-in
- **Production-ready** - Security, monitoring, and backups configured out of the box
- **Scalable** - From laptop to petabyte-scale clusters
"""

    (DOCS_DIR / "index.md").write_text(index_content)
    print("  Created: index.md")

    # Getting started index
    getting_started = """# Getting Started

This section will help you get OpenLakes up and running.

## Prerequisites

Before installing OpenLakes, ensure you have:

- Kubernetes cluster (v1.25+) or Docker Desktop with Kubernetes enabled
- `kubectl` configured to access your cluster
- `helm` v3.10+
- Minimum 16GB RAM, 4 CPUs for local development

## Installation Options

| Environment | Description |
|-------------|-------------|
| Local | Development on Docker Desktop or minikube |
| Development | Shared development cluster |
| Staging | Pre-production testing |
| Production | Full production deployment |

## Next Steps

1. [Quick Start](quickstart.md) - Deploy OpenLakes in 5 minutes
2. [Prerequisites](prerequisites.md) - Detailed requirements
3. [Installation](installation.md) - Step-by-step installation guide
"""

    ensure_dir(DOCS_DIR / "getting-started")
    (DOCS_DIR / "getting-started" / "index.md").write_text(getting_started)
    print("  Created: getting-started/index.md")

    # Core index
    core_index = """# Core Platform

The OpenLakes Core platform provides the foundation for your data lakehouse.

## Architecture Overview

OpenLakes Core deploys as a set of Helm charts organized into layers:

1. **Infrastructure** - Storage, networking, secrets
2. **Compute** - Spark, Trino query engines
3. **Streaming** - Kafka, Schema Registry
4. **Orchestration** - Airflow, JupyterHub
5. **Catalog** - OpenMetadata, Hive Metastore
6. **BI** - Superset dashboards
7. **Monitoring** - Prometheus, Grafana

## Components

| Component | Purpose |
|-----------|---------|
| [Apache Spark](components/spark.md) | Large-scale data processing |
| [Trino](components/trino.md) | Fast SQL queries |
| [MinIO](components/minio.md) | S3-compatible storage |
| [Kafka](components/kafka.md) | Event streaming |
| [OpenMetadata](components/openmetadata.md) | Data catalog & governance |
| [JupyterHub](components/jupyterhub.md) | Interactive notebooks |

## Guides

- [Deployment Guide](deployment.md) - Deploy to any environment
- [Storage Architecture](storage.md) - Understanding data storage
- [Developer Guide](guide.md) - Contributing to OpenLakes
"""

    ensure_dir(DOCS_DIR / "core")
    (DOCS_DIR / "core" / "index.md").write_text(core_index)
    print("  Created: core/index.md")

    # Tutorials index
    tutorials_index = """# Tutorials

Hands-on tutorials for common data engineering workflows with OpenLakes.

## Available Tutorials

### Batch Processing
- [Batch ETL Patterns](batch-etl.md) - Extract, transform, load workflows

### Real-time Processing
- [Stream Processing](streaming.md) - Kafka to Iceberg pipelines

### Query Patterns
- [Federated Queries](federated.md) - Query across multiple data sources

### Analytics
- [Analytics & BI](analytics.md) - Building dashboards with Superset

## Tutorial Structure

Each tutorial includes:

1. **Overview** - What you'll learn
2. **Prerequisites** - What you need before starting
3. **Step-by-step instructions** - Detailed walkthrough
4. **Code samples** - Ready-to-run examples
5. **Next steps** - Where to go from here
"""

    ensure_dir(DOCS_DIR / "tutorials")
    (DOCS_DIR / "tutorials" / "index.md").write_text(tutorials_index)
    print("  Created: tutorials/index.md")

    # Reference index
    reference_index = """# Reference

Technical reference documentation for OpenLakes.

## Docker Images

Pre-built container images for OpenLakes components:

- [Spark with OpenMetadata](docker/spark.md)
- [JupyterHub Notebook](docker/jupyterhub-notebook.md)
- [OpenLakes Dashboard](docker/openlakes-dashboard.md)

## Helm Charts

Helm chart documentation for each layer:

- [Infrastructure Layer](helm/01-infrastructure.md)
- [Compute Layer](helm/02-compute.md)
- [Streaming Layer](helm/03-streaming.md)

## Configuration

- [Environment Variables](configuration.md)
- [Secrets Management](configuration.md#secrets)

## CLI Reference

- [deploy-openlakes.sh](cli.md) - Main deployment script
"""

    ensure_dir(DOCS_DIR / "reference")
    (DOCS_DIR / "reference" / "index.md").write_text(reference_index)
    print("  Created: reference/index.md")


def create_placeholder_files() -> None:
    """Create placeholder files for sections that need manual content."""
    print("\n=== Creating Placeholder Files ===")

    placeholders = {
        "getting-started/quickstart.md": "# Quick Start\n\n!!! note\n    This page is auto-generated. Content coming soon.\n",
        "getting-started/prerequisites.md": "# Prerequisites\n\n!!! note\n    This page is auto-generated. Content coming soon.\n",
        "getting-started/installation.md": "# Installation\n\n!!! note\n    This page is auto-generated. Content coming soon.\n",
        "core/architecture.md": "# Architecture\n\n!!! note\n    This page is auto-generated. Content coming soon.\n",
        "core/components/spark.md": "# Apache Spark\n\n!!! note\n    This page is auto-generated. Content coming soon.\n",
        "core/components/trino.md": "# Trino\n\n!!! note\n    This page is auto-generated. Content coming soon.\n",
        "core/components/kafka.md": "# Apache Kafka\n\n!!! note\n    This page is auto-generated. Content coming soon.\n",
        "core/components/minio.md": "# MinIO\n\n!!! note\n    This page is auto-generated. Content coming soon.\n",
        "core/components/openmetadata.md": "# OpenMetadata\n\n!!! note\n    This page is auto-generated. Content coming soon.\n",
        "core/components/jupyterhub.md": "# JupyterHub\n\n!!! note\n    This page is auto-generated. Content coming soon.\n",
        "pier/automation.md": "# Automation\n\n!!! note\n    This page is auto-generated. Content coming soon.\n",
        "tutorials/batch-etl.md": "# Batch ETL\n\n!!! note\n    This page is auto-generated. Content coming soon.\n",
        "tutorials/streaming.md": "# Streaming\n\n!!! note\n    This page is auto-generated. Content coming soon.\n",
        "tutorials/federated.md": "# Federated Queries\n\n!!! note\n    This page is auto-generated. Content coming soon.\n",
        "tutorials/analytics.md": "# Analytics & BI\n\n!!! note\n    This page is auto-generated. Content coming soon.\n",
        "reference/docker-images.md": "# Docker Images\n\n!!! note\n    This page is auto-generated. Content coming soon.\n",
        "reference/helm-charts.md": "# Helm Charts\n\n!!! note\n    This page is auto-generated. Content coming soon.\n",
        "reference/cli.md": "# CLI Reference\n\n!!! note\n    This page is auto-generated. Content coming soon.\n",
        "reference/configuration.md": "# Configuration\n\n!!! note\n    This page is auto-generated. Content coming soon.\n",
        "development/contributing.md": "# Contributing\n\n!!! note\n    This page is auto-generated. Content coming soon.\n",
        "development/guide.md": "# Developer Guide\n\n!!! note\n    This page is auto-generated. Content coming soon.\n",
    }

    for path, content in placeholders.items():
        full_path = DOCS_DIR / path
        if not full_path.exists():
            ensure_dir(full_path.parent)
            full_path.write_text(content)
            print(f"  Created placeholder: {path}")


def main() -> None:
    """Main entry point."""
    print("OpenLakes Documentation Sync")
    print("=" * 40)

    # Create base docs directory
    ensure_dir(DOCS_DIR)

    # Create overrides directory for custom theme
    ensure_dir(DOCS_DIR / "overrides")
    ensure_dir(DOCS_DIR / "stylesheets")
    ensure_dir(DOCS_DIR / "assets")

    # Create extra CSS
    extra_css = """/* OpenLakes Documentation Custom Styles */

:root {
  --md-primary-fg-color: #0891b2;
  --md-primary-fg-color--light: #22d3ee;
  --md-primary-fg-color--dark: #0e7490;
  --md-accent-fg-color: #0891b2;
}

.md-header {
  background-color: var(--md-primary-fg-color);
}

.md-tabs {
  background-color: var(--md-primary-fg-color);
}

/* Grid cards for homepage */
.grid.cards > ul {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
  gap: 1rem;
  padding: 0;
  list-style: none;
}

.grid.cards > ul > li {
  border: 1px solid var(--md-default-fg-color--lightest);
  border-radius: 0.5rem;
  padding: 1.5rem;
}
"""

    (DOCS_DIR / "stylesheets" / "extra.css").write_text(extra_css)
    print("  Created: stylesheets/extra.css")

    # Sync documentation
    create_index_files()
    create_placeholder_files()
    sync_core_docs()
    sync_pier_docs()

    print("\n" + "=" * 40)
    print("Documentation sync complete!")
    print(f"Docs directory: {DOCS_DIR}")
    print("\nTo preview locally:")
    print("  pip install -r requirements-docs.txt")
    print("  mkdocs serve")


if __name__ == "__main__":
    main()
