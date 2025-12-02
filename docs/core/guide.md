---
title: Developer Guide
---

# OpenLakes Development Guide for Claude

This document provides context for Claude AI sessions working on the OpenLakes project. It explains the project structure, development workflows, and conventions that should be followed when making changes.

---

## CRITICAL: Current Development Phase and Versioning

**⚠️ ACTIVE DEVELOPMENT - PRE-1.0.0 RELEASE**

OpenLakes is currently in **active development** and has **NOT** released version 1.0.0 yet.

### Versioning Rules During Development:

1. **ALL custom Docker images MUST use version `1.0.0`**
   - Dashboard: `ghcr.io/openlakes/core/openlakes-dashboard:1.0.0`
   - Spark: `ghcr.io/openlakes/core/spark-openmetadata:1.0.0`
   - Any other custom images: Always `1.0.0`

2. **DO NOT increment version numbers during development**
   - ❌ WRONG: Building version 1.0.1, 1.0.2, 1.1.0, etc.
   - ✅ CORRECT: Always rebuild and push as `1.0.0`

3. **Version 1.0.0 is a moving target**
   - The `1.0.0` tag will be updated repeatedly during development
   - Changes to images are deployed by rebuilding `1.0.0`, not by incrementing versions

4. **When to increment versions (NOT YET)**
   - Version increments will only happen AFTER the official 1.0.0 release
   - The first official release will create immutable version 1.0.0
   - Subsequent changes will then use semantic versioning (1.0.1, 1.1.0, etc.)

### Build Commands During Development:

```bash
# Dashboard - ALWAYS use 1.0.0
cd docker/openlakes-dashboard
BUILD_ENV=production VERSION=1.0.0 ./build.sh

# Spark - ALWAYS use 1.0.0
cd docker/spark-openmetadata
BUILD_ENV=production VERSION=1.0.0 ./build.sh

# Any custom image - ALWAYS use 1.0.0
BUILD_ENV=production VERSION=1.0.0 ./build.sh
```

**Never ask about version numbers. Never suggest incrementing versions. Always use 1.0.0.**

---

## Project Overview

**OpenLakes** is a comprehensive Kubernetes-based data platform that provides:
- Data storage (PostgreSQL, MinIO S3, Kafka)
- Compute engines (Spark, Trino)
- Stream processing (Spark Structured Streaming)
- Orchestration (Airflow)
- Analytics (Superset, JupyterHub)
- Data catalog & lineage (OpenMetadata)
- Ingestion (Meltano/Singer, Debezium)

**Technology Stack**:
- Kubernetes (Rancher Desktop for local dev)
- Helm for deployment management
- Docker for custom images
- GitHub Container Registry (GHCR) for image hosting

---

## Directory Structure

```
openlakes-core/
├── .github/workflows/       # CI/CD workflows
│   └── release.yml          # Production release automation
│
├── docker/                  # Centralized Docker images
│   ├── spark-openmetadata/  # Spark with OpenMetadata agent
│   │   ├── Dockerfile       # Image definition
│   │   ├── build.sh         # Multi-environment build script
│   │   └── README.md        # Image documentation
│   ├── superset/            # (Future) Superset with custom drivers
│   └── scripts/             # Shared build utilities
│
├── layers/                  # Helm charts organized by function
│   ├── 01-infrastructure/   # PostgreSQL, MinIO, Kafka, etc.
│   ├── 02-compute/          # Spark, Trino
│   ├── 03-streaming/        # Spark Streaming
│   ├── 04-orchestration/    # Airflow
│   ├── 05-analytics/        # Superset, JupyterHub
│   ├── 06-ingestion/        # Meltano/Singer, Debezium
│   └── 07-catalog/          # OpenMetadata
│
├── deploy-openlakes.sh      # Unified deployment script
├── VERSION                  # Current project version
└── CLAUDE.md                # This file
```

---

## Four-Environment Architecture

OpenLakes uses a four-tier promotion workflow:

### 1. LOCAL (Fastest Iteration)

**Purpose**: Individual developer rapid iteration
**Image Storage**: Rancher Desktop local images
**Image Tag**: `{image-name}:local`
**Pull Policy**: `Never` (no network access)

**Workflow**:
```bash
# 1. Build image locally
cd docker/spark-openmetadata
./build.sh  # Defaults to BUILD_ENV=local

# 2. Deploy with local images
cd ../..
./deploy-openlakes.sh --env local

# 3. Test changes immediately
kubectl logs -f deployment/compute-spark-master -n openlakes
```

**When to use**: Daily development, testing code changes before committing

---

### 2. DEV (Collaborative Development)

**Purpose**: Team shares work-in-progress changes
**Image Storage**: GHCR public registry
**Image Tags**: `dev`, `dev-{username}`
**Pull Policy**: `Always` (always pull latest)

**Workflow**:
```bash
# 1. Build and push to GHCR
cd docker/spark-openmetadata
BUILD_ENV=dev ./build.sh  # Prompts for GHCR push

# 2. Deploy dev environment
cd ../..
./deploy-openlakes.sh --env dev

# 3. Team members can test
# Other developers run:
./deploy-openlakes.sh --env dev  # Gets your changes
```

**When to use**: Sharing features in-progress, collaborative debugging, pre-PR testing

---

### 3. STAGING (QA & Regression Testing)

**Purpose**: Full regression testing, customer feedback, release candidates
**Image Storage**: GHCR public registry
**Image Tags**: `staging`, `{version}-staging` (e.g., `1.1.0-staging`)
**Pull Policy**: `Always`

**Workflow**:
```bash
# 1. Promote dev build to staging
cd docker/spark-openmetadata
BUILD_ENV=staging VERSION=1.1.0 ./build.sh  # Prompts for push

# 2. Deploy to staging environment
cd ../..
./deploy-openlakes.sh --env staging

# 3. Run full regression test suite
# Run QA validation
# Gather customer feedback

# 4. If approved, promote to production:
git tag -a v1.1.0 -m "Release 1.1.0"
git push origin v1.1.0  # Triggers GitHub Actions
```

**When to use**: Pre-release testing, QA approval, customer previews

---

### 4. PRODUCTION (Stable Releases)

**Purpose**: Stable, versioned releases for production deployments
**Image Storage**: GHCR public registry
**Image Tags**: `{version}`, `{major}.{minor}`, `{major}`, `latest`
**Pull Policy**: `IfNotPresent` (version-pinned, immutable)

**Workflow** (Automated via GitHub Actions):
```bash
# Manual trigger (creates git tag):
git tag -a v1.1.0 -m "Release 1.1.0: Add Iceberg support"
git push origin v1.1.0

# GitHub Actions automatically:
# 1. Validates semantic version (X.Y.Z)
# 2. Builds all custom images
# 3. Pushes to GHCR with multiple tags:
#    - ghcr.io/openlakes/openlakes-core/spark-openmetadata:1.1.0
#    - ghcr.io/openlakes/openlakes-core/spark-openmetadata:1.1
#    - ghcr.io/openlakes/openlakes-core/spark-openmetadata:1
#    - ghcr.io/openlakes/openlakes-core/spark-openmetadata:latest
# 4. Creates GitHub release with changelog

# Deploy production:
git checkout v1.1.0
./deploy-openlakes.sh --env production
# OR (since production is default):
./deploy-openlakes.sh
```

**When to use**: Production deployments, customer installations, stable releases

---

## Docker Image Development

### Custom Images

OpenLakes maintains custom Docker images for components requiring specialized dependencies:

1. **spark-openmetadata** (required):
   - Base: `apache/spark:4.1.0-preview3`
   - Adds: OpenLineage agent, OpenMetadata transporter, Hadoop AWS + S3A support
   - Location: `docker/spark-openmetadata/`
   - Registry: `ghcr.io/openlakes/openlakes-core/spark-openmetadata`

2. **superset** (future):
   - Base: `apache/superset:5.0.0`
   - Adds: Trino connector, custom drivers
   - Location: `docker/superset/`

### Building Images

All images support four build environments via `BUILD_ENV`:

```bash
# Local build (default)
cd docker/spark-openmetadata
./build.sh

# Dev build (collaborative)
BUILD_ENV=dev ./build.sh

# Staging build (release candidate)
BUILD_ENV=staging VERSION=1.1.0 ./build.sh

# Production build (automated via CI, manual emergency only)
BUILD_ENV=production VERSION=1.1.0 ./build.sh
```

### Dockerfile Conventions

**Required OCI Labels**:
```dockerfile
# Build arguments for dynamic versioning
ARG VERSION=1.0.0
ARG BUILD_DATE
ARG VCS_REF

# OCI labels (required for GHCR association)
LABEL org.opencontainers.image.title="..."
LABEL org.opencontainers.image.description="..."
LABEL org.opencontainers.image.version="${VERSION}"
LABEL org.opencontainers.image.created="${BUILD_DATE}"
LABEL org.opencontainers.image.source="https://github.com/openlakes/openlakes-core"
LABEL org.opencontainers.image.url="https://github.com/openlakes/openlakes-core"
LABEL org.opencontainers.image.vendor="OpenLakes"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL org.opencontainers.image.revision="${VCS_REF}"
```

**Why these labels matter**:
- `source` label associates image with GitHub repository
- Enables automatic linking in GHCR UI
- Provides metadata for image consumers
- Required for compliance and security scanning

---

## Helm Values Architecture

Each layer has environment-specific values files:

```
layers/02-compute/
├── values.yaml           # Production (default) - version-pinned GHCR images
├── values-local.yaml     # Local dev - Rancher Desktop images
├── values-dev.yaml       # Collaborative dev - GHCR dev tag
└── values-staging.yaml   # QA/staging - GHCR staging tag
```

### Values File Precedence

The `deploy-openlakes.sh` script automatically merges values files:

```bash
# Production (no override)
./deploy-openlakes.sh
# Uses: values.yaml only

# Local development
./deploy-openlakes.sh --env local
# Uses: values.yaml + values-local.yaml (local overrides)

# Dev environment
./deploy-openlakes.sh --env dev
# Uses: values.yaml + values-dev.yaml (dev overrides)

# Staging environment
./deploy-openlakes.sh --env staging
# Uses: values.yaml + values-staging.yaml (staging overrides)
```

### Example values-local.yaml

```yaml
spark:
  image:
    repository: spark-openmetadata  # No registry prefix
    tag: "local"
    pullPolicy: Never  # Don't pull from remote
```

### Example values-dev.yaml

```yaml
spark:
  image:
    repository: ghcr.io/openlakes/openlakes-core/spark-openmetadata
    tag: "dev"
    pullPolicy: Always  # Always pull latest dev changes
```

---

## Semantic Versioning

OpenLakes follows strict semantic versioning (X.Y.Z):

### Version Increment Rules

**PATCH (X.Y.Z → X.Y.Z+1)**: Bug fixes, security patches
- Update dependency versions (same major version)
- Security patches
- Bug fixes with no API changes
- Examples:
  - Update Hadoop AWS from 3.4.1 to 3.4.2
  - Fix S3A timeout configuration

**MINOR (X.Y.Z → X.Y+1.0)**: New features, backward-compatible
- Add new JARs or dependencies
- New features that don't break existing functionality
- Backward-compatible API additions
- Examples:
  - Add Apache Iceberg JARs to Spark image
  - Add new Superset connector

**MAJOR (X.Y.Z → X+1.0.0)**: Breaking changes
- Upgrade base image major version
- Remove or change existing APIs
- Incompatible dependency changes
- Examples:
  - Upgrade Spark 4.x to 5.x
  - Remove deprecated features
  - Change OpenLineage agent major version

### Version Examples

```
1.0.0  Initial release
1.0.1  Security: Update Hadoop AWS to 3.4.2
1.0.2  Fix: Correct S3A timeout format
1.1.0  Feature: Add Iceberg support to Spark
1.1.1  Security: Update AWS SDK
1.2.0  Feature: Add Delta Lake support
2.0.0  BREAKING: Upgrade to Spark 5.0
```

---

## Common Development Scenarios

### Scenario 1: Fix a Bug in Spark Image

```bash
# 1. Make changes to Dockerfile
vi docker/spark-openmetadata/Dockerfile

# 2. Build locally
cd docker/spark-openmetadata && ./build.sh

# 3. Test locally
cd ../.. && ./deploy-openlakes.sh --env local

# 4. Verify fix
kubectl logs -f deployment/compute-spark-master -n openlakes

# 5. Commit and push
git add docker/spark-openmetadata/Dockerfile
git commit -m "fix: correct S3A timeout configuration"
git push

# 6. Create PR for team review
```

### Scenario 2: Add New Feature (Team Collaboration)

```bash
# Developer A: Build feature and share
cd docker/spark-openmetadata
# Make changes to Dockerfile
BUILD_ENV=dev ./build.sh  # Pushes to GHCR dev tag

# Developer B: Test the feature
./deploy-openlakes.sh --env dev
# Tests Developer A's changes

# Both: Once approved, merge PR
git checkout main
git pull

# Release manager: Promote to staging
cd docker/spark-openmetadata
BUILD_ENV=staging VERSION=1.2.0 ./build.sh

# QA team: Test staging
./deploy-openlakes.sh --env staging
# Run regression tests

# Release manager: Promote to production
git tag -a v1.2.0 -m "Release 1.2.0: Add Delta Lake support"
git push origin v1.2.0
# GitHub Actions builds and publishes
```

### Scenario 3: Emergency Production Hotfix

```bash
# 1. Create hotfix branch from production tag
git checkout -b hotfix/1.0.1 v1.0.0

# 2. Make minimal fix
vi docker/spark-openmetadata/Dockerfile

# 3. Test locally
cd docker/spark-openmetadata && ./build.sh
cd ../.. && ./deploy-openlakes.sh --env local

# 4. Build staging candidate
cd docker/spark-openmetadata
BUILD_ENV=staging VERSION=1.0.1 ./build.sh

# 5. Quick staging validation
./deploy-openlakes.sh --env staging

# 6. Merge to main and tag
git checkout main
git merge hotfix/1.0.1
git tag -a v1.0.1 -m "Hotfix 1.0.1: Security patch for Hadoop AWS"
git push origin main v1.0.1
```

---

## Guidelines for Claude Sessions

When working on OpenLakes, Claude should:

### 1. Understand Environment Context

**Always ask** which environment the user is targeting:
- Local dev? Use `./build.sh` and `./deploy-openlakes.sh --env local`
- Team collaboration? Use `BUILD_ENV=dev ./build.sh`
- Pre-release? Use `BUILD_ENV=staging VERSION=X.Y.Z ./build.sh`
- Production? Create git tag (automated via GitHub Actions)

### 2. Dockerfile Modifications

When modifying Dockerfiles:
- ✅ **DO**: Keep `ARG VERSION`, `BUILD_DATE`, `VCS_REF`
- ✅ **DO**: Maintain all OCI labels, especially `source` label
- ✅ **DO**: Document why each JAR/dependency is added
- ✅ **DO**: Use specific versions (not `latest`)
- ❌ **DON'T**: Remove or modify OCI labels
- ❌ **DON'T**: Use `latest` tags for dependencies

### 3. Version Bump Decisions

When changes require a new version:
- Bug fix only? → PATCH (X.Y.Z+1)
- New feature, backward-compatible? → MINOR (X.Y+1.0)
- Breaking change? → MAJOR (X+1.0.0)

**Always explain** versioning rationale to the user.

### 4. Values File Changes

When updating image references:
- ✅ **DO**: Update `values.yaml` for production
- ✅ **DO**: Keep environment values files in sync
- ✅ **DO**: Update Chart.yaml `appVersion` to match
- ❌ **DON'T**: Hardcode `latest` tags in production values
- ❌ **DON'T**: Use `Always` pull policy in production

### 5. Testing Recommendations

Always suggest testing workflow:
```bash
# 1. Local test first
./build.sh && ./deploy-openlakes.sh --env local

# 2. Share with team if needed
BUILD_ENV=dev ./build.sh && ./deploy-openlakes.sh --env dev

# 3. Staging validation
BUILD_ENV=staging VERSION=X.Y.Z ./build.sh

# 4. Production release (create tag)
git tag -a vX.Y.Z -m "Release X.Y.Z: <description>"
```

### 6. Documentation Updates

When making significant changes:
- Update `docker/{image}/README.md` with new dependencies
- Update relevant layer README if behavior changes
- Add migration notes for breaking changes
- Update VERSION file if creating new release

---

## File Locations Quick Reference

| What | Where |
|------|-------|
| Spark Dockerfile | `docker/spark-openmetadata/Dockerfile` |
| Spark build script | `docker/spark-openmetadata/build.sh` |
| Spark Helm chart | `layers/02-compute/` |
| Spark production values | `layers/02-compute/values.yaml` |
| Spark local dev values | `layers/02-compute/values-local.yaml` |
| Spark dev values | `layers/02-compute/values-dev.yaml` |
| Spark staging values | `layers/02-compute/values-staging.yaml` |
| Deployment script | `deploy-openlakes.sh` |
| GitHub Actions release | `.github/workflows/release.yml` |
| Project version | `VERSION` |
| This guide | `CLAUDE.md` |

---

## Common Commands Cheat Sheet

```bash
# ─────────────────────────────────────────────────────────────
# Local Development (Fastest)
# ─────────────────────────────────────────────────────────────
cd docker/spark-openmetadata
./build.sh                          # Build local image
cd ../..
./deploy-openlakes.sh --env local   # Deploy with local images

# ─────────────────────────────────────────────────────────────
# Collaborative Development
# ─────────────────────────────────────────────────────────────
cd docker/spark-openmetadata
BUILD_ENV=dev ./build.sh            # Build and push to GHCR dev
cd ../..
./deploy-openlakes.sh --env dev     # Deploy dev environment

# ─────────────────────────────────────────────────────────────
# Staging (Release Candidate)
# ─────────────────────────────────────────────────────────────
cd docker/spark-openmetadata
BUILD_ENV=staging VERSION=1.1.0 ./build.sh
cd ../..
./deploy-openlakes.sh --env staging

# ─────────────────────────────────────────────────────────────
# Production Release (Automated)
# ─────────────────────────────────────────────────────────────
git tag -a v1.1.0 -m "Release 1.1.0"
git push origin v1.1.0
# GitHub Actions builds and publishes automatically

# ─────────────────────────────────────────────────────────────
# Useful Debugging Commands
# ─────────────────────────────────────────────────────────────
# Check which image is running
kubectl get pod -n openlakes -o jsonpath='{.items[*].spec.containers[*].image}'

# View image labels
docker inspect spark-openmetadata:local | jq '.[0].Config.Labels'

# Test local image
docker run --rm spark-openmetadata:local spark-submit --version

# Force pod restart with new image
kubectl rollout restart deployment/compute-spark-master -n openlakes
kubectl rollout status deployment/compute-spark-master -n openlakes
```

---

## Critical Conventions

1. **Always use `org.opencontainers.image.source` label** in Dockerfiles
   - This associates the image with the GitHub repository
   - Required for GHCR UI integration

2. **Never commit with `latest` tag in production `values.yaml`**
   - Production must be version-pinned
   - Use `latest` only in local/dev environments

3. **Follow four-environment promotion**: local → dev → staging → production
   - Each stage has specific purpose and image tags
   - Never skip staging for significant changes

4. **Semantic versioning is strict**:
   - Breaking changes = MAJOR bump
   - New features = MINOR bump
   - Bug fixes = PATCH bump

5. **GitHub Actions is source of truth for production**:
   - Manual production builds are emergency-only
   - Always create git tags for releases
   - Let CI handle the build and publish

---

## Questions to Ask Users

When a user requests changes, Claude should clarify:

1. **"Which environment are you targeting?"**
   - Local dev (just you)? Dev (team collaboration)? Staging (QA)? Production?

2. **"What type of change is this?"**
   - Bug fix (PATCH)? New feature (MINOR)? Breaking change (MAJOR)?

3. **"Do you want to test locally first?"**
   - Recommend local build → test → then push to team

4. **"Should I update the VERSION file?"**
   - If planning a release, update VERSION and Chart.yaml

5. **"Do you need documentation updates?"**
   - README, migration notes, changelog entries?

---

This guide ensures consistent development practices across all Claude sessions working on OpenLakes.
