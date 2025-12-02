# Developer Guide

This guide covers the development workflow for building and modifying OpenLakes components.

## Architecture Overview

OpenLakes consists of:

- **8 Helm chart layers** in `layers/`
- **Custom Docker images** in `docker/`
- **Deployment script** `deploy-openlakes.sh`
- **Central configuration** `core-config.yaml`

## Four-Environment Workflow

OpenLakes uses a four-tier promotion workflow:

| Environment | Image Tag | Pull Policy | Use Case |
|-------------|-----------|-------------|----------|
| **Local** | `local` | `Never` | Individual developer iteration |
| **Dev** | `dev` | `Always` | Team collaboration |
| **Staging** | `staging` | `Always` | QA and release candidates |
| **Production** | `1.0.0` | `IfNotPresent` | Stable releases |

### Local Development

Fastest iteration cycle using Rancher Desktop local images:

```bash
# Build image locally (no push)
cd docker/spark
./build.sh  # Uses BUILD_ENV=local by default

# Deploy with local images
cd ../..
./deploy-openlakes.sh --env local

# Test changes
kubectl logs -f deployment/compute-spark-master -n openlakes
```

### Collaborative Development

Share work-in-progress with team via GHCR:

```bash
# Build and push to GHCR dev tag
cd docker/spark
BUILD_ENV=dev ./build.sh

# Team members deploy dev environment
./deploy-openlakes.sh --env dev
```

### Staging

Full regression testing before release:

```bash
# Build staging candidate
cd docker/spark
BUILD_ENV=staging VERSION=1.0.0 ./build.sh

# Deploy staging environment
./deploy-openlakes.sh --env staging

# Run QA tests
```

### Production Release

Automated via GitHub Actions:

```bash
# Create release tag
git tag -a v1.0.0 -m "Release 1.0.0"
git push origin v1.0.0

# GitHub Actions automatically:
# - Builds all images
# - Pushes to GHCR with version tags
# - Creates GitHub release
```

## Building Custom Images

### Spark Image

The Spark image includes OpenLineage, S3A, and Iceberg dependencies:

```bash
cd docker/spark

# Local build
./build.sh

# Dev build (pushes to GHCR)
BUILD_ENV=dev ./build.sh

# Staging build
BUILD_ENV=staging VERSION=1.0.0 ./build.sh
```

### Dashboard Image

React-based service portal:

```bash
cd docker/openlakes-dashboard

# Local build
./build.sh

# Production build
BUILD_ENV=production VERSION=1.0.0 ./build.sh
```

### JupyterHub Notebook Image

Custom notebook environment with Spark integration:

```bash
cd docker/jupyterhub-notebook
./build.sh
```

## Modifying Helm Charts

### Values File Structure

Each layer has environment-specific values:

```
layers/02-compute/
├── Chart.yaml
├── values.yaml           # Production defaults
├── values-local.yaml     # Local overrides
├── values-dev.yaml       # Dev overrides
├── values-staging.yaml   # Staging overrides
└── templates/
```

### Adding a New Component

1. Create templates in the appropriate layer
2. Add configuration to `values.yaml`
3. Update `core-config.yaml` if needed
4. Test with local deployment

Example: Adding a new service to infrastructure layer:

```yaml
# layers/01-infrastructure/values.yaml
myservice:
  enabled: true
  image:
    repository: myservice/image
    tag: "1.0.0"
  resources:
    requests:
      memory: "256Mi"
      cpu: "100m"
```

### Testing Chart Changes

```bash
# Lint the chart
helm lint layers/01-infrastructure

# Dry-run deployment
helm upgrade --install 01-infrastructure ./layers/01-infrastructure \
  --namespace openlakes \
  --dry-run

# Deploy specific layer
helm upgrade --install 01-infrastructure ./layers/01-infrastructure \
  --namespace openlakes \
  --timeout 10m \
  --wait
```

## Configuration Management

### core-config.yaml

Central configuration file that drives all deployments:

```yaml
cluster:
  mode: auto              # auto|single|multi
  namespace: openlakes
  storageClass: longhorn

credentials:
  minio:
    rootUser: admin
    rootPassword: admin123
  # ... other credentials

storage:
  tiers:
    hot:
      - node: worker-1
        paths: ["/minio1", "/minio2"]
    # ... other tiers
```

### Environment Variables

The deployment script reads configuration and exports environment variables:

| Variable | Description |
|----------|-------------|
| `NAMESPACE` | Target namespace |
| `TIMEOUT` | Helm timeout |
| `DRY_RUN` | Enable dry-run mode |
| `ENV` | Target environment |

## Debugging

### Pod Issues

```bash
# Check pod status
kubectl get pods -n openlakes

# View pod logs
kubectl logs -f <pod-name> -n openlakes

# Describe pod for events
kubectl describe pod <pod-name> -n openlakes

# Execute into pod
kubectl exec -it <pod-name> -n openlakes -- /bin/bash
```

### Helm Issues

```bash
# List releases
helm list -n openlakes

# View release history
helm history <release-name> -n openlakes

# Rollback release
helm rollback <release-name> <revision> -n openlakes

# Get deployed values
helm get values <release-name> -n openlakes
```

### Storage Issues

```bash
# Check PVCs
kubectl get pvc -n openlakes

# Check PVs
kubectl get pv

# Describe storage issues
kubectl describe pvc <pvc-name> -n openlakes
```

## Common Tasks

### Updating Component Version

1. Update image tag in `values.yaml`
2. Update `Chart.yaml` appVersion
3. Test with local deployment
4. Submit PR

### Adding New Ingress Route

1. Add IngressRoute template to appropriate layer
2. Configure host in `values.yaml`
3. Update dashboard links if needed
4. Test with local deployment

### Modifying Credentials

1. Update `core-config.yaml`
2. Run `./deploy-openlakes.sh` to apply
3. Helm rolls affected pods automatically

## File Reference

| Path | Description |
|------|-------------|
| `deploy-openlakes.sh` | Main deployment script |
| `core-config.yaml` | Central configuration |
| `VERSION` | Project version |
| `layers/` | Helm charts |
| `docker/` | Custom Docker images |
| `examples/` | Tutorial notebooks |
| `.github/workflows/` | CI/CD workflows |

## Best Practices

1. **Always test locally first** before pushing to dev
2. **Use explicit versions** for all dependencies
3. **Document breaking changes** in commit messages
4. **Keep values files in sync** across environments
5. **Follow existing patterns** in the codebase
