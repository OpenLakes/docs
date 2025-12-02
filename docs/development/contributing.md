# Contributing to OpenLakes

Thank you for your interest in contributing to OpenLakes! This guide explains how to set up your development environment and submit contributions.

## Getting Started

### Prerequisites

- **Git**: For version control
- **Docker**: For building custom images
- **kubectl**: Kubernetes CLI
- **Helm**: v3.10+
- **Kubernetes cluster**: Rancher Desktop (local) or RKE2 (multi-node)

### Fork and Clone

```bash
# Fork the repository on GitHub, then:
git clone https://github.com/YOUR-USERNAME/openlakes-core.git
cd openlakes-core
git remote add upstream https://github.com/openlakes-io/openlakes-core.git
```

### Development Environment

For local development, we recommend Rancher Desktop:

1. Install [Rancher Desktop](https://rancherdesktop.io/)
2. Enable Kubernetes in preferences
3. Allocate at least 10 CPU cores and 18 GB RAM

## Development Workflow

### 1. Create a Branch

```bash
git checkout main
git pull upstream main
git checkout -b feature/your-feature-name
```

### 2. Make Changes

OpenLakes follows these conventions:

- **Helm charts**: Located in `layers/`
- **Custom Docker images**: Located in `docker/`
- **Configuration**: Centralized in `core-config.yaml`
- **Documentation**: Located in the `openlakes-site` repository

### 3. Test Locally

```bash
# Build custom images locally
cd docker/spark
./build.sh  # Builds with BUILD_ENV=local

# Deploy to local cluster
cd ../..
./deploy-openlakes.sh --env local

# Verify deployment
kubectl get pods -n openlakes
```

### 4. Commit Changes

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```bash
git add .
git commit -m "feat: add Iceberg support to Spark image"
# OR
git commit -m "fix: correct S3A timeout configuration"
# OR
git commit -m "docs: update deployment guide"
```

### 5. Submit Pull Request

```bash
git push origin feature/your-feature-name
```

Then open a Pull Request on GitHub against the `main` branch.

## Code Style

### Helm Charts

- Use 2-space indentation in YAML files
- Include comments for non-obvious configurations
- Follow existing patterns in `values.yaml` files
- Test with `helm lint` before committing

### Dockerfiles

- Include OCI labels (see `docker/spark/Dockerfile` for example)
- Pin dependency versions explicitly
- Document why each dependency is added
- Use multi-stage builds where appropriate

### Shell Scripts

- Use `#!/bin/bash` shebang
- Include error handling (`set -e`)
- Add comments for complex logic
- Test on both macOS and Linux

## Testing

### Local Testing

```bash
# Deploy and test locally
./deploy-openlakes.sh --env local

# Check pod status
kubectl get pods -n openlakes

# View logs
kubectl logs -f deployment/compute-spark-master -n openlakes

# Test specific functionality
kubectl exec -it deployment/compute-spark-master -n openlakes -- spark-submit --version
```

### Integration Testing

Before submitting a PR, verify:

1. All pods reach `Running` state
2. Services are accessible via ingress
3. Basic workflows function (e.g., Spark job submission)
4. No regressions in existing functionality

## Documentation

Documentation lives in the `openlakes-site` repository:

```bash
git clone https://github.com/openlakes-io/openlakes-site.git
cd openlakes-site

# Install dependencies
pip install -r requirements.txt

# Serve locally
mkdocs serve

# View at http://localhost:8000
```

### Documentation Guidelines

- Use clear, concise language
- Include code examples where helpful
- Update relevant docs when changing functionality
- Add screenshots for UI-related changes

## Versioning

OpenLakes follows [Semantic Versioning](https://semver.org/):

- **PATCH** (x.y.Z): Bug fixes, security patches
- **MINOR** (x.Y.0): New features, backward-compatible
- **MAJOR** (X.0.0): Breaking changes

!!! warning "Development Phase"
    During active development (pre-1.0.0), all custom images use version `1.0.0`. Do not increment versions until the official release.

## Issue Reporting

When reporting issues, include:

1. **Environment**: Rancher Desktop/RKE2, OS, Kubernetes version
2. **Steps to reproduce**: Exact commands run
3. **Expected behavior**: What should happen
4. **Actual behavior**: What actually happened
5. **Logs**: Relevant pod logs (`kubectl logs`)

## Getting Help

- **GitHub Issues**: For bugs and feature requests
- **GitHub Discussions**: For questions and ideas
- **Documentation**: [openlakes.io/docs](https://openlakes.io/docs)

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
