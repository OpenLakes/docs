# openlakes-monitoring

OpenLakes monitoring layer - Prometheus, Grafana, Loki, and application exporters


## Chart Info

| Property | Value |
|----------|-------|
| Version | `1.0.0` |
| App Version | `N/A` |
| Type | application |

## Dependencies

| Chart | Version | Repository |
|-------|---------|------------|
| kube-prometheus-stack | `68.2.2` | https://prometheus-community.github.io/helm-charts |
| loki | `6.23.0` | https://grafana.github.io/helm-charts |
| promtail | `6.16.6` | https://grafana.github.io/helm-charts |
| prometheus-kafka-exporter | `2.12.0` | https://prometheus-community.github.io/helm-charts |
| prometheus-postgres-exporter | `6.4.0` | https://prometheus-community.github.io/helm-charts |
| prometheus-statsd-exporter | `0.15.0` | https://prometheus-community.github.io/helm-charts |

## Configuration

Key configuration values (see `values.yaml` for full reference):

```yaml
global:
  networking:
    type: Ingress
    ingressClass: traefik
    domain: openlakes.dev
    enableIngressRoutes: true  # Create Traefik IngressRoutes for hostname routing
  storageClass: longhorn
  namespace: openlakes

# ═══════════════════════════════════════════════════════════════
# Prometheus + Grafana + Alertmanager Stack
# ═══════════════════════════════════════════════════════════════
# Complete monitoring solution for Kubernetes and OpenLakes
# Includes pre-configured dashboards, alerts, and metrics collection
prometheus:
  enabled: true

kube-prometheus-stack:
  fullnameOverride: monitoring

  # ═══════════════════════════════════════════════════════════════
  # Grafana - Visualization and Dashboards
  # ═══════════════════════════════════════════════════════════════
  grafana:
    enabled: true
    fullnameOverride: monitoring-grafana

    # Admin credentials
    adminPassword: admin123  # Change in production!

    # Service configuration
    service:
      type: ClusterIP
      port: 3000

    # Ingress handled by Traefik IngressRoute (see templates/grafana-ingress.yaml)
    ingress:
      enabled: false

    # Persistence for dashboards
    persistence:
      enabled: true
      size: 5Gi
      storageClassName: longhorn

    # Additional data sources
    additionalDataSources:
    - name: Loki
      type: loki
      access: proxy
# ... (truncated)
```