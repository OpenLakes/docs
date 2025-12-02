# openlakes-streaming

OpenLakes streaming layer - real-time data processing


## Chart Info

| Property | Value |
|----------|-------|
| Version | `1.0.0` |
| App Version | `N/A` |
| Type | application |

## Configuration

Key configuration values (see `values.yaml` for full reference):

```yaml
global:
  networking:
    type: ClusterIP  # Service type (ClusterIP + Ingress for external access via Traefik)
    ingressClass: traefik
    domain: openlakes.dev
  storageClass: longhorn
  namespace: openlakes

# ═══════════════════════════════════════════════════════════════
# Streaming Layer (Layer 03)
# ═══════════════════════════════════════════════════════════════
# This layer is reserved for future streaming components.
# Spark Structured Streaming is available in Layer 02 (Compute).
#
# For streaming workloads, use:
#   - Spark Structured Streaming (micro-batch, exactly-once semantics)
#   - Kafka Streams (embedded in applications)
#
# To add custom streaming engines, create templates in this layer.

```