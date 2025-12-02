# Traefik

## Overview

Traefik provides ingress for every UI/API exposed by OpenLakes Core. It terminates TLS (via Cloudflare DNS-01 or user-provided certificates) and routes requests to the appropriate ClusterIP service.

## Configuration in OpenLakes

- **Deployment:** Layer 01 includes Traefik as a dependency, but RKE2 installations often reuse the built-in HelmChartConfig shown in the docs. Either way, the ingress class is `traefik`.
- **EntryPoints:** `web` (80) and `websecure` (443). Hostnames like `minio.<domain>` or `superset.<domain>` are defined via `IngressRoute` resources.
- **Certificates:** By default, Traefik uses a Let’s Encrypt DNS-01 resolver with Cloudflare tokens. You can remove the ACME settings and supply your own TLS secrets if preferred.
- **Dashboard:** Available at `https://traefik.<domain>/dashboard/` when ingress is enabled.

## Integration points

- All service charts create `IngressRoute` manifests referencing the Traefik ingress class.
- For single-node (Rancher Desktop) setups, Traefik runs inside Kubernetes; for RKE2, you can reuse the built-in Traefik that RKE2 manages via `/var/lib/rancher/rke2/server/manifests/`.
- TLS configuration is centralized so you only need to update the Traefik values once.

## License

Traefik is licensed under the [Apache License 2.0](https://github.com/traefik/traefik/blob/master/LICENSE).
