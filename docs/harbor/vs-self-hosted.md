# Harbor vs Self-Hosted Core

OpenLakes offers two ways to run the same platform: Harbor (managed) and Core (self-hosted). This guide helps you choose the right option.

## Quick Comparison

| Factor | Harbor | Core |
|--------|--------|------|
| **Setup time** | Minutes | Hours to days |
| **Infrastructure** | We manage | You manage |
| **Kubernetes required** | No | Yes |
| **Customization** | Standard configuration | Full control |
| **Data location** | OpenLakes infrastructure | Your infrastructure |
| **Cost model** | Subscription (coming soon) | Your infrastructure costs |
| **Support** | Included | Community (GitHub) |

## Choose Harbor When

**You want to evaluate quickly**

- Start using the platform in minutes, not days
- No need to set up Kubernetes or configure infrastructure
- Ideal for proof-of-concept and evaluation

**You don't have Kubernetes expertise**

- Harbor handles all infrastructure complexity
- No need to manage Helm charts, storage classes, or networking
- Updates and patches applied automatically

**You want managed operations**

- We monitor and maintain the platform
- Harbor Pro (coming Q1 2026) includes SLA guarantees
- Focus on data work, not infrastructure

## Choose Core When

**Data must stay in your environment**

- Regulatory requirements mandate data residency
- Security policies prohibit external data storage
- You need to audit all infrastructure access

**You need full customization**

- Modify component configurations
- Add custom integrations
- Control resource allocation precisely

**You have Kubernetes expertise**

- Your team already operates Kubernetes clusters
- You want to integrate with existing infrastructure
- You prefer managing your own deployments

**Cost optimization is critical**

- Use existing infrastructure capacity
- No subscription fees (only your infrastructure costs)
- Scale resources based on actual usage

## Migration Path

### Harbor Trial → Harbor Pro

Upgrade your trial environment to production:

- Same environment, upgraded infrastructure
- 99% uptime SLA
- Data loss protection
- Coming Q1 2026

### Harbor → Core

Export your work and deploy on your own infrastructure:

1. Export notebooks, DAGs, and configurations from Harbor
2. Deploy Core on your Kubernetes cluster
3. Import your work into the self-hosted environment

Your Iceberg tables, Airflow DAGs, and Superset dashboards use open formats that work in both environments.

### Core → Harbor

Teams sometimes start self-hosted and move to managed:

1. Export your configurations and data
2. Sign up for Harbor
3. Import into your managed environment

## Hybrid Approaches

Some organizations use both:

- **Harbor for development** - Fast iteration without infrastructure overhead
- **Core for production** - Full control over production data

The same open formats (Iceberg, Parquet, standard SQL) work across both environments.

## Still Unsure?

**Start with Harbor Trial** if:

- You're evaluating whether OpenLakes fits your needs
- You want to learn the platform before committing to infrastructure
- You're not sure about your long-term requirements

**Start with Core** if:

- You have strict data residency requirements from day one
- You already have a Kubernetes cluster ready
- You need specific customizations immediately

Questions? Contact [sales@openlakes.io](mailto:sales@openlakes.io) to discuss your specific situation.
