# md-sequencer Kubernetes manifests

Kustomize base + five environment overlays: **local** (Kubernetes in Docker Desktop), **dev**,
**uat**, **prod**, and **prod-dr** — all AWS/EKS. Every environment runs the *same* container
image; only a Kubernetes namespace, a `SPRING_PROFILES_ACTIVE` value, and a handful of resource
sizing/scheduling knobs differ. Everything environment-specific about the *service's own
behavior* (NATS endpoints, leadership namespace, metrics tags) lives in the matching
`application-{env}.yml` baked into the jar — see `../README.md`'s configuration reference.

## Layout

```
k8s/
├── base/
│   ├── core/            deployment.yaml, rbac.yaml, service.yaml — always deployed
│   ├── pdb/              PodDisruptionBudget — only meaningful with 2+ replicas
│   └── observability/    ServiceMonitor — requires the Prometheus Operator CRDs
└── overlays/
    ├── local/    Docker Desktop: 1 replica, tiny resources, ships its own NATS pod
    ├── dev/      AWS/EKS: 2 replicas, 1 CPU / 2Gi
    ├── uat/      AWS/EKS: 2 replicas, 2 CPU / 4Gi
    ├── prod/     AWS/EKS: 2 replicas, 4 CPU / 8Gi, pinned to a static-CPU-manager node pool
    └── prod-dr/  AWS/EKS: same tier as prod, independent region/namespace/NATS cluster
```

`pdb/` and `observability/` are each tiny one-resource kustomizations rather than loose files
because Kustomize requires anything an overlay pulls in from outside its own directory tree to be
a full kustomization directory — that's also why they're separate from `core/` instead of one
`base/` directory: the `local` overlay deliberately doesn't include either of them (no HA to
protect with a PDB at 1 replica, and Docker Desktop clusters don't usually have the Prometheus
Operator installed).

## How an overlay differs from base

Each `overlays/<env>/kustomization.yaml`:
- sets the Kubernetes **namespace** for every resource
- rewrites the **image** to that environment's registry/tag via the `images:` transformer
- generates a `gcm-md-sequencer-config` **ConfigMap** containing exactly one key,
  `SPRING_PROFILES_ACTIVE=<env>` — this one variable is what selects every other
  environment-specific value, since it activates the matching `application-{env}.yml`
- applies `patch-deployment.yaml`, a partial `Deployment` patch for whatever else needs to scale
  with environment tier: replica count, CPU/memory requests+limits, JVM heap flags, and (prod/
  prod-dr only) the `cpuManagerPolicy: static` node selector

## Before you apply any of these

Every file below has a placeholder that must be replaced with a real value before deploying:

| Placeholder | Where | Replace with |
|---|---|---|
| `<NONPROD_ACCOUNT_ID>` | `overlays/dev`, `overlays/uat` `kustomization.yaml` | AWS account ID hosting the dev/uat ECR repo |
| `<PROD_ACCOUNT_ID>` | `overlays/prod`, `overlays/prod-dr` `kustomization.yaml` | AWS account ID hosting the prod ECR repo (same account, two regions) |
| `nats.dev.md-platform.internal`, `nats.uat...`, `nats.prod...`, `nats.prod-dr...` | `md-sequencer/src/main/resources/application-{env}.yml` | The real DNS name of each environment's NATS cluster |

**`prod-dr` must point at an independent NATS cluster from `prod`** (different region, different
underlying infrastructure) — pointing both at the same NATS defeats the entire point of a DR
environment. See the note in `application-prod.yml`.

## Building and applying

```
# Preview the fully-rendered manifests for any environment:
kubectl kustomize k8s/overlays/dev

# Apply directly:
kubectl apply -k k8s/overlays/local
kubectl apply -k k8s/overlays/dev
kubectl apply -k k8s/overlays/uat
kubectl apply -k k8s/overlays/prod
kubectl apply -k k8s/overlays/prod-dr
```

### Local (Docker Desktop)

```
mvn -pl md-sequencer -am install
docker build -t gcm-md-sequencer:local -f md-sequencer/Dockerfile .
kubectl apply -k md-sequencer/k8s/overlays/local
```

The `local` overlay also deploys a single-node NATS pod (`nats.yaml`, JetStream enabled) in the
same namespace — there's no shared NATS infrastructure on a laptop, unlike every AWS environment.
`imagePullPolicy: Never` is set so the cluster uses the image you just `docker build`ed instead of
trying to pull it from a registry.

### AWS environments

Each expects an already-existing, independently-operated NATS cluster and an EKS node pool
(`prod`/`prod-dr` additionally expect a node pool labeled `cpuManagerPolicy: static` with
exclusive-core CPU management enabled, per the project spec's throughput requirements). Push the
built image to the environment's ECR repo at the tag referenced in that overlay's
`kustomization.yaml` (`dev-latest`, `uat-latest`, `prod-latest`) before applying.

## Why prod-dr isn't just "prod with more replicas"

`prod-dr` is a **fully separate, independently-active deployment** in its own namespace
(`md-sequencer-dr`), pointed at its own NATS cluster, electing leadership over its own Kubernetes
Lease. There is no cross-region coordination between `prod` and `prod-dr` — the sequencer's
leader-election and sequence-block-leasing are both scoped to a single cluster's API server and a
single NATS cluster's KV bucket, by design (see `../README.md`). Promoting DR to system-of-record
during a regional prod outage is a DNS/traffic-cutover decision made outside this service (e.g.
by whatever redirects line handlers and consumers to the DR NATS cluster), not something the two
deployments negotiate between themselves.
