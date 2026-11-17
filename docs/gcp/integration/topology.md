# Topology contract (dual-substrate Kubernetes)

The invariants an integration must preserve. Facts (objects, ports, env, images)
are generated into [`contract.yaml`](contract.yaml); this page explains *why* the
shape is what it is and what must not be relaxed.

## Target shape

```
        client (real cloud hostnames, HTTPS)
                     │
              ┌──────▼───────┐
              │ istio-system │  optional front door: TLS + host routing
              │  Gateway     │  (HTTP(S) only; no mesh inside a substrate)
              └──┬────────┬──┘
      *.amazonaws│        │*.googleapis.com / *.run.app / *.cloudfunctions.net
       ┌─────────▼──┐  ┌──▼───────────────┐
       │ jaiscloud  │  │ jaiscloud-gcp    │
       │ AWS :4566  │  │ REST :8080       │
       │            │  │ gRPC :8081 (h2c) │
       │            │  │ HMS  :9083-9093  │
       └─────┬──────┘  └──┬───────────────┘
             │            │  (in-cluster clients use Service DNS directly)
             └─────┬──────┘
             ┌─────▼──────────┐
             │ jaiscloud-data │  one PostgreSQL server
             │                │  databases: jaiscloud, jaiscloud_gcp
             └────────────────┘
```

## Namespaces and ownership

| namespace | role | owns |
|---|---|---|
| `jaiscloud` | AWS substrate | Deployment `jaiscloud`, `jaiscloud-blobs-pvc`, `jaiscloud-state-pvc`, Secret `jaiscloud-dsn`, Service `jaiscloud:4566`, ServiceAccount `jaiscloud` |
| `jaiscloud-gcp` | GCP substrate | Deployment `jaiscloud-gcp`, `jaiscloud-gcp-blobs-pvc`, `jaiscloud-gcp-state-pvc`, Secret `jaiscloud-gcp-dsn`, Service `jaiscloud-gcp:{8080,8081,9083..9093}`, ServiceAccount `jaiscloud-gcp` |
| `jaiscloud-data` | shared store | Deployment `postgres`, `postgres-pvc`, Secret `postgres-secret` (server env), Services `postgres:5432` / `postgres-external:30543` |
| `istio-system` | optional front door | `istio-ingressgateway`, `Gateway jaiscloud`, TLS secret `jaiscloud-wildcard`, CA ConfigMap `jaiscloud-ca-cert` (also copied into each substrate namespace for managed workloads) |

## Install intent and state

Presence is not intent: a cluster can hold only AWS on purpose, and a leftover
GCP namespace can be a leak rather than an invitation to deploy GCP. `deploy.sh`
records the intent in ConfigMap `jaiscloud-install` in `jaiscloud-data`.

| key | value |
|---|---|
| `schema` | `1` |
| `installed` | comma list of clouds that were installed, e.g. `aws,gcp` |
| `desired` | comma list of `cloud=state`, e.g. `aws=running,gcp=parked` |

The record lives with the store, so a substrate reset (which deletes only its own
namespace) never loses it. **`deploy.sh` owns this object; no static manifest
declares it** — a manifest apply would reset `installed`/`desired` to a fixed
value and erase the operator's intent. Every path that changes what runs records
it: install (`aws=running`), `--gcp` (`gcp=running`), `--delete` (`aws=absent`),
`--reset` / `--gcp-reset` (back to `running`), and `--migrate-store` (`aws` and
`gcp` both `running`). When `jaiscloud-data` does not exist yet, the write is
skipped with a warning rather than creating the store just to record intent.

Desired states are `running`, `parked` (Deployment scaled to 0; namespaces, PVCs
and databases survive) and `absent` (no Deployment). The preflight derives the
observed `substrates.<cloud>.state` from the cluster and additionally reports
`degraded` (present and scaled up, but nothing available). The JSON carries
`replicas` (desired scale) and `imageID` (the digest the pod actually runs, per
the first pod container status) next to `present`/`ready`.

`deploy/deploy.sh --check` resolves intent in this order: `--substrate` /
`--desired` flags, then the `jaiscloud-install` record, then inference from the
substrate namespaces/Deployments (`intent.source` = `flag|cluster|inferred`).
Advice is relative to that intent:

- only an installed cloud gets a `deploy-<cloud>` recommendation, and it is
  non-optional when that cloud's state is `absent`;
- a cloud outside `installed` that still has a namespace, Deployment or database
  is reported as a leak warning (`unexpected <cloud> footprint (intent=...)`);
- `--strict` exits 1 when any non-optional recommendation or leak warning
  exists; optional work (the mesh) never fails it.

## Invariants

1. **The store outlives the substrates.** `jaiscloud-data` is never deleted by a
   substrate reset. `deploy.sh --reset` drops only the `jaiscloud` database and
   deletes only the `jaiscloud` namespace; `--gcp-reset` drops only
   `jaiscloud_gcp` and the GCP blobs PVC.
2. **Secrets do not cross namespaces.** `env.valueFrom.secretKeyRef` cannot, so
   each substrate namespace owns its DSN secret (`jaiscloud-dsn`,
   `jaiscloud-gcp-dsn`). The server's `postgres-secret` lives with the server.
3. **One database per cloud.** Schema isolation (`search_path=aws|gcp`) means one
   database serves one instance per cloud: `jaiscloud` for AWS, `jaiscloud_gcp`
   for GCP. Do not point both substrates at one database.
4. **State is not blob data.** The emulator's state dir (instance-ID file,
   platform TLS key) is a small PVC (`*-state-pvc`), never the blob PVC: the blob
   store snapshots its base directory recursively, so state there would be
   captured as S3 objects (including a private key). The blob PVC root stays the
   S3 base dir (`/data/blobs`) so existing objects keep their layout.
5. **Each substrate has one identity.** `JAISCLOUD_INSTANCE_ID` is pinned
   (`aws-substrate-1`, `gcp-substrate-1`) and workloads carry it, so one
   substrate's startup sweep cannot reap the other's. Namespaces the emulator
   creates carry the same identity in `jaiscloud.io/substrate` and
   `jaiscloud.io/instance-id`, and `ListManagedNamespaces` /
   `SweepManagedNamespaces` / `DeleteManagedNamespace` filter on them: a
   namespace the other substrate (or another instance of this one) created is
   invisible to the sweep and refused by delete. Parallel deployments on a
   shared cluster need distinct IDs *and* namespaces.
6. **The mesh is only the front door.** Nothing inside a substrate transits it:
   Spark drivers/executors, Cloud Run revision Pods, function Pods and Managed
   Kafka brokers talk to the emulator over Service DNS. The gateway never
   rewrites `:authority` (GKE vs Managed Kafka share `/v1/.../clusters`; Cloud
   Run/Cloud Functions hosts and S3 virtual-hosted requests carry identity).
7. **Executors stay optional.** The k8s executor modes (Dataproc, Managed Kafka,
   Cloud Run, functions) are the only cluster-dependent features; every engine
   defaults to `mock` and the binary stays usable with no cluster, no mesh and no
   Postgres. Do not make the mesh, RBAC or a DSN required to start the binary.
8. **Per-resource namespaces are the emulator's.** Dataproc clusters and Managed
   Kafka brokers create `gcp-*` namespaces labelled
   `jaiscloud.io/managed-by=jaiscloud`, `jaiscloud.io/service=<svc>` and the
   identity labels above, and bind the executor ClusterRole there for the
   emulator's *own* resolved ServiceAccount (the RoleBinding carries the same
   labels). Those namespaces are deleted with their resource; adopted namespaces
   are left alone. A namespace an older emulator created before identity labels
   existed is backfilled in place when it is next ensured, and is otherwise left
   untouched — the sweep cannot prove it is ours.
9. **Reaching each substrate.** The AWS Service is a `LoadBalancer` (localhost
   convenience on Docker Desktop; pending on kind/k3d). The GCP Service is a
   `ClusterIP` because it also carries the Thrift HMS ports — use a port-forward
   (`8080`, `8081`) or the front door for external HTTP(S).
10. **Namespace isolation is enforced by admission, not by convention.** Both
    substrate ServiceAccounts hold `jaiscloud-namespace-admin`, a cluster-scoped
    ClusterRole with namespace create/delete plus update/patch (the identity-label
    backfill; per-cluster Dataproc namespaces, Managed Kafka brokers), and RBAC
    cannot scope a delete by label.
    `deploy/k8s/rbac-admission-policy.yaml` installs a
    `ValidatingAdmissionPolicy` + `Deny` binding (`failurePolicy: Fail`) that
    matches only those two ServiceAccounts and requires a namespace
    create/update/delete to carry `jaiscloud.io/managed-by=jaiscloud` and the
    caller's own `jaiscloud.io/substrate`; a namespace already owned by the
    other substrate cannot be relabelled or deleted. RoleBindings created by
    those identities must carry the same labels. **Prerequisite: Kubernetes
    >= 1.30** (ValidatingAdmissionPolicy is GA there); `deploy/deploy.sh`
    refuses an older cluster before creating anything, and
    `scripts/rbac-isolation-gate.sh` proves the allow/deny matrix with
    server-side dry runs. An operator's own `kubectl` is not matched, so the
    documented reset paths (`--reset` deletes the `jaiscloud` namespace) keep
    working.
11. **Upgrade is a data move.** The pre-split layout ran Postgres and the GCP
    emulator in `jaiscloud`; a PVC cannot change namespace. `--migrate-store`
    dumps the old store into both databases, copies the old GCP blob PVC into
    `jaiscloud-gcp`, removes the legacy workloads and keeps both old PVCs as
    rollback copies. Running the new manifests without it silently starts
    against an empty store.

## Where each fact lives

- **Objects, ports, env, images, PVCs, secrets (keys), RBAC subjects**:
  [`contract.yaml`](contract.yaml), generated from the manifests.
- **The namespace boundary**: `deploy/k8s/rbac-admission-policy.yaml`
  (ValidatingAdmissionPolicy + binding), reported by the preflight as
  `rbac.admissionPolicy` and verified by `scripts/rbac-isolation-gate.sh`.
- **Commands, detection predicates, failure taxonomy, destructive operations**:
  the `playbook` section of `contract.yaml` (authored in
  [`playbook.yaml`](playbook.yaml)).
- **Read-only cluster state**: `deploy/deploy.sh --check [--json]`
  (`scripts/integration-preflight.sh`).
- **The same pack without the source tree**: `jaiscloud-gcp agent-guide`
  (brief), `--json` (contract), `--emit DIR` (manifests + scripts + docs); both
  binaries embed it.
- **Human narrative**: [`README-GCP.md`](../../../README-GCP.md),
  [`DEVELOPER_GUIDE.md`](../../../DEVELOPER_GUIDE.md) Setup 3,
  [`deploy/k8s/istio/README.md`](../../../deploy/k8s/istio/README.md),
  [`docs/gcp/ci.md`](../ci.md).
