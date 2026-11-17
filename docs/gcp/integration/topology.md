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

Each substrate's objects come from that substrate's own manifests: the AWS
substrate from `namespace-aws.yaml` + `rbac-shared.yaml` + `rbac-aws.yaml`, the
GCP substrate from `namespace-gcp.yaml` + `rbac-shared.yaml` + `rbac-gcp.yaml`,
and the shared store from `namespace-data.yaml` + `postgres.yaml`. Installing one
substrate creates no namespace, ServiceAccount or binding for the other; the
shared objects are the store and `rbac-admission-policy.yaml`. The
`jaiscloud-namespace-admin` ClusterRole is bound per substrate
(`jaiscloud-namespace-admin-aws` / `jaiscloud-namespace-admin-gcp`), so a
single-substrate install grants namespace admin to its own identity only.

## Install intent and state

Presence is not intent: a cluster can hold only AWS on purpose, and a leftover
GCP namespace can be a leak rather than an invitation to deploy GCP. `deploy.sh`
records the intent in ConfigMap `jaiscloud-install` in `jaiscloud-data`.

| key | value |
|---|---|
| `schema` | `1` |
| `installed` | comma list of clouds that were installed, e.g. `aws,gcp` |
| `desired` | comma list of `cloud=state`, e.g. `aws=running,gcp=parked` |
| `store` | `bundled` (PostgreSQL Deployment in `jaiscloud-data`) or `external` (a server the operator brought) |

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

## Lifecycle: running, parked, deleted, reset

Presence is not the same as being allowed to drop data. The lifecycle modes are
deliberately separated by *what they destroy*:

| mode | substrate compute | namespaces | PVCs (blobs + state) | databases | resource records (instance ID, LROs, topic/service records) |
|---|---|---|---|---|---|
| `--park <cloud>` | terminated (Deployment scaled to 0, engine runtimes stopped, executor Pods/Jobs reaped) | kept | kept | kept | kept (Managed Kafka **topics** excepted — no broker PVC) |
| `--resume <cloud>` | recreated (scale to 1, then `POST /_jaiscloud/resume`) | kept | kept | kept | kept; refuses a changed binary unless `--allow-upgrade`, else pins the parked digest |
| `--switch <target>` | other substrate parked, target resumed | kept | kept | kept | kept |
| `--delete [aws\|gcp\|both]` | removed (Deployment/Service/DSN Secret; AWS ConfigMap too) | kept | kept | kept | kept |
| `--reset` / `--gcp-reset` | removed, then reinstalled | **deleted** for that substrate | **deleted** (blobs; GCP keeps its state PVC) | **dropped and recreated** for that substrate | **lost** for that substrate |

The park record is **per substrate**: the namespace carries
`jaiscloud.io/<cloud>.state`, `.parked-at`, `.parked-image`,
`.parked-image-id` and `.parked-version` (`cloud` = `aws` | `gcp`), next to the
per-cloud `desired` in the install record. Two substrates sharing one namespace
therefore keep both records instead of the second park overwriting the first.

Park is the only deliberate compute teardown that never touches data. Before the
Deployment scales down, the emulator makes its own control plane honest through
`POST /_jaiscloud/park`:

- **Dataproc** — every non-terminal job is cancelled (`CancelJob`), so no job is
  left RUNNING with a dead driver. Clusters are control-plane records and are
  left alone; a RUNNING cluster with no driver is normal and the next job creates
  a driver.
- **Managed Kafka** — every cluster's broker Pod and Service is deleted, but not
  the per-cluster namespace and not the cluster/topic metadata. **Kafka topics
  are lost**: the broker has no PVC, so its log dir dies with the Pod. Topic
  records survive and are recreated on the broker by the next data-plane call.
  `Resume` starts a fresh broker per cluster. This is surfaced, not silent: the
  `/park` result carries `"topicDataLost": true` when a real broker was stopped,
  `deploy.sh --park gcp` warns when `JAISCLOUD_KAFKA_BROKER_MODE` is
  `k8s`/`docker`/`native`, and the preflight warns (and reports
  `substrates.gcp.kafkaBrokerMode`). A broker PVC is deferred (the broker is
  ephemeral by design); topic persistence is not claimed.
- **Cloud Run** — every service's revision runtimes are removed; the Service and
  Revision records stay. `Resume` recreates the runtime for the service's
  latest-ready revision only.
- **Cloud Functions** — no hook: each invocation's Pod is reaped by the
  workload teardown.

Long-running operations (Dataproc job submission, Managed Kafka/Cloud Run LROs)
are persisted records with their stored terminal state, not live goroutines:
park does not rewrite them, and an LRO that was already done stays done. Use the
emulator's operation read to resettle anything outstanding before parking.

`deploy.sh --park` reaches the emulator through a temporary port-forward on a
free port in `18080–18089` and requires `curl`; a `404` means the running image
predates the park/resume API (rebuild and redeploy it). A failed hook is fatal
unless `PARK_FORCE=1`, which warns and continues with the scale/teardown.

Both substrate images are tagged `:latest` for now (`imagePullPolicy:
IfNotPresent`), so the digest a pod runs is whatever the node cached. A resume
must not silently swap the binary (and run its store migrations):

- `--resume` compares the Deployment image with the parked one *before* scaling;
  a different image is refused with `--allow-upgrade` named as the opt-in.
- Without `--allow-upgrade` it pins the parked `imageID` digest
  (`kubectl set image deployment/<dep> <dep>=<repo>@sha256:…`) before scaling, so
  a moving `:latest` cannot slip a new build in behind the tag. With no recorded
  digest it warns that it cannot pin, and the post-rollout digest comparison
  stays a warning. `--allow-upgrade` skips the pin and warns that the store may
  migrate.
- `deploy.sh --check` still warns about `:latest` images; pin a release tag when
  one is available — DSL7 tracks that work.

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
   (`8080`, `8081`) or the front door for external HTTP(S). In-cluster callers
   use Service DNS and need no mesh object; for a literal-IP dialer, a
   `hostAliases` entry or an explicitly pinned `clusterIP`, and the sidecar-client
   path, see
   [`deploy/k8s/istio/README.md`](../../../deploy/k8s/istio/README.md#addressing-the-substrates).
10. **Namespace isolation is enforced by admission, not by convention.** Each
    substrate ServiceAccount is bound to `jaiscloud-namespace-admin`, a
    cluster-scoped ClusterRole with namespace create/delete plus update/patch
    (the identity-label backfill; per-cluster Dataproc namespaces, Managed Kafka
    brokers), through its own ClusterRoleBinding
    (`jaiscloud-namespace-admin-aws` / `jaiscloud-namespace-admin-gcp`), and RBAC
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
    The policy matches the literal usernames
    `system:serviceaccount:jaiscloud:jaiscloud` and
    `system:serviceaccount:jaiscloud-gcp:jaiscloud-gcp`, so renaming a namespace
    means editing the policy too.
11. **Upgrade is a data move, not a teardown.** The pre-split layout ran Postgres
    and the GCP emulator in `jaiscloud`; a PVC cannot change namespace.
    `--migrate-store` dumps the old store (a `Deployment` or `StatefulSet`,
    `LEGACY_POSTGRES_WORKLOAD`, or the server in `LEGACY_DSN` — dumped from the
    new store pod) into both databases, copies the GCP blob claim when the legacy
    GCP emulator exists and the AWS claim when the legacy namespace differs, and
    **keeps** the legacy workload, Services and PVCs by default — the Postgres is
    only scaled to 0. **The tooling never touches a workload it cannot identify
    as JaisCloud's**: the store's objects carry `jaiscloud.io/managed-by=jaiscloud`
    (Deployment, PVC, Services, Secret), and the migration accepts only that
    marker, the conventional `app.kubernetes.io/managed-by=jaiscloud`, or
    `jaiscloud.io/substrate` on the legacy GCP workload. `app=postgres` alone is
    not evidence. A pre-split object predates the marker, so it needs a one-time
    `LEGACY_CONFIRM=1`; an unmarked object without it is refused, and a
    `LEGACY_NAMESPACE` outside `jaiscloud`/`jaiscloud-gcp`/`jaiscloud-data`
    likewise requires `LEGACY_CONFIRM=1`. `--delete-legacy` requires the marker
    and additionally `LEGACY_DELETE_FORCE=1` when the object only carries
    `app=postgres`; a PVC is never deleted by the migration. The preflight
    reports a marked, scaled-to-0 legacy object as `legacy.*Retained` (a rollback
    copy, not pending work), a marked one as `legacy.pending`, and an unmarked
    same-named workload as `legacy.candidate` plus a warning — never a migration
    recommendation. Running the new manifests without the migration silently
    starts against an empty store.
12. **Namespace names are literal in the manifests.** Each Namespace `name`, every
    `namespace:` field, the DSN hostnames and the VAP usernames spell out
    `jaiscloud`, `jaiscloud-gcp` and `jaiscloud-data`. `NS_AWS` / `NS_GCP` /
    `NS_DATA` let the gate scripts run against scratch names; every mutating
    `deploy.sh` mode verifies the manifests agree with the configured names
    (`check_namespace_overrides`) and refuses a partial override rather than
    operating on a half-renamed install. A custom layout requires editing those
    fields together.
13. **The store may be bundled or external.** `--store-dsn` /
    `JAISCLOUD_STORE_DSN` (or the per-substrate `JAISCLOUD_AWS_DSN` /
    `JAISCLOUD_GCP_DSN`) installs against a server the operator runs:
    `postgres.yaml` is **not** applied, the databases are created through a
    short-lived `psql` pod in `jaiscloud-data`, and the substrate DSN Secrets are
    overwritten with the external DSN (the base DSN's database component is
    replaced per cloud; per-substrate overrides are verbatim). The choice is
    recorded as `store` in the install record (`bundled|external`) and reported
    by the preflight as `store.mode`. An external store is not probeable from the
    cluster, so `store.ready` / `store.databases.*` are `false` (unknown, not
    missing), and `--migrate-store` into an external target is unsupported (it
    refuses).

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
  (`scripts/integration-preflight.sh`). The leak and legacy predicates are
  regression-tested offline with a fake `kubectl`:
  `scripts/integration-preflight-test.sh`.
- **The same pack without the source tree**: `jaiscloud-gcp agent-guide`
  (brief), `--json` (contract), `--emit DIR` (manifests + scripts + docs); both
  binaries embed it.
- **Human narrative**: [`README-GCP.md`](../../../README-GCP.md),
  [`DEVELOPER_GUIDE.md`](../../../DEVELOPER_GUIDE.md) Setup 3,
  [`deploy/k8s/istio/README.md`](../../../deploy/k8s/istio/README.md),
  [`docs/gcp/ci.md`](../ci.md).
