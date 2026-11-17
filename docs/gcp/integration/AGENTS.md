# Agent runbook: integrating JaisCloud into a Kubernetes cluster

This file is written for an **LLM agent or automation** driving an integration.
It is short on prose and long on decisions, exact commands, and checks.

**No source tree?** The binary carries this pack. `jaiscloud-gcp agent-guide`
prints a short brief, `--json` prints the full contract, and `--emit DIR` writes
the deployment kit (manifests, `deploy/deploy.sh`, the preflight script, the
Istio and CI assets, and these docs) so the commands below work from that
directory. Inside a cluster:
`kubectl -n jaiscloud-gcp exec deploy/jaiscloud-gcp -- /jaiscloud agent-guide`.
The kit carries these integration docs and the top-level guides; links inside the
guides to the repository's deeper references (wire contract, fidelity matrix)
point at the source repository.

## Finding this guide

- **Repo checkout**: this file, [`../../gcp/README.md`](../README.md),
  [`llms.txt`](../../../llms.txt), or the root `AGENTS.md` pointer.
- **Binary or image**: `jaiscloud-<cloud> --help` lists `agent-guide`;
  `jaiscloud-<cloud> --help-agent` prints the brief; the image carries
  `docker inspect -f '{{index .Config.Labels "dev.jaiscloud.agent-guide"}}'`.
- **Running emulator**: `GET /_jaiscloud/doctor` returns `agent_guide`, and
  startup logs a one-line hint.

Read, in this order:

1. [`contract.yaml`](contract.yaml) — generated facts: namespaces, objects,
   images, env, ports, PVCs, RBAC subjects, plus a `playbook` section with
   scenarios, failure taxonomy, destructive operations and tooling.
2. [`topology.md`](topology.md) — the invariants the layout must keep.
3. This runbook — how to sequence the work.

Do not infer facts from prose when the contract has them. If the contract and a
manifest disagree, the manifest wins — regenerate with
`make gen-integration-contract` and report the drift.

## Hard rules

- **Inspect before you act**: `deploy/deploy.sh --check --json` is read-only and
  returns the cluster state, the install intent and `recommended[]`. Start there
  on every run. Pass `--substrate aws|gcp|both` (and optionally
  `--desired aws=running,gcp=parked`) when you know the intended shape: the
  advice then covers only installed clouds, and anything else is a leak.
- **The install record is intent, not a manifest**: `jaiscloud-data/jaiscloud-install`
  (`schema`, `installed`, `desired`) is written by `deploy.sh` and survives
  substrate resets. Read it through the preflight; do not hand-edit it and do
  not add it to a manifest.
- **Use the tools, not ad-hoc YAML**: apply the repository manifests via
  `deploy/deploy.sh`/`make`. Hand-editing a live Deployment creates drift that
  neither the contract check nor the preflight will see.
- **Ask before anything in `contract.playbook.destructive`.** The emulator's
  `--reset` drops a database; the isolation gate needs `ALLOW_RESET=1`; the
  migration rewrites the store.
- **Never rewrite `:authority`/`Host`** in the mesh. Never apply
  `deploy/k8s/istio/*.yaml` (the glob includes object-less `dns.yaml`).
- **Do not delete `jaiscloud-data`** or its PVC. It is both substrates' store.
- **Do not remove or weaken `jaiscloud-namespace-isolation`.** Both substrate
  ServiceAccounts hold cluster-wide namespace delete; that ValidatingAdmissionPolicy
  (K8s >= 1.30) is the only thing keeping one substrate out of the other's
  per-cluster namespaces. If it is missing, apply
  `deploy/k8s/rbac-admission-policy.yaml` instead of proceeding.
- **Keep the standalone path true**: no step may make the mesh, RBAC or Postgres
  required to start the emulator.

## Inputs to collect first

| Input | How |
|---|---|
| kubectl context / cluster kind | `kubectl config current-context`; `kubectl cluster-info` |
| does the cluster already run JaisCloud? | `deploy/deploy.sh --check --json` |
| what is installed, and what is wanted? | preflight `intent.source` / `intent.installed` / `intent.desired`, or `kubectl -n jaiscloud-data get configmap jaiscloud-install -o yaml` |
| legacy pre-split layout? | preflight `legacy.pending` (marked, migrate) vs `legacy.candidate` (unmarked same-named workload — confirm with `LEGACY_CONFIRM=1` or ignore) |
| is the store already at the new layout? | preflight `store.present` / `store.databases` |
| is the mesh installed? | preflight `mesh.installed` |
| is the cluster new enough to enforce the namespace boundary? | preflight `rbac.serverVersion` (needs >= 1.30) |
| is namespace isolation enforced? | preflight `rbac.admissionPolicy` (the ValidatingAdmissionPolicy) and `rbac.namespaceAdmin` (the intended substrate's `jaiscloud-namespace-admin-<cloud>` binding) |
| image strategy | published image (default) or your own (`custom-images`) |
| executors needed? | only if tests submit Dataproc/Kafka/Cloud Run/function work |

**Executor modes resolve per engine, then globally, then by default**:
`JAISCLOUD_<SUBSYSTEM>_EXECUTOR_MODE` (`SPARK`, `FUNCTIONS`, `CLOUDRUN`, `LAMBDA`,
`ECS`; `JAISCLOUD_KAFKA_BROKER_MODE` for Managed Kafka) wins over the global
`JAISCLOUD_EXECUTOR_MODE`, and an unset per-engine variable falls back to the
global, then to the engine default (`mock`) — so a global `docker`/`k8s` covers
every engine, and one engine opts out by naming its own variable.
`contract.playbook.executor_modes` carries the list.

## Decision tree

```
deploy/deploy.sh --check --json
├─ legacy.pending .................................. upgrade-legacy (migration; ask first)
├─ legacy.candidate only ........................... read the warning: an unmarked
│                                                    same-named workload; confirm
│                                                    or ignore, never migrate blindly
├─ no namespaces / no store ........................ new install
│   ├─ only GCP tests? ............................. new-install-gcp
│   └─ both substrates? ............................ new-install-both
├─ store ready, substrate missing .................. deploy that substrate
├─ rbac.admissionPolicy=false (a substrate exists) . install-rbac-policy (K8s >= 1.30; non-optional)
├─ substrates ready, need external real hostnames .. mesh
├─ need compute off but data kept ................. park / switch (then resume)
├─ Dataproc/Kafka/Cloud Run jobs fail .............. executors (rbac + spark image)
└─ nothing to do ................................... verify + stop
```

A missing policy is a non-optional recommendation: `deploy/deploy.sh` (any
install/reset path) applies `deploy/k8s/rbac-admission-policy.yaml` for you, and
refuses to run on Kubernetes < 1.30. If the server is older, the warning says so
and the cluster cannot enforce the policy — upgrade the cluster (or run each
substrate on its own) rather than removing the check.

## Scenario 1 — new install (GCP only)

```bash
deploy/deploy.sh --check --json           # expect no store/substrate yet
deploy/deploy.sh --gcp --plan             # review the commands (no mutation)
JAISCLOUD_GCP_IMAGE=<image> deploy/deploy.sh --gcp --yes
deploy/deploy.sh --check --json           # expect store.ready=true, substrates.gcp.ready=true
```

Omit `JAISCLOUD_GCP_IMAGE` to use the published image. The command creates only
the objects the GCP substrate needs — the store (`namespace-data.yaml` +
`postgres.yaml`), the `jaiscloud-gcp` namespace, its RBAC (`rbac-shared.yaml` +
`rbac-gcp.yaml`), `jaiscloud_gcp` and the substrate — and never the AWS
namespace, ServiceAccount or bindings. It also applies the namespace-isolation
policy first, then waits for the rollout. On failure it prints the pod and its
events.

Reach it with `kubectl -n jaiscloud-gcp port-forward svc/jaiscloud-gcp 8080:8080`
(REST) and `8081:8081` (gRPC h2c). In-cluster clients use
`jaiscloud-gcp.jaiscloud-gcp.svc.cluster.local:8080/8081` directly.

## Scenario 2 — new install (both substrates)

```bash
deploy/deploy.sh --yes
deploy/deploy.sh --gcp --yes
deploy/deploy.sh --check --json
```

The AWS substrate is `jaiscloud:4566` (LoadBalancer on Docker Desktop; on
kind/k3d use the port-forward the summary prints). The GCP substrate is a
ClusterIP: port-forward it. The two never share a database. Each install creates
only its own namespace and RBAC (`namespace-aws.yaml` + `rbac-aws.yaml`, or
`namespace-gcp.yaml` + `rbac-gcp.yaml`); the store and the namespace-isolation
policy are shared.

## Scenario 3 — the mesh (optional)

Install order matters: substrates first, then Istio, then the dev CA, then the
routes, then the smoke. See the `mesh` scenario in `contract.playbook` and
[`deploy/k8s/istio/README.md`](../../../deploy/k8s/istio/README.md). Verify with
`SMOKE_PORT_FORWARD=1 scripts/istio-smoke.sh` (4 assertions; the first is the
authority-preservation canary).

On a cluster that **already has istiod**, do not run
`istioctl install --set profile=minimal` (it can replace that cluster's
IOP/meshConfig and prune components). Add only the gateway with the overlay:
`istioctl install -f deploy/k8s/istio/istiooperator-gateway-only.yaml` (or
`istioctl manifest generate -f … | kubectl apply -f -`). `deploy.sh --check`
warns when istiod pre-exists (`mesh.existingInstall`) and names the overlay.

The mesh covers `*.amazonaws.com`, `*.googleapis.com`, `*.run.app`,
`*.cloudfunctions.net` over HTTP(S) only. It is not a Kafka path and not an
in-substrate path: a sidecar-injected client reaches the substrates directly over
Service DNS with plaintext (`jaiscloud:4566`, `jaiscloud-gcp:8080` REST,
`jaiscloud-gcp:8081` gRPC h2c) and needs no `PeerAuthentication`,
`DestinationRule` or `ServiceEntry` — see
[`deploy/k8s/istio/README.md`](../../../deploy/k8s/istio/README.md) ("Addressing
the substrates" and "Clients with a sidecar").

## Scenario 4 — upgrading a pre-split install

Detection is in the preflight (`legacy.*`; it probes `deployment/postgres` and
`statefulset/postgres` plus `deployment/jaiscloud-gcp` in the AWS namespace,
`legacy.pending` is the actionable flag, `legacy.*Retained` marks a rollback copy,
and `legacy.candidate` marks an *unmarked* same-named workload — a warning, never
a migration recommendation). The migration is destructive towards the *old*
store in the sense that the new store becomes authoritative, but the old workload
and volumes are kept by default as rollback copies.
**Confirm with the operator, then:**

```bash
SKIP_IMAGE_BUILD=1 deploy/deploy.sh --migrate-store --yes
# verify: a known row survives in both databases; an existing GCS object reads
# back through the new pod; deploy/deploy.sh --check --json shows no pending
# legacy workloads (legacy.pending=false)
kubectl -n jaiscloud delete deployment postgres service postgres postgres-external
kubectl -n jaiscloud delete pvc postgres-pvc jaiscloud-gcp-blobs-pvc   # after verification
```

What it does and what it keeps is in `contract.playbook.scenarios.upgrade-legacy`
and `destructive`. The source is `deployment/postgres` or `statefulset/postgres`
(override with `LEGACY_POSTGRES_WORKLOAD=deployment|statefulset/<name>`), in
`LEGACY_NAMESPACE` (default `jaiscloud`; any other namespace requires
`LEGACY_CONFIRM=1`). If the old store is outside the cluster,
`LEGACY_DSN=postgres://…` dumps it from the **new** store pod after that pod is
up — no legacy store workload is scaled or deleted in that mode (the emulators
still stop before the dump). `LEGACY_DSN` is an explicit source, so it needs no
marker.

**The tooling never touches a workload it cannot identify as JaisCloud's.** The
store's objects carry `jaiscloud.io/managed-by=jaiscloud` (Deployment, PVC,
Services, Secret); the migration accepts that marker, the conventional
`app.kubernetes.io/managed-by=jaiscloud`, or `jaiscloud.io/substrate` on the
legacy GCP workload — and *never* `app=postgres` alone, which any operator-run
Postgres has. A pre-split build predates the marker, so the first migration needs
`LEGACY_CONFIRM=1` (or `--legacy-confirm`); without it the command refuses the
source before dumping anything. An unmarked workload of the same name is a
`legacy.candidate`: read the preflight warning, and either confirm it with
`LEGACY_CONFIRM=1` or ignore it.

Deletion is opt-in: by default only the legacy Postgres is scaled to 0, and its
Deployment/StatefulSet, Services and PVCs stay. `--delete-legacy` (or
`DELETE_LEGACY=1`) removes the workload and the `postgres`/`postgres-external`
Services only when the object carries the JaisCloud marker; a bare
`app=postgres` object additionally needs `LEGACY_DELETE_FORCE=1`. No PVC is ever
deleted by the migration.

Object bytes move with `migrate_blobs`: the GCP claim when the legacy GCP
emulator exists, and the AWS claim when `LEGACY_NAMESPACE` differs from the AWS
namespace (a PVC cannot move namespaces). Override the claims with
`LEGACY_GCP_BLOBS_PVC` / `NEW_GCP_BLOBS_PVC` / `LEGACY_AWS_BLOBS_PVC` /
`NEW_AWS_BLOBS_PVC`. The blob copy needs a pullable `busybox`; preload it, or set
`SKIP_BLOB_MIGRATE=1` and copy the claim yourself.

If the new store already exists the command refuses — do **not** delete the new
PVC unless you have confirmed it is disposable (`MIGRATE_FORCE=1` overrides, and
the restore then overwrites the schema in that volume).

## Scenario 5 — bring your own store

`deploy.sh` normally installs a bundled PostgreSQL (`postgres.yaml`) in
`jaiscloud-data`. To use a server you already run, point the install at it:

```bash
deploy/deploy.sh --yes --store-dsn 'postgres://user:pass@db.example.com:5432/postgres'
deploy/deploy.sh --gcp --yes --store-dsn 'postgres://user:pass@db.example.com:5432/postgres'
# or environment: JAISCLOUD_STORE_DSN=… (JAISCLOUD_AWS_DSN / JAISCLOUD_GCP_DSN
# override it per substrate and are what the substrate Secrets receive)
```

In this mode `postgres.yaml` is not applied. The databases (`jaiscloud`,
`jaiscloud_gcp`) are created through a short-lived `psql` pod in
`jaiscloud-data` (`STORE_INIT_IMAGE`, default `postgres:16-alpine`), and the
substrate DSN Secrets (`jaiscloud-dsn`, `jaiscloud-gcp-dsn`) are overwritten with
the external DSN, followed by a rollout restart. The base DSN's **database
component is replaced** with `jaiscloud` / `jaiscloud_gcp` for those Secrets (one
database per cloud); `JAISCLOUD_AWS_DSN` / `JAISCLOUD_GCP_DSN` are used verbatim
when you need an exact per-substrate DSN. `namespace-data.yaml` is still
applied: it holds the install record. The install record carries
`store=bundled|external`, and the preflight reports `store.mode`.

Limits: the preflight cannot probe an external server (no pod to exec), so
`store.ready`/`store.databases.*` are `false` = "not probeable", not "missing".
`--migrate-store` into an external target is **not supported yet** (it refuses
before doing anything); dump/restore that server yourself.

## Scenario 6 — park / resume / switch (keep the data, stop the compute)

Park is the deliberate, compute-destructive, data-retentive mode: it terminates
the substrate and every executor workload but keeps the database, the blobs and
state PVCs (including the instance ID), the namespaces, the CA and all resource
records.

```bash
deploy/deploy.sh --park aws --plan        # review: no manifests re-applied
deploy/deploy.sh --park gcp               # stop GCP compute, keep its data
deploy/deploy.sh --resume gcp             # one rollout; keeps the parked binary
deploy/deploy.sh --switch aws --yes       # park GCP (when installed), resume AWS
```

The park record is **per substrate**: the namespace gets
`jaiscloud.io/<cloud>.state`, `.parked-at`, `.parked-image`, `.parked-image-id`
and `.parked-version` (`aws` | `gcp`), alongside the per-cloud `desired` in the
install record. Two substrates sharing a namespace therefore keep both records;
read the one for the cloud you care about (from the preflight's
`substrates.<cloud>` and the install record, not a shared key).

Before scaling the Deployment to 0, `deploy.sh` calls the emulator's
`POST /_jaiscloud/park`, where each engine quiesces itself: Dataproc cancels
non-terminal jobs, Managed Kafka deletes broker Pods/Services (keeping cluster
and topic *records*; broker topic data is lost because the broker has no PVC),
and Cloud Run removes revision runtimes (keeping Service/Revision records).
`Resume` runs the reverse through `POST /_jaiscloud/resume`. A 404 from either
endpoint means the running image predates the API — rebuild and redeploy.

**A resume never changes the binary silently.** Before scaling, it compares the
Deployment image with the parked one and refuses a mismatch (naming
`--allow-upgrade` as the only opt-in), then pins the parked `imageID` digest with
`kubectl set image`, so a moving `:latest` tag cannot slip a new build in and run
its store migrations. With no recorded digest it warns that it cannot pin; the
post-rollout digest comparison remains a warning. `--allow-upgrade`
(`ALLOW_UPGRADE=1`) is the explicit "use the new image, the store may migrate".

**Managed Kafka topic loss is surfaced, not implied.** The park result carries
`"topicDataLost": true` when a real broker was stopped, `deploy.sh --park gcp`
warns when `JAISCLOUD_KAFKA_BROKER_MODE` is `k8s`/`docker`/`native`, and the
preflight reports `substrates.gcp.kafkaBrokerMode` plus a warning. Cluster/topic
metadata and the namespace survive; the topics do not (no broker PVC).

The switch direction is always park-then-resume, so only one substrate's compute
runs at a time. Images are tagged `:latest` for now; a resume pins the parked
digest as described. `--delete [aws|gcp|both]` is the workload-only alternative:
it removes the Deployment/Service (and DSN Secret, plus the AWS ConfigMap) but
never the PVCs or the database; the other substrate is untouched unless you pass
`both`.

Verify with the lifecycle gate (compute-destructive; needs both substrates
installed from a build that has the park/resume API):

```bash
GATE_ALLOW_LIFECYCLE=1 make test-lifecycle-k8s
```

## Scenario 7 — your own build

Build, deliver (push or `k3d image import`/`kind load`), then
`JAISCLOUD_GCP_IMAGE=<image> deploy/deploy.sh --gcp --yes`. Dataproc also needs a
Spark image with the GCS connector in `JAISCLOUD_K8S_SPARK_IMAGE`; see
`custom-images` in the playbook.

## Verifying

Use `contract.playbook.verification`. The cheapest full check is the preflight
(`recommended: []` means the intended shape holds; `--strict` returns 0 only
then, with no leak warnings); the strongest is
`ALLOW_RESET=1 make test-dual-substrate-k8s` (destructive: it drops the AWS
database to prove GCP survives).

## When something fails

1. Read the last command's stderr. `deploy.sh` prints pod state and events on a
   failed rollout.
2. Match the symptom in `contract.playbook.failure_taxonomy`.
3. Re-run `deploy/deploy.sh --check --json` and act on `recommended[]`.
4. Only then consider reading the human docs
   ([`README-GCP.md`](../../../README-GCP.md),
   [`DEVELOPER_GUIDE.md`](../../../DEVELOPER_GUIDE.md) Setup 3).

Do not "fix" a failure by disabling a guard (for example by deleting the store,
exposing 443, or rewriting host headers). Those guards encode the invariants.

## Multi-tenant / parallel installs

One namespace and one `JAISCLOUD_INSTANCE_ID` per instance. The shipped manifests
pin single values because they describe one deployment; copy them and change both
for a shared cluster, otherwise one instance's startup sweep can reap another's
Pods (the sweep only sees its own IDs, but the namespace/DB pair is the real
boundary). Namespaces the emulator creates are stamped with the instance ID and
substrate, and the admission policy refuses a namespace request from a substrate
ServiceAccount that does not carry its own identity.

The namespace *names* are literal in the manifests (Namespace `name`,
`namespace:` fields, DSN hostnames, and the ValidatingAdmissionPolicy's
`system:serviceaccount:<namespace>:<sa>` usernames). `NS_AWS` / `NS_GCP` /
`NS_DATA` exist for the gate scripts, and `deploy.sh` verifies the manifests
against them on every mutating mode: a partial override is refused with the list
of mismatches instead of acting on a half-renamed install. To run under custom
names, edit those fields together and re-run `deploy/deploy.sh --check`.

## Tooling summary

| Need | Command |
|---|---|
| read-only state | `deploy/deploy.sh --check [--json] [--substrate aws\|gcp\|both] [--desired aws=running,gcp=parked]` (`scripts/integration-preflight.sh`) |
| preflight predicates (offline) | `scripts/integration-preflight-test.sh` (fake kubectl: leak + legacy regression, no cluster) |
| install intent | preflight `intent.*`; record in ConfigMap `jaiscloud-data/jaiscloud-install` (written by `deploy.sh`) |
| store mode | preflight `store.mode` (`bundled\|external`); install record key `store` |
| the pack, from the binary | `jaiscloud-gcp agent-guide` / `--json` / `--emit DIR` (`--force` to overwrite) |
| preview | `deploy/deploy.sh <mode> --plan` |
| install AWS / GCP | `deploy/deploy.sh [--yes]` / `deploy/deploy.sh --gcp --yes` |
| bring your own store | `deploy/deploy.sh --yes --store-dsn postgres://…` (`JAISCLOUD_STORE_DSN`, `JAISCLOUD_AWS_DSN`, `JAISCLOUD_GCP_DSN`) |
| park / resume / switch (compute only) | `deploy/deploy.sh --park <aws\|gcp>` / `--resume <aws\|gcp>` (pins the parked digest; `--allow-upgrade` opts into a new image) / `--switch <aws\|gcp> --yes` |
| migrate (keep the legacy workload) | `deploy/deploy.sh --migrate-store --yes` (marked source; `--legacy-confirm` for a pre-split build, `--delete-legacy` removes the workload + Services, never a PVC) |
| delete workloads (keep data) | `deploy/deploy.sh --delete [aws\|gcp\|both]` |
| resets (destructive, scoped) | `--reset` (AWS) / `--gcp-reset` (GCP) |
| namespace boundary | preflight `rbac.*`; `GATE_ALLOW_RBAC=1 KUBE_CONTEXT=<ctx> make test-rbac-isolation-k8s` (server-side dry runs, K8s >= 1.30) |
| gates | `make check-deploy-images`, `make check-integration-contract`, `make test-istio-smoke`, `ALLOW_RESET=1 make test-dual-substrate-k8s`, `GATE_ALLOW_LIFECYCLE=1 make test-lifecycle-k8s`, `make gcp-ci-preflight` |
| emulator state API | `GET /_jaiscloud/health`, `GET /_jaiscloud/doctor`, `POST /_jaiscloud/reset`, `GET /_jaiscloud/export`, `POST /_jaiscloud/import`, `POST /_jaiscloud/clock` |
| emulator lifecycle API | `POST /_jaiscloud/park`, `POST /_jaiscloud/resume` (engine hooks; called by `deploy.sh --park`/`--resume`) |
