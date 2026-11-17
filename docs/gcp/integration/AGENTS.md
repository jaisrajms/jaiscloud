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
| legacy pre-split layout? | preflight `legacy.postgresInAws` / `legacy.gcpInAws` |
| is the store already at the new layout? | preflight `store.present` / `store.databases` |
| is the mesh installed? | preflight `mesh.installed` |
| is the cluster new enough to enforce the namespace boundary? | preflight `rbac.serverVersion` (needs >= 1.30) |
| is namespace isolation enforced? | preflight `rbac.admissionPolicy` (the ValidatingAdmissionPolicy) and `rbac.namespaceAdmin` (the cluster role binding for both substrate ServiceAccounts) |
| image strategy | published image (default) or your own (`custom-images`) |
| executors needed? | only if tests submit Dataproc/Kafka/Cloud Run/function work |

## Decision tree

```
deploy/deploy.sh --check --json
├─ legacy.postgresInAws or legacy.gcpInAws ......... upgrade-legacy (migration; ask first)
├─ no namespaces / no store ........................ new install
│   ├─ only GCP tests? ............................. new-install-gcp
│   └─ both substrates? ............................ new-install-both
├─ store ready, substrate missing .................. deploy that substrate
├─ rbac.admissionPolicy=false (a substrate exists) . install-rbac-policy (K8s >= 1.30; non-optional)
├─ substrates ready, need external real hostnames .. mesh
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

Omit `JAISCLOUD_GCP_IMAGE` to use the published image. The command creates the
namespaces it needs, the store, `jaiscloud_gcp`, the substrate and waits for the
rollout. On failure it prints the pod and its events.

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
ClusterIP: port-forward it. The two never share a database.

## Scenario 3 — the mesh (optional)

Install order matters: substrates first, then Istio, then the dev CA, then the
routes, then the smoke. See the `mesh` scenario in `contract.playbook` and
[`deploy/k8s/istio/README.md`](../../../deploy/k8s/istio/README.md). Verify with
`SMOKE_PORT_FORWARD=1 scripts/istio-smoke.sh` (4 assertions; the first is the
authority-preservation canary).

The mesh covers `*.amazonaws.com`, `*.googleapis.com`, `*.run.app`,
`*.cloudfunctions.net` over HTTP(S) only. It is not a Kafka path and not an
in-substrate path.

## Scenario 4 — upgrading a pre-split install

Detection is in the preflight (`legacy.*`). The migration is destructive and
irreversible in the sense that the old store is replaced; the old PVCs are kept
as rollback copies. **Confirm with the operator, then:**

```bash
SKIP_IMAGE_BUILD=1 deploy/deploy.sh --migrate-store --yes
# verify: a known row survives in both databases; an existing GCS object reads
# back through the new pod; deploy.sh --check --json shows no legacy workloads
kubectl -n jaiscloud delete pvc postgres-pvc jaiscloud-gcp-blobs-pvc   # after verification
```

What it does and what it keeps is in `contract.playbook.scenarios.upgrade-legacy`
and `destructive`. The blob copy needs a pullable `busybox`; preload it, or set
`SKIP_BLOB_MIGRATE=1` and copy the claim yourself.

If the new store already exists the command refuses — do **not** delete the new
PVC unless you have confirmed it is disposable.

## Scenario 5 — your own build

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

## Tooling summary

| Need | Command |
|---|---|
| read-only state | `deploy/deploy.sh --check [--json] [--substrate aws\|gcp\|both] [--desired aws=running,gcp=parked]` (`scripts/integration-preflight.sh`) |
| install intent | preflight `intent.*`; record in ConfigMap `jaiscloud-data/jaiscloud-install` (written by `deploy.sh`) |
| the pack, from the binary | `jaiscloud-gcp agent-guide` / `--json` / `--emit DIR` (`--force` to overwrite) |
| preview | `deploy/deploy.sh <mode> --plan` |
| install AWS / GCP | `deploy/deploy.sh [--yes]` / `deploy/deploy.sh --gcp --yes` |
| migrate | `deploy/deploy.sh --migrate-store --yes` |
| delete workloads (keep data) | `deploy/deploy.sh --delete` |
| resets (destructive, scoped) | `--reset` (AWS) / `--gcp-reset` (GCP) |
| namespace boundary | preflight `rbac.*`; `GATE_ALLOW_RBAC=1 KUBE_CONTEXT=<ctx> make test-rbac-isolation-k8s` (server-side dry runs, K8s >= 1.30) |
| gates | `make check-deploy-images`, `make check-integration-contract`, `make test-istio-smoke`, `ALLOW_RESET=1 make test-dual-substrate-k8s`, `make gcp-ci-preflight` |
| emulator state API | `GET /_jaiscloud/health`, `GET /_jaiscloud/doctor`, `POST /_jaiscloud/reset`, `GET /_jaiscloud/export`, `POST /_jaiscloud/import`, `POST /_jaiscloud/clock` |
