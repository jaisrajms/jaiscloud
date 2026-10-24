# k8shelpers

Generic Kubernetes job helpers shared by all JaisCloud executor packages (Spark, Lambda, etc.).
Provides pod-spec building, job submission, terminal waiting, log collection, ownership
patching, orphan cleanup, and idempotent terminal-state snapshots.

---

## Key types

```go
// JobHandle is returned by SubmitJob and consumed by WaitTerminal, TailLogs, Cancel.
type JobHandle struct {
    JobID, Namespace, JobName string
    JobUID, PodUID             types.UID
    CreatedAt                  time.Time
}

// Final is the pod-level terminal result from WaitTerminal.
// Workload-specific classification (e.g. Spark exit codes) is the caller's job.
type Final struct {
    Succeeded bool
    Cancelled bool
    ExitCode  int32
    Reason    string // OOMKilled, Error, Completed, ...
    Message   string // truncated last termination message
    StartTime, EndTime time.Time
}

// Snapshot is persisted to a ResourceStore when a job terminates so
// Describe-style APIs answer correctly after the k8s Job is GC'd.
type Snapshot struct {
    JobID, State, Reason, Message string
    StartTime, EndTime             time.Time
    ExitCode                       int32
    LogURIs, CallerMeta            map[string]string
}

// SubmitJobRequest is the full input to SubmitJob.
type SubmitJobRequest struct {
    Namespace, JobName             string
    Spec                           corev1.PodSpec
    Labels, Annotations            map[string]string
    Parallelism, BackoffLimit       int32
    TTLSecondsAfterFinished        *int32
    ActiveDeadlineSeconds          *int64
    OwnerRef                       *OwnerRefHint
}

// PatcherConfig specifies pods the ownership patcher should watch.
type PatcherConfig struct {
    LabelSelector string                              // e.g. "spark-role in (driver,executor)"
    ResolveOwner  func(*corev1.Pod) (*OwnerRefHint, error)
    Namespace     string   // primary namespace
    Namespaces    []string // additional per-resource namespaces (see ListManagedNamespaces)
}

// CleanupConfig controls the startup orphan sweep.
type CleanupConfig struct {
    Namespace, InstanceID string
    Namespaces            []string // additional per-resource namespaces
    OrphanSelectors       []string
    OnTerminalJob         func(jobName, state, reason string)
    OnUnownedPod          func(pod *corev1.Pod) (delete bool)
}
```

---

## Key functions

### NewClient
Builds the shared `kubernetes.Interface` used by every executor: in-cluster
config first (service-account token + CA), then the `JAISCLOUD_K8S_*`
environment (`APISERVER`, `TOKEN`, `TOKEN_FILE`, `CA_FILE`, `CLIENT_CERT_FILE`).
This is the single client bootstrap for `cmd/jaiscloud-aws`, `cmd/jaiscloud-gcp`
and the Lambda/Cloud Functions executor. It sets no HTTP timeout so long-lived
watches (ownership patchers, terminal waiters) are not severed; callers bound
individual calls with contexts.

### BuildPodSpec
Assembles a `corev1.PodTemplateSpec` in four layers:
1. `PodSpecInput` — caller's main container, labels, annotations
2. Platform overlay (`platform.PlatformConfig`) — init containers, volumes, env vars
3. Caller-supplied YAML `PodTemplateSpec` — merged on top (caller wins per field)
4. `IdentityMutator` — final cloud-specific identity wiring (IRSA, Azure MI, GCP WI)

**Security-classified env keys** (`JAVA_OPTS`, `JAVA_TOOL_OPTIONS`, `SSL_CERT_FILE`,
`AWS_CA_BUNDLE`, `REQUESTS_CA_BUNDLE`, etc.) use **caller-wins-with-platform-appended**:
the platform value is space-appended to the caller's value so TLS CA chains accumulate.
All other env keys: caller wins outright.

Opt-out annotation on the caller template:
```yaml
annotations:
  jaiscloud.io/platform-overlay: "skip-tls,skip-env=JAVA_OPTS"
```
Unknown tokens return `ErrUnknownOptOutToken` so typos surface at submit time.

### SubmitJob
Creates a `batch/v1 Job` from a `SubmitJobRequest`. Wires `OwnerReferences` if
`req.OwnerRef` is non-nil. Returns a `JobHandle`.

### WaitTerminal
Watches (or polls with exponential backoff) the Job's pod until it reaches a terminal
container state. Returns a pod-level `Final`. Spark-specific exit classification lives in
`sparkhelpers.WaitTerminal`.

### TailLogs
Streams pod logs to an `io.Writer`. `LogKind` selects which container group:
`LogKindMain`, `LogKindInit`, `LogKindSidecar`, `LogKindAll`.

### StartOwnershipPatcher
Launches a background goroutine that watches pods matching `PatcherConfig.LabelSelector`
and issues a JSON merge-patch to backfill `ownerReferences` on pods with empty ownership.
Runs a reconcile sweep on startup to catch pods created during a crash. Returns a `cancel`
function; also stops when the parent context is cancelled.

### CleanupOrphans
Startup sweep over every configured namespace (`Namespace` plus `Namespaces`):
1. `batch/v1 Jobs` labelled `app.kubernetes.io/managed-by=jaiscloud` (optionally filtered
   by `jaiscloud.io/instance-id`): terminal Jobs invoke `OnTerminalJob` and are deleted;
   suspended Jobs are unsuspended (re-adopted).
2. Each `OrphanSelectors` entry: pods with no `OwnerReferences` invoke `OnUnownedPod`.

### Workload lifecycle (Pod + ClusterIP Service)
`EnsureWorkload` creates a single-replica Pod plus a ClusterIP Service that fronts it
(idempotently), then waits for the Service's in-cluster TCP endpoint to accept a
connection, returning `<pod>.<ns>.svc.cluster.local:<port>`. On any failure after
creation begins it reaps the Pod/Service so a failed start cannot leak.
`DeleteWorkload` removes both (NotFound-tolerant); `SweepWorkloads` deletes every
Pod/Service matching a label selector (capped, paginated, best-effort). The Pod is a
`corev1.Pod` the caller builds — cloud-specific policy (image, env, labels, ports,
restart policy, readiness) stays with the caller. Consumers: Managed Kafka
(`k8sBroker`), Cloud Run revisions (`internal/gcp/runexec`), and the shared Lambda /
Cloud Functions executor (`internal/executor/lambda`). All are unit-tested with
`kubernetes/fake` by injecting the readiness `Probe`.

### Namespace lifecycle (per-resource isolation)
Engine-bearing, cluster-shaped resources can own one namespace each instead of sharing the
process-wide one. The seam lives in `namespace.go`:

- `NamespaceName(service, project, id)` — deterministic, RFC-1123, <=63:
  `gcp-<service>-<project>-<sanitized-id>-<hash8>` (hash over the raw inputs, so sanitize
  collisions stay distinct; restart-safe).
- `EnsureManagedNamespace(ctx, client, ns, service) (created, err)` — create-if-missing.
  `created == true` marks an emulator-owned namespace; a pre-existing namespace is adopted
  (`created == false`) and never deleted. A Forbidden Get/Create returns
  `ErrNamespaceForbidden` so the caller can fall back to `JAISCLOUD_K8S_NAMESPACE`.
- `DeleteManagedNamespace` / `SweepManagedNamespaces` / `ListManagedNamespaces` — delete and
  enumerate only namespaces carrying `jaiscloud.io/managed-by=jaiscloud` (and
  `jaiscloud.io/service=<service>`), so an adopted or foreign namespace is untouched.
- `EnsureNamespaceRBAC(ctx, client, ns, sa, saNamespace)` — creates the executor RoleBinding
  in a new namespace, referencing the `jaiscloud-executor` ClusterRole
  (`deploy/k8s/rbac.yaml`), so the executor ServiceAccount can run there.

`JAISCLOUD_K8S_NAMESPACE` remains the default for engines that have not migrated, and the
fallback when the ServiceAccount cannot manage cluster-scoped namespaces.

### PersistTerminalSnapshot / LoadTerminalSnapshot
First-write-wins terminal state persistence keyed by `prefix/jobID` under resource type
`k8s_terminal_snapshot`. Idempotent: a second write logs WARN and returns nil.

### BuildSnapshot / BuildSnapshotFromError
Convenience constructors:
```go
snap := k8shelpers.BuildSnapshot(final, "COMPLETED")
snap := k8shelpers.BuildSnapshotFromError(err)
```

---

## Testing

All tests use `k8s.io/client-go/kubernetes/fake` — no real cluster required.

```go
fakeClient := fake.NewSimpleClientset()
handle, err := k8shelpers.SubmitJob(ctx, fakeClient, req)
```
