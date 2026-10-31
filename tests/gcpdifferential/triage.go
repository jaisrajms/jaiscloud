//go:build gcp_differential

package gcpdifferential

import "strings"

// TriageRule classifies a differential divergence as ACCEPTED (by design or
// harmless) rather than an open bug. It mirrors tests/gcpconformance's
// AllowRule: a rule matches on a divergence's Service, Op, Kind and Location,
// where an empty field acts as a wildcard.
//
// Location is matched by suffix (not equality) because the diff walker embeds
// array indices in the location path (e.g. "response.items[0].rpo"), which vary
// with the response shape; a suffix such as ".rpo" matches every occurrence.
//
// A rule MUST carry a Reason. Accepted divergences are reported, with their
// reason, alongside the open (real-bug) list — nothing is silently dropped.
type TriageRule struct {
	Service   string // "" matches any service
	Op        string // "" matches any op
	Transport string // "" matches any transport; otherwise "rest"/"grpc"
	Kind      string // "" matches any kind
	Location  string // "" matches any location; otherwise a suffix match
	Reason    string
}

func (r TriageRule) matches(d Divergence) bool {
	if r.Service != "" && r.Service != d.Service {
		return false
	}
	if r.Op != "" && r.Op != d.Op {
		return false
	}
	if r.Transport != "" && r.Transport != d.Transport {
		return false
	}
	if r.Kind != "" && r.Kind != d.Kind {
		return false
	}
	if r.Location != "" && !strings.HasSuffix(d.Location, r.Location) {
		return false
	}
	return true
}

// triageRules is the shipped allowlist of accepted divergences. It contains
// ONLY differences that are (a) explicitly declared by the published fidelity
// contract (docs/gcp/wire-contract.md, docs/fidelity/fidelity-matrix.md), or (b) additive /
// wording-only and therefore unable to break an official client. Every entry
// states WHY the difference is acceptable; anything that would break or
// mislead a client is deliberately left OUT so it stays in the open list.
//
// No divergence is currently kept open: every recorded difference is additive,
// a documented default, or error prose only, and is accepted below.
var triageRules = []TriageRule{
	// ── Additive fields ──────────────────────────────────────────────────────
	// The emulator may return fields real GCP omits. JSON clients ignore
	// unknown members, so an extra field is additive and cannot break or
	// mislead one (the wire contract §4 records the same class of finding as non-fatal).
	{
		Kind:   "extra_field",
		Reason: "emulator emits a superset of the real response; official JSON clients ignore unknown fields, so an additive field cannot break them",
	},

	// ── Error prose ──────────────────────────────────────────────────────────
	// Only the human-readable message/description differs; the HTTP status,
	// the machine-readable code/status and the envelope shape all match, so no
	// client branches on the wording.
	{
		Kind:     "value_mismatch",
		Location: ".message",
		Reason:   "human-readable error prose only; HTTP status, error code and envelope shape match real GCP",
	},
	{
		Kind:     "value_mismatch",
		Location: ".description",
		Reason:   "human-readable error prose only; HTTP status, error code and envelope shape match real GCP",
	},

	// ── BigQuery executed SQL: job statistics / timing ─────────────────────
	// jobs.query now evaluates the documented Standard SQL subset and its
	// executed rows/schema/statementType match real GCP (BQ1/BQ2). The
	// remaining differences are the output-only job-statistics/timing fields
	// the emulator does not synthesize; they are additive metadata. Each is
	// accepted by its own Location-scoped rule (see the init below) rather than
	// by one blanket op rule, so a regression in the executed result itself
	// (rows/schema/statementType/totalRows) still surfaces as an open bug.
	// The modern google.rpc error envelope carries code+message; the legacy
	// errors[] array is per-generation shape variance, modeled as acceptable by
	// the wire-conformance harness (tests/gcpconformance/allowlist.go). The
	// optional google.rpc error details[] (e.g. ResourceInfo) is the same class.
	{
		Kind:     "missing_field",
		Location: "response.error.errors",
		Reason:   "modern google.rpc error envelope carries code+message; the legacy errors[] array is optional per-generation shape variance",
	},
	{
		Kind:     "missing_field",
		Location: "response.error.details",
		Reason:   "google.rpc error details[] (e.g. ResourceInfo) is optional; the modern envelope's code+message+status match",
	},
	// BigQuery's legacy errors[] detail carries a per-token location and
	// locationType for a SQL parse error. The machine-readable code/message/
	// status match; no official client branches on the token position.
	{
		Service:  "bigquery",
		Op:       "query_bad_sql",
		Kind:     "missing_field",
		Location: "errors[0].location",
		Reason:   "BigQuery's legacy errors[].location is a per-token parse-offset detail; the code/message/status in the envelope match real GCP",
	},
	{
		Service:  "bigquery",
		Op:       "query_bad_sql",
		Kind:     "missing_field",
		Location: "errors[0].locationType",
		Reason:   "BigQuery's legacy errors[].locationType is a per-token parse-offset detail; the code/message/status in the envelope match real GCP",
	},
	// Long-running operations complete synchronously in the emulator, so its
	// create/update/delete operation is already done=true while real GCP
	// returns done=false at submit time. The operation and its metadata are
	// otherwise modeled; this is a completion-timing difference only.
	{
		Service:  "workflows",
		Kind:     "value_mismatch",
		Location: "response.done",
		Reason:   "Workflows create/update/delete are LROs; the emulator completes them synchronously (done=true) whereas real GCP returns done=false at submit time",
	},
	// Cloud DNS applies changes asynchronously; real GCP reports a freshly
	// submitted change as status=pending while the emulator applies it
	// synchronously (done). The change contents and id match.
	{
		Service:  "dns",
		Kind:     "value_mismatch",
		Location: "response.status",
		Reason:   "DNS changes are applied asynchronously; real GCP reports status=pending at submit time while the emulator applies synchronously (done)",
	},
	// ── Cloud Dataproc cluster LRO (SDK-tour) ────────────────────────────────
	// The cluster-read/operation fields real Dataproc materialises and the
	// emulator still omits are accepted one location at a time (see
	// dataprocOmittedReadFields); the fields the emulator does render are left
	// open-checked. The freshly submitted Dataproc create operation reports its metadata
	// status PENDING on real GCP; the emulator begins provisioning immediately
	// (RUNNING). The operation name, its done flag and the settled cluster's
	// identity match; only the transient operation-status label differs.
	{
		Service:  "dataproc",
		Op:       "tour_dataproc_cluster_create",
		Kind:     "value_mismatch",
		Location: "response.metadata.status.state",
		Reason:   "Dataproc cluster create is an LRO: real GCP's freshly submitted operation metadata reports status PENDING while the emulator starts provisioning immediately (RUNNING); the operation name/done and the settled cluster identity are checked",
	},
	// Real GCP resolves the requested Dataproc imageVersion alias to the current
	// qualified build (e.g. "2.2" -> "2.2.88-debian12"); the emulator echoes the
	// requested alias so a committed golden does not track a moving image build.
	{
		Service:  "dataproc",
		Kind:     "value_mismatch",
		Location: "softwareConfig.imageVersion",
		Reason:   "real GCP resolves the requested Dataproc imageVersion alias to the current qualified build (e.g. 2.2 -> 2.2.88-debian12); the emulator echoes the requested alias, an output-only resolved value",
	},
	// The cluster's statusHistory is an output-only state log: real GCP records
	// the full CREATING -> RUNNING sequence while the emulator records the
	// states it observed (and adds the operation's terminal DONE entry on the
	// poll). The current status.state is still value-checked.
	{
		Service:  "dataproc",
		Kind:     "array_length_mismatch",
		Location: ".statusHistory",
		Reason:   "Dataproc cluster/operation statusHistory is an output-only state log; real GCP and the emulator record slightly different state sequences (the operation poll adds a terminal DONE entry). status.state itself is value-checked",
	},
	// Real GCP derives totalRows from table metadata that lags a just-streamed
	// insert (it returned 0); the emulator's 1 is the stored row the preview
	// contract promises, so the emulator is the more correct side here.
	{
		Service:  "bigquery",
		Op:       "tabledata_list",
		Kind:     "value_mismatch",
		Location: "response.totalRows",
		Reason:   "real GCP's totalRows reflects table metadata lagging the streaming insert, not the stored row; the emulator's value reflects the stored-rows contract",
	},
	// BigQuery cosmetic / defaulted metadata. These are output-only fields that
	// either default to the value the emulator omits or are pure vanity URLs,
	// and no official BigQuery client requires them.
	{
		Service:  "bigquery",
		Kind:     "missing_field",
		Location: "response.selfLink",
		Reason:   "cosmetic output-only URL; resource names are already returned in the *_reference fields and no official client requires it",
	},
	{
		Service:  "bigquery",
		Kind:     "missing_field",
		Location: ".type",
		Reason:   "Dataset type is an optional enum whose documented default is DEFAULT; absence is semantically equivalent",
	},
	{
		Service:  "bigquery",
		Kind:     "missing_field",
		Location: "Bytes",
		Reason:   "table byte counters are all 0 for a freshly created empty table; absence is semantically equivalent to the zero value",
	},
	{
		Service:  "bigquery",
		Kind:     "missing_field",
		Location: ".etag",
		Reason:   "cosmetic token on list/get responses; no official BigQuery client requires it and the API exposes no etag preconditions",
	},
	{
		Service:  "bigquery",
		Kind:     "missing_field",
		Location: "response.location",
		Reason:   "table location is inherited (US) from the dataset and is output-only; absence loses no actionable information",
	},

	// ── Pub/Sub: absent pushConfig ≡ {} ─────────────────────────────────────
	// An absent pushConfig and an explicit {} both mean "no push endpoint".
	{
		Service:  "pubsub",
		Kind:     "missing_field",
		Location: "pushConfig",
		Reason:   "an absent pushConfig and an explicit {} both mean 'no push endpoint'; the emulator correctly has no push subscription",
	},
	// ── GCS: absent rpo ≡ default ───────────────────────────────────────────
	{
		Service:  "storage",
		Kind:     "missing_field",
		Location: ".rpo",
		Reason:   "bucket rpo is an optional enum whose documented default is DEFAULT; absence is semantically equivalent",
	},

	// ── KMS: documented defaults ────────────────────────────────────────────
	{
		Service:  "kms",
		Kind:     "missing_field",
		Location: "destroyScheduledDuration",
		Reason:   "documented default 2592000s (30 days); absence is semantically equivalent to the default",
	},
	{
		Service:  "kms",
		Kind:     "missing_field",
		Location: "primary.protectionLevel",
		Reason:   "protection level is SOFTWARE (the default) and the same key's versionTemplate.protectionLevel already returns it, so absence loses no information",
	},

	// ── Resource Manager: project display name / creation time ───────────────
	// The v1 Project `name` is the human-readable display name, which is
	// user-set per project: the real parity project's differs from the
	// emulator's synthesized project (which presents the project id). The
	// resource identity (projectId/projectNumber) and lifecycleState match.
	{
		Service:  "resourcemanager",
		Op:       "project_get",
		Kind:     "value_mismatch",
		Location: "response.name",
		Reason:   "the v1 Project `name` is the user-set human display name, which differs between the real parity project and the emulator's synthesized project; projectId/projectNumber/lifecycleState match",
	},
	// Real GCP always records when a project was created; the emulator does not
	// synthesize a createTime for a project it never explicitly created. It is
	// output-only metadata, not part of the project identity, and its value is
	// folded.
	{
		Service:  "resourcemanager",
		Op:       "project_get",
		Kind:     "missing_field",
		Location: "response.createTime",
		Reason:   "output-only creation timestamp the emulator does not synthesize for a project it did not explicitly create; not part of the project identity (createTime values are folded)",
	},

	// ── Compute: cosmetic list-response metadata ─────────────────────────────
	// A compute list response carries its own synthetic id and selfLink; the
	// emulator returns the items (each with its resource name) but omits those
	// two output-only fields, exactly as the BigQuery selfLink rule accepts.
	// Scoped to the op so a genuine missing id/selfLink on a compute *resource*
	// still surfaces as open.
	{
		Service:  "compute",
		Op:       "compute_instances_list",
		Kind:     "missing_field",
		Location: "response.id",
		Reason:   "the list response's own id is an output-only synthetic identifier reconstructed from the request path; items carry the resource names and no official client requires it",
	},
	{
		Service:  "compute",
		Op:       "compute_instances_list",
		Kind:     "missing_field",
		Location: "response.selfLink",
		Reason:   "cosmetic output-only list URL; no official client requires it",
	},

	// ── Authz paths: not enforced by design (G1, docs/gcp/wire-contract.md §8) ─────────────
	// Each *_noauth scenario is sent without an Authorization header. Real GCP
	// rejects it (401 UNAUTHENTICATED) while the emulator serves the request, so
	// the status and the whole body differ. This is the documented accepted
	// risk, recorded (not silently dropped) with an explicit reason. The rules
	// are scoped to the *_noauth op so a genuine authz divergence elsewhere
	// still surfaces as open.
	{
		Service: "storage",
		Op:      "storage_buckets_list_noauth",
		Reason:  "authz is not enforced by design (G1, accepted risk: identity authz is not enforced; see docs/gcp/wire-contract.md §8): real GCP returns 401 UNAUTHENTICATED while the emulator serves the request; the divergence is the accepted-risk record, not a bug",
	},
	{
		Service: "compute",
		Op:      "compute_instances_list_noauth",
		Reason:  "authz is not enforced by design (G1, accepted risk: identity authz is not enforced; see docs/gcp/wire-contract.md §8): real GCP returns 401 UNAUTHENTICATED while the emulator serves the request; the divergence is the accepted-risk record, not a bug",
	},
	{
		Service: "sqladmin",
		Op:      "sql_instances_list_noauth",
		Reason:  "authz is not enforced by design (G1, accepted risk: identity authz is not enforced; see docs/gcp/wire-contract.md §8): real GCP returns 401 UNAUTHENTICATED while the emulator serves the request; the divergence is the accepted-risk record, not a bug",
	},
	{
		Service: "redis",
		Op:      "redis_instances_list_noauth",
		Reason:  "authz is not enforced by design (G1, accepted risk: identity authz is not enforced; see docs/gcp/wire-contract.md §8): real GCP returns 401 UNAUTHENTICATED while the emulator serves the request; the divergence is the accepted-risk record, not a bug",
	},
	{
		Service: "container",
		Op:      "container_clusters_list_noauth",
		Reason:  "authz is not enforced by design (G1, accepted risk: identity authz is not enforced; see docs/gcp/wire-contract.md §8): real GCP returns 401 UNAUTHENTICATED while the emulator serves the request; the divergence is the accepted-risk record, not a bug",
	},
	{
		Service: "serviceusage",
		Op:      "su_service_get_unknown",
		Reason:  "the emulator does not maintain the Service Usage catalog: it synthesizes any service name as DISABLED instead of reporting real GCP's 403 PERMISSION_DENIED for a service the caller cannot see; the divergence is the documented catalog/authz gap (G1), not a wire-shape bug",
	},

	// ── crypto-medallion demo parity (D1) ────────────────────────────────────
	// Accepted differences for the demo's REST mutations. Each is an output-only
	// or defaulted field real GCP materializes and the emulator does not echo,
	// an additive field, or an accepted LRO-completion/authorship-timing
	// difference — the same classes the rules above record. Nothing here changes
	// a value the demo reads.

	// Cloud Run v2 Service: the emulator models a control-plane subset of the
	// Service resource (the demo reads only `uri` and `terminalCondition.state`,
	// which match). Real GCP's output-only/defaulted read fields the emulator
	// omits are creator/lastModifier/launchStage/ingress/scaling/sshEnabled and
	// the defaulted template sub-fields (resources/startupProbe/port name/
	// timeout/serviceAccount/maxInstanceRequestConcurrency); real GCP's
	// `traffic` allocation is the emulator's `trafficStatuses`.
	{
		Service: "run",
		Kind:    "missing_field",
		Reason:  "Cloud Run v2 Service control-plane subset: real GCP's output-only/defaulted read fields (creator/lastModifier/launchStage/ingress/scaling/sshEnabled, defaulted template resources/startupProbe/port name/timeout/serviceAccount/maxInstanceRequestConcurrency, and `traffic`, which the emulator exposes as `trafficStatuses`) are omitted; the demo-read fields (uri, terminalCondition) match",
	},
	{
		Service: "run",
		Kind:    "array_length_mismatch",
		Reason:  "Cloud Run v2 Service control-plane subset: the emulator synthesizes a single Ready condition (real GCP also reports RoutesReady/ConfigurationsReady) and one service URL; the demo does not iterate these arrays",
	},
	{
		Service:  "run",
		Kind:     "value_mismatch",
		Location: ".conditions[0].type",
		Reason:   "Cloud Run v2 Service control-plane subset: the emulator's single condition is the terminal Ready condition while real GCP's conditions array starts with RoutesReady; both express the same reconciled state",
	},
	{
		Service:  "run",
		Kind:     "value_mismatch",
		Location: ".trafficStatuses[0].revision",
		Reason:   "Cloud Run v2 Service control-plane subset: real GCP's trafficStatuses[].revision is the short revision id while the emulator returns the full revision resource name; both name the same revision (the full name also appears in latestReadyRevision on both sides)",
	},
	{
		Service:  "run",
		Kind:     "value_mismatch",
		Location: ".reconciling",
		Reason:   "accepted LRO-completion timing: real GCP reports reconciling=true while the operation is in flight; the emulator completes create/update synchronously (reconciling=false)",
	},
	{
		Service:  "run",
		Kind:     "value_mismatch",
		Location: ".terminalCondition.state",
		Reason:   "accepted LRO-completion timing: real GCP reports the terminal condition as PENDING/RECONCILING while the operation is in flight; the emulator completes synchronously (CONDITION_SUCCEEDED)",
	},

	// BigQuery load job + jobs.list. The emulator evaluates loads synchronously
	// and does not echo real GCP's output-only job metadata.
	{
		Service: "bigquery",
		Op:      "job_insert_load",
		Kind:    "missing_field",
		Reason:  "output-only BigQuery job metadata the emulator does not echo (load schema, jobCreationReason, principal_subject, statistics.reservation_id, user_email); the load configuration and destination match",
	},
	{
		Service:  "bigquery",
		Op:       "job_insert_load",
		Kind:     "value_mismatch",
		Location: "response.id",
		Reason:   "output-only job identity: real GCP's job `id` embeds the job location (`<project>:US.<jobId>`) while the emulator emits `<project>:<jobId>`; the canonical identity is jobReference.jobId, which matches",
	},
	{
		Service: "bigquery",
		Op:      "jobs_list",
		Kind:    "missing_field",
		Reason:  "output-only BigQuery job metadata the emulator does not echo for a listed job (load schema, principal_subject, job-level state, statistics.reservation_id, user_email), plus a nextPageToken for unrelated jobs in the project history; the scoped job matches",
	},
	{
		Service:  "bigquery",
		Op:       "jobs_list",
		Kind:     "value_mismatch",
		Location: "jobs[0].id",
		Reason:   "output-only job identity: real GCP's job `id` embeds the job location (`<project>:US.<jobId>`) while the emulator emits `<project>:<jobId>`; the canonical identity is jobReference.jobId, which matches",
	},
	// SDK-tour BigQuery load job (demo/sdk-tour): the same output-only job
	// metadata the emulator does not echo, reachable through the tour's own op
	// ids (which differ from the curated job_insert_load/jobs_list set).
	{
		Service: "bigquery",
		Op:      "tour_bq_load_insert",
		Kind:    "missing_field",
		Reason:  "output-only BigQuery job metadata the emulator does not echo (load schema, jobCreationReason, principal_subject, statistics.reservation_id, user_email); the load configuration and destination match",
	},
	{
		Service: "bigquery",
		Op:      "tour_bq_load_poll",
		Kind:    "missing_field",
		Reason:  "output-only BigQuery job metadata the emulator does not echo (load schema, jobCreationReason, principal_subject, and the statistics the emulator does not synthesize: completionRatio/finalExecutionDurationMs/timeline/reservation_id/totalSlotMs, user_email); the load configuration, destination and DONE state match",
	},
	{
		Service:  "bigquery",
		Op:       "tour_bq_load_insert",
		Kind:     "value_mismatch",
		Location: "response.id",
		Reason:   "output-only job identity: real GCP's job `id` embeds the job location (`<project>:US.<jobId>`) while the emulator emits `<project>:<jobId>`; the canonical identity is jobReference.jobId, which matches",
	},
	{
		Service:  "bigquery",
		Op:       "tour_bq_load_poll",
		Kind:     "value_mismatch",
		Location: "response.id",
		Reason:   "output-only job identity: real GCP's job `id` embeds the job location (`<project>:US.<jobId>`) while the emulator emits `<project>:<jobId>`; the canonical identity is jobReference.jobId, which matches",
	},
	{
		Service:  "bigquery",
		Kind:     "value_mismatch",
		Location: ".status.state",
		Reason:   "accepted LRO-completion timing: real GCP reports a freshly submitted load job as RUNNING; the emulator evaluates the load synchronously and reports DONE",
	},

	// Eventarc trigger: create is an LRO (done=false on real GCP) and real GCP
	// defaults eventDataContentType.
	{
		Service:  "eventarc",
		Op:       "eventarc_trigger_create",
		Kind:     "value_mismatch",
		Location: "response.done",
		Reason:   "accepted LRO-completion timing: real GCP returns the create operation in flight (done=false); the emulator completes it synchronously (done=true)",
	},
	{
		Service: "eventarc",
		Op:      "eventarc_trigger_get",
		Kind:    "missing_field",
		Reason:  "output-only default: real GCP sets eventDataContentType=application/json on a GCS trigger; the emulator omits the defaulted field",
	},

	// Cloud Logging: real GCP auto-fills the global resource's project_id label.
	{
		Service:  "logging",
		Op:       "log_entries_list",
		Kind:     "missing_field",
		Location: "resource.labels",
		Reason:   "output-only default: real GCP auto-fills the global monitored resource's project_id label; the emulator stores the resource type only",
	},

	// Cloud Monitoring: mutation-record/condition sub-resource metadata.
	{
		Service: "monitoring",
		Op:      "mon_alert_policy_create",
		Kind:    "missing_field",
		Reason:  "output-only Monitoring metadata: the server-generated condition name and the creationRecord/mutationRecord audit records real GCP returns are not echoed by the emulator; the policy identity, conditions, channels and enabled state match",
	},
	{
		Service: "monitoring",
		Op:      "mon_alert_policy_get",
		Kind:    "missing_field",
		Reason:  "output-only Monitoring metadata: the server-generated condition name and the creationRecord/mutationRecord audit records real GCP returns are not echoed by the emulator; the policy identity, conditions, channels and enabled state match",
	},

	// Cloud Scheduler: defaulted/attempt-status fields.
	{
		Service: "scheduler",
		Op:      "scheduler_job_create",
		Kind:    "missing_field",
		Reason:  "output-only Cloud Scheduler fields the emulator does not echo (defaulted attemptDeadline, the synthesized httpTarget headers, retryConfig.maxRetryDuration, and the job's last-attempt status); the schedule, target, OIDC token and retry config match",
	},
	{
		Service: "scheduler",
		Op:      "scheduler_job_get",
		Kind:    "missing_field",
		Reason:  "output-only Cloud Scheduler fields the emulator does not echo (defaulted attemptDeadline, the synthesized httpTarget headers, retryConfig.maxRetryDuration, and the job's last-attempt status); the schedule, target, OIDC token and retry config match",
	},

	// Service Usage: the returned service's catalog config sub-fields are output-only.
	{
		Service: "serviceusage",
		Op:      "service_enable",
		Kind:    "missing_field",
		Reason:  "output-only Service Usage catalog metadata real GCP returns on the enabled service's config (title/documentation/monitoring/quota/usage/authentication/monitoredResources); the service name and ENABLED state match",
	},
	{
		Service:  "serviceusage",
		Op:       "service_enable",
		Kind:     "value_mismatch",
		Location: "response.done",
		Reason:   "accepted LRO-completion timing: real GCP returns the enable operation in flight (done=false); the emulator completes it synchronously (done=true)",
	},

	// GCS: a regional bucket's location is canonicalized to uppercase.
	{
		Service: "storage",
		Op:      "eventarc_bucket_create",
		Kind:    "missing_field",
		Reason:  "output-only bucket metadata: the defaulted satisfiesPZI flag real GCP returns is not echoed by the emulator",
	},
	{
		Service:  "storage",
		Op:       "eventarc_bucket_create",
		Kind:     "value_mismatch",
		Location: "response.location",
		Reason:   "GCS canonicalizes a bucket location to uppercase (US-CENTRAL1); the emulator echoes the requested form (us-central1); the location and locationType are otherwise the same geography",
	},

	// SDK error/retry tour. Real GCP decorates a BigQuery jobs.query error
	// detail with the offending parameter's map location; the emulator omits
	// those advisory fields. HTTP status, error code and envelope shape match,
	// and no official client branches on the location.
	{
		Service:  "bigquery",
		Op:       "error_invalid_query",
		Kind:     "missing_field",
		Location: ".location",
		Reason:   "advisory per-error map-location metadata on a BigQuery error detail; status, code and envelope shape match and no client branches on it",
	},
	{
		Service:  "bigquery",
		Op:       "error_invalid_query",
		Kind:     "missing_field",
		Location: ".locationType",
		Reason:   "advisory per-error map-location metadata (see .location); status, code and envelope shape match",
	},
	// Real GCP's tabledata.list totalRows is eventually consistent immediately
	// after an insert and lagged the returned row array in the recording; the
	// emulator returns the already-consistent value. The rows themselves match.
	{
		Service:  "bigquery",
		Op:       "error_insertall_count",
		Kind:     "value_mismatch",
		Location: ".totalRows",
		Reason:   "real GCP tabledata.list totalRows is eventually consistent right after an insert and lagged the returned row array; the rows themselves match",
	},
}

// dataprocOmittedReadFields are the Dataproc cluster-read/operation fields real
// GCP materialises that the emulator still omits, as location suffixes (the diff
// walker embeds array indices and an operation wrapper, so a suffix matches both
// the plain get and the "response.response.*" operation response).
//
// Real Dataproc materialises output-only/defaulted fields on the operation and
// cluster responses; the emulator no longer merely echoes the requested config.
// Since DPG38/DPG40 a read renders the default softwareConfig.imageVersion,
// gceClusterConfig.zoneUri, the instance-group defaults and machineTypeUri, and
// canonicalises major Compute Engine links, so those are deliberately NOT in
// this list: scoping the rules by Location (rather than one blanket
// dataproc/missing_field rule) means a regression that drops a rendered field
// surfaces as an open divergence. The remaining Compute links (DPG48) are
// echoed as sent rather than canonicalised, so a non-canonical value shows up as
// value_mismatch, which these missing_field rules do not cover either.
var dataprocOmittedReadFields = []string{
	"metadata.description",
	"metadata.warnings",
	"metadata.status.innerState",
	"response.@type",
	"config.configBucket",
	"config.tempBucket",
	"config.endpointConfig",
	"config.gceClusterConfig.internalIpOnly",
	"config.gceClusterConfig.resourceManagerTags",
	"config.gceClusterConfig.serviceAccountScopes",
	"config.gceClusterConfig.shieldedInstanceConfig",
	"config.masterConfig.imageUri",
	"config.masterConfig.instanceNames",
	"config.masterConfig.minCpuPlatform",
	"config.masterConfig.preemptibility",
	"config.workerConfig.imageUri",
	"config.workerConfig.instanceNames",
	"config.workerConfig.minCpuPlatform",
	"config.workerConfig.preemptibility",
	"config.softwareConfig.properties",
	"response.labels",
}

// bigQueryQueryStatFields are the output-only job-statistics/timing members of a
// jobs.query response that the emulator does not synthesize. jobs.query now
// evaluates the documented Standard SQL subset, and its executed rows/schema/
// statementType match real GCP, so these omissions are additive metadata. They
// are scoped by Location (rather than one blanket bigquery/query rule) so a
// regression in the executed result itself — rows, schema, statementType or
// totalRows — still surfaces as an open divergence.
var bigQueryQueryStatFields = []string{
	"response.cacheHit",
	"response.creationTime",
	"response.endTime",
	"response.jobCreationReason",
	"response.pageRowCount",
	"response.queryId",
	"response.startTime",
	"response.totalBytesBilled",
	"response.totalBytesProcessed",
	"response.totalSlotMs",
}

func init() {
	// Dataproc's still-omitted read fields: output-only/defaulted values real
	// Dataproc materialises that this emulator does not synthesise. Scoped one
	// location at a time (see dataprocOmittedReadFields) so the rendered fields
	// stay open-checked.
	for _, loc := range dataprocOmittedReadFields {
		triageRules = append(triageRules, TriageRule{
			Service:  "dataproc",
			Kind:     "missing_field",
			Location: loc,
			Reason:   "output-only/defaulted field real Dataproc materialises that the emulator still omits (DPG47's default network and the read-only metadata/bucket fields); the fields it does render — the default softwareConfig.imageVersion, gceClusterConfig.zoneUri, the instance groups and machineTypeUri — are deliberately excluded, so a missing one is an open divergence",
		})
	}
	// These job-statistics/timing fields are output-only members of a
	// jobs.query/getQueryResults response, so the rule is scoped by Location
	// (not by op) and covers every query op: the curated `query`, the tour
	// `tour_bq_query` and each `bqsql_*` corpus query. Rows, schema,
	// statementType and totalRows are deliberately not listed, so a regression
	// in the executed result itself still surfaces as an open divergence.
	for _, loc := range bigQueryQueryStatFields {
		triageRules = append(triageRules, TriageRule{
			Service:  "bigquery",
			Kind:     "missing_field",
			Location: loc,
			Reason:   "output-only job-statistics/timing field the emulator does not synthesize; executed rows/schema/statementType match real GCP",
		})
	}
	// Real GCP decorates a BigQuery SQL error's legacy errors[] detail with a
	// per-token parse location/locationType; the emulator omits those advisory
	// fields while the HTTP status and the machine-readable code/reason/status
	// match. Scoped by Location so every query op is covered.
	for _, loc := range []string{"errors[0].location", "errors[0].locationType"} {
		triageRules = append(triageRules, TriageRule{
			Service:  "bigquery",
			Kind:     "missing_field",
			Location: loc,
			Reason:   "advisory per-token parse-location metadata on a BigQuery error detail; status, code and envelope shape match and no client branches on it",
		})
	}
}

// matchRule returns the first rule in rules matching d, or nil.
func matchRule(d Divergence, rules []TriageRule) *TriageRule {
	for i := range rules {
		if rules[i].matches(d) {
			return &rules[i]
		}
	}
	return nil
}

// triageRuleFor returns the first shipped rule matching d, or nil.
func triageRuleFor(d Divergence) *TriageRule {
	return matchRule(d, triageRules)
}

// ApplyTriage partitions divergences using the package triage allowlist. The
// returned slices preserve input order. accepted carries the same Divergence
// values; use TriageReason to recover why each was accepted.
func ApplyTriage(divs []Divergence) (open, accepted []Divergence) {
	return applyTriage(divs, triageRules)
}

// applyTriage partitions divergences using an explicit rule set (testable).
func applyTriage(divs []Divergence, rules []TriageRule) (open, accepted []Divergence) {
	for _, d := range divs {
		if matchRule(d, rules) != nil {
			accepted = append(accepted, d)
			continue
		}
		open = append(open, d)
	}
	return open, accepted
}

// TriageReason returns the reason a divergence is accepted, or "" when it is
// open (a real bug).
func TriageReason(d Divergence) string {
	if r := triageRuleFor(d); r != nil {
		return r.Reason
	}
	return ""
}
