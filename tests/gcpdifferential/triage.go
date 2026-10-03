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
	Service  string // "" matches any service
	Op       string // "" matches any op
	Kind     string // "" matches any kind
	Location string // "" matches any location; otherwise a suffix match
	Reason   string
}

func (r TriageRule) matches(d Divergence) bool {
	if r.Service != "" && r.Service != d.Service {
		return false
	}
	if r.Op != "" && r.Op != d.Op {
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
// contract (docs/GA.md, docs/fidelity/fidelity-matrix.md), or (b) additive /
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
	// mislead one (GA.md §5 records the same class of finding as non-fatal).
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

	// ── Authz paths: not enforced by design (G1, docs/GA.md §10) ─────────────
	// Each *_noauth scenario is sent without an Authorization header. Real GCP
	// rejects it (401 UNAUTHENTICATED) while the emulator serves the request, so
	// the status and the whole body differ. This is the documented accepted
	// risk, recorded (not silently dropped) with an explicit reason. The rules
	// are scoped to the *_noauth op so a genuine authz divergence elsewhere
	// still surfaces as open.
	{
		Service: "storage",
		Op:      "storage_buckets_list_noauth",
		Reason:  "authz is not enforced by design (G1, docs/GA.md §10): real GCP returns 401 UNAUTHENTICATED while the emulator serves the request; the divergence is the accepted-risk record, not a bug",
	},
	{
		Service: "compute",
		Op:      "compute_instances_list_noauth",
		Reason:  "authz is not enforced by design (G1, docs/GA.md §10): real GCP returns 401 UNAUTHENTICATED while the emulator serves the request; the divergence is the accepted-risk record, not a bug",
	},
	{
		Service: "sqladmin",
		Op:      "sql_instances_list_noauth",
		Reason:  "authz is not enforced by design (G1, docs/GA.md §10): real GCP returns 401 UNAUTHENTICATED while the emulator serves the request; the divergence is the accepted-risk record, not a bug",
	},
	{
		Service: "redis",
		Op:      "redis_instances_list_noauth",
		Reason:  "authz is not enforced by design (G1, docs/GA.md §10): real GCP returns 401 UNAUTHENTICATED while the emulator serves the request; the divergence is the accepted-risk record, not a bug",
	},
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
	for _, loc := range bigQueryQueryStatFields {
		triageRules = append(triageRules, TriageRule{
			Service:  "bigquery",
			Op:       "query",
			Kind:     "missing_field",
			Location: loc,
			Reason:   "output-only job-statistics/timing field the emulator does not synthesize; executed rows/schema/statementType match real GCP",
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
