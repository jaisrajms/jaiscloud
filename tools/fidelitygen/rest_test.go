//go:build gcp_conformance

package main

import (
	"testing"

	conf "jaiscloud/tests/gcpconformance"
)

const (
	testDiscoveryDir   = "../../tests/gcpconformance/discovery"
	testReportPath     = "../../tests/gcpconformance/testdata/report/report.json"
	testTranscriptPath = "../../tests/gcpconformance/testdata/transcripts.json"
	testOverridesPath  = "../../docs/fidelity-overrides.yaml"
)

func loadDocsForTest(t *testing.T) map[string]*conf.DiscoveryDoc {
	t.Helper()
	docs, err := conf.LoadSnapshots(testDiscoveryDir)
	if err != nil {
		t.Fatalf("LoadSnapshots: %v", err)
	}
	return docs
}

func loadReportForTest(t *testing.T) *conf.Report {
	t.Helper()
	rep, err := conf.ReadReport(testReportPath)
	if err != nil {
		t.Fatalf("ReadReport: %v", err)
	}
	return rep
}

// restCoverageForTest derives the transcript coverage join over the committed
// transcript, mirroring what fidelitygen's run() feeds RestFacts.
func restCoverageForTest(t *testing.T, ops []conf.Operation, docs map[string]*conf.DiscoveryDoc) map[string]int {
	t.Helper()
	tr, err := conf.ReadTranscript(testTranscriptPath)
	if err != nil {
		t.Fatalf("ReadTranscript: %v", err)
	}
	return conf.TranscriptCoverage(ops, docs, tr)
}

func loadOverridesForTest(t *testing.T, ops []conf.Operation) *Overrides {
	t.Helper()
	ov, err := LoadOverrides(testOverridesPath, ops, conf.EnumerateGRPC())
	if err != nil {
		t.Fatalf("LoadOverrides: %v", err)
	}
	return ov
}

func findFact(t *testing.T, facts []Facts, operation string) Facts {
	t.Helper()
	for _, f := range facts {
		if f.Operation == operation {
			return f
		}
	}
	t.Fatalf("no fact for operation %s", operation)
	return Facts{}
}

// TestRestFactsCount checks the acceptance criterion: one REST fact per
// enumerated operation, Implemented, transport rest.
func TestRestFactsCount(t *testing.T) {
	ops := conf.Enumerate()
	facts := RestFacts(ops, loadDocsForTest(t), loadReportForTest(t), loadOverridesForTest(t, ops), restCoverageForTest(t, ops, loadDocsForTest(t)))

	if len(facts) != len(ops) {
		t.Fatalf("got %d facts, want one per enumerated op (%d)", len(facts), len(ops))
	}

	seen := map[string]bool{}
	covered, mutating := 0, 0
	for _, f := range facts {
		if f.Transport != "rest" {
			t.Errorf("%s: transport = %q, want rest", f.Operation, f.Transport)
		}
		if !f.Implemented {
			t.Errorf("%s: Implemented = false, want true", f.Operation)
		}
		if f.Service == "" {
			t.Errorf("%s: empty service", f.Operation)
		}
		if seen[f.Operation] {
			t.Errorf("%s: duplicate fact", f.Operation)
		}
		seen[f.Operation] = true
		if f.DiscoveryMethod != "" {
			covered++
		}
		if f.Mutating {
			mutating++
		}
	}
	t.Logf("rest facts: %d (enumerated ops: %d); discovery-covered: %d; mutating: %d",
		len(facts), len(ops), covered, mutating)
}

// TestRestFactsDiscoveryCoverage pins a known-covered op to a Discovery method
// and known-uncovered ops to no method.
func TestRestFactsDiscoveryCoverage(t *testing.T) {
	ops := conf.Enumerate()
	facts := RestFacts(ops, loadDocsForTest(t), loadReportForTest(t), loadOverridesForTest(t, ops), restCoverageForTest(t, ops, loadDocsForTest(t)))

	if got := findFact(t, facts, "Storage.ObjectsGet"); got.DiscoveryMethod == "" {
		t.Errorf("Storage.ObjectsGet: DiscoveryMethod is empty, want a method id")
	} else {
		t.Logf("Storage.ObjectsGet -> %s", got.DiscoveryMethod)
	}

	for _, op := range []string{"BigQuery.Models", "Function.GetLocation"} {
		if got := findFact(t, facts, op); got.DiscoveryMethod != "" {
			t.Errorf("%s: DiscoveryMethod = %q, want empty (uncovered)", op, got.DiscoveryMethod)
		}
	}
}

// TestRestFactsMutating checks the mutating classification on representative
// actions.
func TestRestFactsMutating(t *testing.T) {
	tests := []struct {
		operation string
		want      bool
	}{
		{"Storage.ObjectsInsert", true},
		{"Storage.ObjectsGet", false},
		{"Storage.BucketsDelete", true},
		{"PubSub.TopicList", false},
	}
	for _, tc := range tests {
		if got := isMutating(operationAction(t, tc.operation)); got != tc.want {
			t.Errorf("isMutating(%s) = %v, want %v", tc.operation, got, tc.want)
		}
	}

	ops := conf.Enumerate()
	facts := RestFacts(ops, loadDocsForTest(t), loadReportForTest(t), loadOverridesForTest(t, ops), restCoverageForTest(t, ops, loadDocsForTest(t)))
	if got := findFact(t, facts, "Storage.ObjectsInsert"); !got.Mutating {
		t.Errorf("Storage.ObjectsInsert: Mutating = false, want true")
	}
	if got := findFact(t, facts, "Storage.ObjectsGet"); got.Mutating {
		t.Errorf("Storage.ObjectsGet: Mutating = true, want false")
	}
}

// TestRestFactsVerified checks the response-evidence signal: a Discovery-mapped
// op with a validated transcript response is Verified, while a Discovery-mapped
// op with no recorded response stays unverified. The signal is independent of
// State (an op can be `limited` yet verified, or `ga` yet unverified) and of the
// conformance report, so this test passes a nil report and derives the two
// examples from the coverage map rather than pinning a specific operation
// (which drifts as transcripts are re-recorded).
func TestRestFactsVerified(t *testing.T) {
	ops := conf.Enumerate()
	docs := loadDocsForTest(t)
	coverage := restCoverageForTest(t, ops, docs)
	facts := RestFacts(ops, docs, nil, loadOverridesForTest(t, ops), coverage)

	var covered, unverified *Facts
	for i := range facts {
		f := &facts[i]
		if f.DiscoveryMethod == "" {
			continue // unmapped ops can never be verified
		}
		switch {
		case f.Verified && covered == nil:
			covered = f
		case !f.Verified && unverified == nil:
			unverified = f
		}
	}
	if covered == nil {
		t.Fatalf("no Discovery-mapped op with a validated transcript response (coverage map empty?)")
	}
	if covered.RESTEvidenceEntries == 0 {
		t.Errorf("%s: RESTEvidenceEntries = 0, want > 0", covered.Operation)
	}
	if !Classify(*covered).Verified {
		t.Errorf("%s: cell Verified = false, want true", covered.Operation)
	}
	if unverified == nil {
		t.Fatalf("no Discovery-mapped op without a validated transcript response (coverage is total?)")
	}
	if unverified.RESTEvidenceEntries != 0 {
		t.Errorf("%s: RESTEvidenceEntries = %d, want 0", unverified.Operation, unverified.RESTEvidenceEntries)
	}
	if Classify(*unverified).Verified {
		t.Errorf("%s: cell Verified = true, want false", unverified.Operation)
	}
}

// TestRestFactsNilCoverageIsTolerated keeps RestFacts usable without a
// transcript (a fresh clone): nil coverage leaves every op unverified.
func TestRestFactsNilCoverageIsTolerated(t *testing.T) {
	ops := conf.Enumerate()
	facts := RestFacts(ops, loadDocsForTest(t), loadReportForTest(t), loadOverridesForTest(t, ops), nil)
	for _, f := range facts {
		if f.Verified {
			t.Fatalf("%s: Verified = true with nil coverage", f.Operation)
		}
	}
}

// operationAction returns the registry action for an operation key.
func operationAction(t *testing.T, operation string) string {
	t.Helper()
	for _, op := range conf.Enumerate() {
		if op.Key() == operation {
			return op.Action
		}
	}
	t.Fatalf("operation %s not in registry", operation)
	return ""
}

// TestRestFactsOverrideWiring checks the curated override reaches the fact.
func TestRestFactsOverrideWiring(t *testing.T) {
	ops := conf.Enumerate()
	facts := RestFacts(ops, loadDocsForTest(t), loadReportForTest(t), loadOverridesForTest(t, ops), restCoverageForTest(t, ops, loadDocsForTest(t)))

	got := findFact(t, facts, "Dataproc.SubmitJob")
	if got.Override == nil {
		t.Fatalf("Dataproc.SubmitJob: Override = nil, want ga/allow_upgrade")
	}
	if got.Override.State != StateGA || !got.Override.AllowUpgrade {
		t.Errorf("Dataproc.SubmitJob: Override = %+v, want state ga with AllowUpgrade", *got.Override)
	}

	// A service default (clouddns: limited) reaches its operations too.
	if got := findFact(t, facts, "CloudDNS.ManagedZoneGet"); got.Override == nil || got.Override.State != StateLimited {
		t.Errorf("CloudDNS.ManagedZoneGet: Override = %+v, want limited", got.Override)
	}
}

// TestRestFactsFindingAttribution checks that a report finding keyed by
// (service, Discovery method) lands on the operation that resolves to that
// method, and that unrelated operations stay clean.
func TestRestFactsFindingAttribution(t *testing.T) {
	ops := conf.Enumerate()
	docs := loadDocsForTest(t)

	// A synthetic report keeps this attribution check deterministic and
	// independent of the recorded transcript (the executed-SQL work removed the
	// last real bigquery tabledata.list divergence): one finding keyed to the
	// tabledata.list Discovery method, one keyed to an unknown method that must
	// fall back to the whole service.
	report := &conf.Report{Divergences: []conf.Divergence{
		{
			Service: "bigquery", Method: "bigquery.tabledata.list",
			Kind: "missing_field", Path: "response.startIndex", Severity: "low",
			Expected: "\"0\"", Actual: "absent",
		},
		{
			Service: "bigquery", Method: "bigquery.nonexistent.method",
			Kind: "extra_field", Path: "response.x", Severity: "info",
			Expected: "absent", Actual: "1",
		},
	}}
	facts := RestFacts(ops, docs, report, loadOverridesForTest(t, ops), restCoverageForTest(t, ops, docs))

	// The tabledata.list finding lands on BigQuery.ListRows (the unknown-method
	// finding also falls back to the whole service, so ListRows carries both).
	listRows := findFact(t, facts, "BigQuery.ListRows")
	foundSpecific := false
	for _, f := range listRows.Findings {
		if f.Kind == "missing_field" && f.Path == "response.startIndex" {
			foundSpecific = true
		}
	}
	if !foundSpecific {
		t.Errorf("BigQuery.ListRows: findings = %+v, want the tabledata.list divergence", listRows.Findings)
	}
	if got := findFact(t, facts, "Storage.ObjectsGet"); len(got.Findings) != 0 {
		t.Errorf("Storage.ObjectsGet: unexpected findings %+v", got.Findings)
	}
	// The unknown-method finding falls back to the whole bigquery service.
	if got := findFact(t, facts, "BigQuery.Query"); len(got.Findings) != 1 {
		t.Errorf("BigQuery.Query: findings = %+v, want the service-wide fallback", got.Findings)
	}

	// Report attribution stats so service-wide fallbacks stay visible.
	services := map[string]bool{}
	for _, op := range ops {
		services[op.Service] = true
	}
	resolver := conf.NewActionResolver(docs)
	byMethod := methodIndex(resolver, ops)
	specific, serviceWide, dropped := 0, 0, 0
	var wide []string
	for _, d := range report.Divergences {
		switch {
		case len(byMethod[attributionKey(d.Service, d.Method)]) > 0:
			specific++
		case services[d.Service]:
			serviceWide++
			wide = append(wide, d.Service+"/"+d.Method)
		default:
			dropped++
		}
	}
	t.Logf("report attribution: %d specific, %d service-wide, %d dropped (of %d)",
		specific, serviceWide, dropped, len(report.Divergences))
	for _, w := range wide {
		t.Logf("  service-wide: %s", w)
	}
	if specific == 0 {
		t.Errorf("no divergence attributed to a specific operation; mapping looks broken")
	}
}
