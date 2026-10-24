//go:build gcp_conformance

package gcpconformance

import "testing"

// testDocs builds a minimal Discovery document with two widgets methods so the
// evidence join can be exercised without the vendored snapshots or a network.
func testDocs() map[string]*DiscoveryDoc {
	return map[string]*DiscoveryDoc{
		"example": {
			Name:    "example",
			Version: "v1",
			Resources: map[string]*Resource{
				"widgets": {Methods: map[string]*Method{
					"get":  {ID: "example.widgets.get", HTTPMethod: "GET", Path: "/v1/widgets/{id}"},
					"list": {ID: "example.widgets.list", HTTPMethod: "GET", Path: "/v1/widgets"},
				}},
			},
		},
	}
}

// TestTranscriptCoverageMappedAndUncovered pins the plan's acceptance
// criterion: a mapped operation with a validating transcript entry is covered;
// a mapped operation with no entry is reported uncovered (absent), and a verb
// with no Discovery method can never be covered.
func TestTranscriptCoverageMappedAndUncovered(t *testing.T) {
	ops := []Operation{
		{ProviderPrefix: "Example", Action: "WidgetsGet", Service: "example"},
		{ProviderPrefix: "Example", Action: "WidgetsList", Service: "example"},
		{ProviderPrefix: "Example", Action: "WidgetsDelete", Service: "example"}, // no Discovery method
	}
	tr := Transcript{Entries: []Entry{
		{Service: "example", Method: "GET", Path: "/v1/widgets/w1", Status: 200},
	}}

	cov := TranscriptCoverage(ops, testDocs(), tr)

	if got := cov["Example.WidgetsGet"]; got != 1 {
		t.Errorf("Example.WidgetsGet: coverage = %d, want 1 (a validated response exists)", got)
	}
	if _, ok := cov["Example.WidgetsList"]; ok {
		t.Errorf("Example.WidgetsList: reported covered, want uncovered (no transcript entry)")
	}
	if _, ok := cov["Example.WidgetsDelete"]; ok {
		t.Errorf("Example.WidgetsDelete: reported covered, want uncovered (no Discovery method)")
	}
}

// TestTranscriptCoverageIgnoresNonSuccessAndUnmatched ensures only 2xx entries
// that match a Discovery method contribute evidence.
func TestTranscriptCoverageIgnoresNonSuccessAndUnmatched(t *testing.T) {
	ops := []Operation{{ProviderPrefix: "Example", Action: "WidgetsList", Service: "example"}}
	tr := Transcript{Entries: []Entry{
		{Service: "example", Method: "GET", Path: "/v1/widgets", Status: 404},       // error
		{Service: "example", Method: "GET", Path: "/v1/unknown/thing", Status: 200}, // unmatched
	}}

	if cov := TranscriptCoverage(ops, testDocs(), tr); len(cov) != 0 {
		t.Fatalf("coverage = %v, want empty (no successful matched response)", cov)
	}
}

// TestTranscriptCoverageCountsRepeatedEntries confirms repeated validated
// responses to the same method accumulate.
func TestTranscriptCoverageCountsRepeatedEntries(t *testing.T) {
	ops := []Operation{{ProviderPrefix: "Example", Action: "WidgetsList", Service: "example"}}
	tr := Transcript{Entries: []Entry{
		{Service: "example", Method: "GET", Path: "/v1/widgets", Status: 200},
		{Service: "example", Method: "GET", Path: "/v1/widgets", Status: 200},
	}}

	if got := TranscriptCoverage(ops, testDocs(), tr)["Example.WidgetsList"]; got != 2 {
		t.Fatalf("Example.WidgetsList: coverage = %d, want 2", got)
	}
}
