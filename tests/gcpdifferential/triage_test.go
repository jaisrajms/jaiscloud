//go:build gcp_differential

package gcpdifferential

import (
	"strings"
	"testing"
)

// TestTriageRulesDocumented guards that every shipped rule carries a reason and
// is constrained in at least one dimension — an unconstrained rule would
// suppress the entire report.
func TestTriageRulesDocumented(t *testing.T) {
	if len(triageRules) == 0 {
		t.Fatal("triageRules is empty")
	}
	for i, r := range triageRules {
		if strings.TrimSpace(r.Reason) == "" {
			t.Errorf("triageRules[%d] (%s/%s/%s/%s) has no Reason", i, r.Service, r.Op, r.Kind, r.Location)
		}
		if r.Service == "" && r.Op == "" && r.Kind == "" && r.Location == "" {
			t.Errorf("triageRules[%d] is unconstrained and would suppress every divergence", i)
		}
	}
}

// TestApplyTriageMechanism exercises the suppression mechanism with an explicit
// rule set, then confirms the shipped rules accept a declared-by-design finding
// and leave a real bug open.
func TestApplyTriageMechanism(t *testing.T) {
	rules := []TriageRule{{
		Service:  "storage",
		Kind:     "missing_field",
		Location: ".rpo",
		Reason:   "hypothetical",
	}}
	divs := []Divergence{
		{Service: "storage", Kind: "missing_field", Location: "response.rpo"},
		{Service: "storage", Kind: "missing_field", Location: "response.items[0].rpo"},
		{Service: "pubsub", Kind: "missing_field", Location: "response.rpo"},
		{Service: "storage", Kind: "value_mismatch", Location: "response.rpo"},
	}
	open, accepted := applyTriage(divs, rules)
	if len(accepted) != 2 {
		t.Fatalf("accepted = %d, want 2", len(accepted))
	}
	if len(open) != 2 {
		t.Fatalf("open = %d, want 2", len(open))
	}

	// A declared-by-design finding is accepted...
	if got := TriageReason(Divergence{Service: "bigquery", Op: "query", Kind: "array_length_mismatch", Location: "response.rows"}); got == "" {
		t.Error("expected bigquery query rows to be accepted by the shipped rules")
	}
	// ...but a real bug is not.
	real := Divergence{Service: "secretmanager", Op: "secret_add_version", Kind: "missing_field", Location: "response.etag"}
	if got := TriageReason(real); got != "" {
		t.Errorf("secret version etag must stay OPEN, got reason %q", got)
	}

	// ApplyTriage partitions without dropping anything.
	open, accepted = ApplyTriage(divs)
	if len(open)+len(accepted) != len(divs) {
		t.Fatalf("partition lost findings: %d open + %d accepted != %d", len(open), len(accepted), len(divs))
	}
}
