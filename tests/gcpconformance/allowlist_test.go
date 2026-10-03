//go:build gcp_conformance

package gcpconformance

import "testing"

// TestAllowlistEmpty asserts the envelope allowlist has been driven to zero:
// ValidateErrorEnvelope now models GCP's per-generation envelope variance
// directly, so no shape-variance finding needs suppressing.
func TestAllowlistEmpty(t *testing.T) {
	if len(errorEnvelopeAllowlist) != 0 {
		t.Fatalf("errorEnvelopeAllowlist = %d entries, want 0", len(errorEnvelopeAllowlist))
	}
}

// TestApplyAllowlistMechanism exercises the suppression mechanism with an
// explicit rule set, since the shipped list is empty.
func TestApplyAllowlistMechanism(t *testing.T) {
	rules := []AllowRule{{
		Kind:     kindBadErrorEnvelope,
		Path:     "error.status",
		Services: []string{"storage"},
		Reason:   "hypothetical",
	}}
	divs := []Divergence{
		{Kind: kindBadErrorEnvelope, Path: "entry[1] storage GET /x error.status", Service: "storage"},
		{Kind: kindBadErrorEnvelope, Path: "entry[2] pubsub GET /x error.status", Service: "pubsub"},
		{Kind: kindWrongType, Path: "schema", Service: "storage"},
	}
	kept, suppressed := applyAllowlist(divs, rules)
	if len(suppressed) != 1 || suppressed[0].Service != "storage" {
		t.Fatalf("suppressed = %+v, want only the storage error.status", suppressed)
	}
	if len(kept) != 2 {
		t.Fatalf("kept = %d, want 2", len(kept))
	}
	// The shipped allowlist suppresses nothing.
	if _, s := ApplyAllowlist(divs); len(s) != 0 {
		t.Fatalf("ApplyAllowlist suppressed %d with an empty allowlist", len(s))
	}
}
