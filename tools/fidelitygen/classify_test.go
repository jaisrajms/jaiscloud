//go:build gcp_conformance

package main

import (
	"strings"
	"testing"
)

func TestClassify(t *testing.T) {
	ga := Facts{
		Service: "storage", Operation: "Storage.ObjectsGet", Transport: "rest",
		Implemented: true, DiscoveryMethod: "storage.objects.get", PersistentBackend: true,
	}
	limitedByFinding := ga
	limitedByFinding.Findings = []Finding{{Severity: "medium", Kind: "unknown_field", Path: "x"}}

	tests := []struct {
		name         string
		facts        Facts
		wantState    string
		wantReason   string // substring
		wantEvidence string // substring
	}{
		{"ga baseline", ga, StateGA, "", ""},
		{
			"not implemented",
			Facts{Service: "clouddns", Operation: "CloudDNS.Unimplemented", Transport: "rest"},
			StateUnsupported, "not implemented", "not in registry",
		},
		{
			"non-allowlisted medium downgrades",
			limitedByFinding, StateLimited, "wire divergence", "",
		},
		{
			"allowlisted high does not downgrade",
			func() Facts {
				f := ga
				f.Findings = []Finding{{Severity: "high", Kind: "wrong_type", Path: "y", Allowlisted: true}}
				return f
			}(), StateGA, "", "",
		},
		{
			"info does not downgrade",
			func() Facts {
				f := ga
				f.Findings = []Finding{{Severity: "info", Kind: "unknown_field", Path: "z"}}
				return f
			}(), StateGA, "", "",
		},
		{
			"no discovery method downgrades",
			func() Facts { f := ga; f.DiscoveryMethod = ""; return f }(),
			StateLimited, "no matching Discovery", "",
		},
		{
			"mutating without postgres downgrades",
			func() Facts { f := ga; f.Mutating = true; f.PersistentBackend = false; return f }(),
			StateLimited, "without a Postgres", "",
		},
		{
			"override downgrade applies",
			func() Facts {
				f := ga
				f.Override = &Override{State: StatePreview, Reason: "under development"}
				return f
			}(), StatePreview, "under development", "",
		},
		{
			"override upgrade rejected without allow_upgrade",
			func() Facts {
				f := limitedByFinding
				f.Override = &Override{State: StateGA, Reason: "trust me"}
				return f
			}(), StateLimited, "", "rejected",
		},
		{
			"override upgrade allowed with allow_upgrade",
			func() Facts {
				f := limitedByFinding
				f.Override = &Override{State: StateGA, Reason: "covered by e2e", AllowUpgrade: true}
				return f
			}(), StateGA, "", "",
		},
		{
			"invalid override state ignored",
			func() Facts {
				f := ga
				f.Override = &Override{State: "bogus"}
				return f
			}(), StateGA, "", "invalid state",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Classify(tc.facts)
			if got.State != tc.wantState {
				t.Fatalf("state = %q, want %q (reason %q)", got.State, tc.wantState, got.Reason)
			}
			if tc.wantReason != "" && !strings.Contains(got.Reason, tc.wantReason) {
				t.Errorf("reason = %q, want substring %q", got.Reason, tc.wantReason)
			}
			if tc.wantEvidence != "" {
				joined := strings.Join(got.Evidence, "; ")
				if !strings.Contains(joined, tc.wantEvidence) {
					t.Errorf("evidence = %q, want substring %q", joined, tc.wantEvidence)
				}
			}
			if got.State != StateGA && got.Reason == "" {
				t.Errorf("non-ga cell has empty reason")
			}
			if got.Service != tc.facts.Service || got.Operation != tc.facts.Operation {
				t.Errorf("cell identity not preserved: %+v", got)
			}
		})
	}
}
