//go:build gcp_conformance

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	conf "jaiscloud/tests/gcpconformance"
)

func TestValidateEntry(t *testing.T) {
	tests := []struct {
		name    string
		e       overrideEntry
		wantErr string // substring, empty = no error
	}{
		{"limited ok", overrideEntry{State: StateLimited, Reason: "why"}, ""},
		{"preview ok", overrideEntry{State: StatePreview, Reason: "why"}, ""},
		{"missing state", overrideEntry{Reason: "why"}, "missing state"},
		{"unknown state", overrideEntry{State: "nope", Reason: "why"}, "unknown state"},
		{"non-ga needs reason", overrideEntry{State: StateLimited}, "requires a reason"},
		{"ga needs allow_upgrade", overrideEntry{State: StateGA}, "allow_upgrade"},
		{"ga with allow_upgrade ok", overrideEntry{State: StateGA, AllowUpgrade: true}, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := validateEntry("f.yaml", "where", tc.e)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want substring %q", err, tc.wantErr)
			}
		})
	}
}

func TestLoadOverridesRegistryChecks(t *testing.T) {
	ops := conf.Enumerate()
	dir := t.TempDir()
	write := func(body string) string {
		p := filepath.Join(dir, "o.yaml")
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}

	if _, err := LoadOverrides(write("defaults:\n  nosuchservice:\n    state: limited\n    reason: x\n"), ops, nil); err == nil {
		t.Error("want error for unknown service in defaults")
	}
	if _, err := LoadOverrides(write("operations:\n  - service: storage\n    operation: Storage.Nope\n    state: limited\n    reason: x\n"), ops, nil); err == nil {
		t.Error("want error for unknown operation")
	}
	if _, err := LoadOverrides(write("operations:\n  - service: dataproc\n    operation: Dataproc.SubmitJob\n    state: ga\n    allow_upgrade: true\n"), ops, nil); err != nil {
		t.Errorf("valid override rejected: %v", err)
	}
}

// TestLoadOverridesSeed validates the committed contract against the real
// registry and checks lookup precedence (operation over service default).
func TestLoadOverridesSeed(t *testing.T) {
	ops := conf.Enumerate()
	ov, err := LoadOverrides(filepath.Join("..", "..", "docs", "fidelity-overrides.yaml"), ops, conf.EnumerateGRPC())
	if err != nil {
		t.Fatalf("LoadOverrides(seed): %v", err)
	}
	if ov.Len() == 0 {
		t.Fatal("seed loaded no overrides")
	}

	if got := ov.For("dataproc", "Dataproc.SubmitJob"); got == nil || got.State != StateGA || !got.AllowUpgrade {
		t.Errorf("dataproc SubmitJob override = %+v, want ga allow_upgrade", got)
	}
	if got := ov.For("clouddns", "CloudDNS.Unimplemented"); got == nil || got.State != StateUnsupported {
		t.Errorf("clouddns Unimplemented override = %+v, want unsupported", got)
	}
	// Operation-level override wins over the service default (clouddns: limited).
	if got := ov.For("clouddns", "CloudDNS.ManagedZonesGet"); got == nil || got.State != StateLimited {
		t.Errorf("clouddns default override = %+v, want limited", got)
	}
	// Service default applies to an operation with no explicit entry.
	if got := ov.For("bigquery", "BigQuery.Query"); got == nil || got.State != StateLimited {
		t.Errorf("bigquery default override = %+v, want limited", got)
	}
	if got := ov.For("storage", "Storage.ObjectsGet"); got != nil {
		t.Errorf("storage should have no override, got %+v", got)
	}
}
