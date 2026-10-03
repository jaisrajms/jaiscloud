//go:build gcp_conformance

package main

import (
	"testing"

	conf "jaiscloud/tests/gcpconformance"
)

// TestGRPCFactsEveryMethod checks the acceptance criterion: one fact per
// enumerated gRPC method, all on the grpc transport and implemented.
func TestGRPCFactsEveryMethod(t *testing.T) {
	services := conf.EnumerateGRPC()
	if len(services) == 0 {
		t.Fatal("EnumerateGRPC returned no services")
	}

	want := map[string]bool{}
	for _, s := range services {
		if s.WireService == "" || s.Service == "" {
			t.Errorf("service with empty wire/service name: %+v", s)
		}
		if len(s.Methods) == 0 {
			t.Errorf("%s (%s): no methods", s.WireService, s.Service)
		}
		for _, m := range s.Methods {
			want[s.Service+"/"+m] = true
		}
	}

	facts := GRPCFacts(services, &Overrides{}, nil)
	if len(facts) != len(want) {
		t.Fatalf("got %d facts, want one per unique (service, method) (%d)", len(facts), len(want))
	}

	seen := map[string]bool{}
	for _, f := range facts {
		if f.Transport != "grpc" {
			t.Errorf("%s/%s: transport = %q, want grpc", f.Service, f.Operation, f.Transport)
		}
		if !f.Implemented {
			t.Errorf("%s/%s: Implemented = false, want true", f.Service, f.Operation)
		}
		if f.Service == "" || f.Operation == "" {
			t.Errorf("fact with empty service/operation: %+v", f)
		}
		if f.DiscoveryMethod != "" {
			t.Errorf("%s/%s: DiscoveryMethod = %q, want empty", f.Service, f.Operation, f.DiscoveryMethod)
		}
		key := f.Service + "/" + f.Operation
		if seen[key] {
			t.Errorf("%s: duplicate fact", key)
		}
		seen[key] = true
	}
	t.Logf("grpc facts: %d methods across %d registered services", len(facts), len(services))
	for _, s := range services {
		t.Logf("  %-50s -> %-14s %3d methods", s.WireService, s.Service, len(s.Methods))
	}
}

// TestGRPCFactDefaultsToLimited checks that a cell with no override classifies
// to limited through Classify, via the injected default override.
func TestGRPCFactDefaultsToLimited(t *testing.T) {
	services := []conf.GRPCService{{
		WireService: "google.storage.v2.Storage",
		Service:     "storage",
		Methods:     []string{"GetObject"},
	}}

	for _, ov := range []*Overrides{nil, {}} {
		facts := GRPCFacts(services, ov, nil)
		if len(facts) != 1 {
			t.Fatalf("got %d facts, want 1", len(facts))
		}
		if facts[0].Override == nil {
			t.Fatalf("no default override attached")
		}
		cell := Classify(facts[0])
		if cell.State != StateLimited {
			t.Errorf("state = %q, want %q (reason %q)", cell.State, StateLimited, cell.Reason)
		}
		if cell.Reason != grpcDefaultReason {
			t.Errorf("reason = %q, want %q", cell.Reason, grpcDefaultReason)
		}
	}
}

// TestGRPCFactOverrideWins checks that a curated override replaces the default
// limited classification, in both directions (upgrade to ga with
// allow_upgrade, and downgrade to preview).
func TestGRPCFactOverrideWins(t *testing.T) {
	services := []conf.GRPCService{{
		WireService: "google.storage.v2.Storage",
		Service:     "storage",
		Methods:     []string{"GetObject"},
	}}

	upgrade := &Overrides{byOp: map[string]Override{
		"storage/GetObject": {State: StateGA, Reason: "verified against proto", AllowUpgrade: true},
	}}
	facts := GRPCFacts(services, upgrade, nil)
	if got := facts[0].Override; got == nil || got.State != StateGA || !got.AllowUpgrade {
		t.Fatalf("Override = %+v, want ga with AllowUpgrade", got)
	}
	if cell := Classify(facts[0]); cell.State != StateGA {
		t.Errorf("state = %q, want ga", cell.State)
	}

	downgrade := &Overrides{byService: map[string]Override{
		"storage": {State: StatePreview, Reason: "not yet conformance-tested"},
	}}
	facts = GRPCFacts(services, downgrade, nil)
	if cell := Classify(facts[0]); cell.State != StatePreview {
		t.Errorf("state = %q, want preview", cell.State)
	}
}

// TestGRPCFactsConsumesReport verifies evidence-driven classification: a
// method whose checks all pass derives to ga, a method with a failing check is
// downgraded with a high-severity finding, and an uncovered method keeps the
// default limited override.
func TestGRPCFactsConsumesReport(t *testing.T) {
	services := []conf.GRPCService{{
		WireService: "google.storage.v2.Storage",
		Service:     "storage",
		Methods:     []string{"GetObject", "PutObject", "DeleteObject"},
	}}
	report := &grpcReport{Results: []grpcResult{
		// GetObject: two probes, both pass -> verified.
		{Service: "storage", RPC: "GetObject", Status: "pass"},
		{Service: "storage", RPC: "GetObject (raw)", Method: "GetObject", Status: "pass"},
		// PutObject: one pass, one fail -> downgrade.
		{Service: "storage", RPC: "PutObject", Status: "pass"},
		{Service: "storage", RPC: "PutObject (chunked)", Method: "PutObject", Status: "fail"},
		// DeleteObject: no report entry -> unverified.
	}}

	facts := GRPCFacts(services, &Overrides{}, report)
	byOp := map[string]Facts{}
	for _, f := range facts {
		byOp[f.Operation] = f
	}

	verified := byOp["GetObject"]
	if verified.Override != nil {
		t.Errorf("GetObject: override = %+v, want nil (verified)", verified.Override)
	}
	if verified.GRPCChecksPassed != 2 || verified.GRPCChecksTotal != 2 {
		t.Errorf("GetObject: evidence = %d/%d, want 2/2", verified.GRPCChecksPassed, verified.GRPCChecksTotal)
	}
	if c := Classify(verified); c.State != StateGA {
		t.Errorf("GetObject: state = %q, want ga (reason %q)", c.State, c.Reason)
	}

	failed := byOp["PutObject"]
	if failed.Override == nil || failed.Override.State != StateLimited {
		t.Errorf("PutObject: override = %+v, want default limited", failed.Override)
	}
	if len(failed.Findings) != 1 || failed.Findings[0].Kind != "grpc_conformance" ||
		failed.Findings[0].Severity != "high" || failed.Findings[0].Allowlisted {
		t.Errorf("PutObject: findings = %+v, want one non-allowlisted high grpc_conformance", failed.Findings)
	}
	if c := Classify(failed); c.State != StateLimited {
		t.Errorf("PutObject: state = %q, want limited (reason %q)", c.State, c.Reason)
	}

	uncovered := byOp["DeleteObject"]
	if uncovered.Override == nil || uncovered.Override.State != StateLimited {
		t.Errorf("DeleteObject: override = %+v, want default limited", uncovered.Override)
	}
	if uncovered.GRPCChecksTotal != 0 {
		t.Errorf("DeleteObject: evidence total = %d, want 0", uncovered.GRPCChecksTotal)
	}
}

// TestReadGRPCReportAbsent verifies the report is optional: a missing file
// yields (nil, nil) so generation works without it.
func TestReadGRPCReportAbsent(t *testing.T) {
	rep, err := ReadGRPCReport("testdata/does-not-exist/report.json")
	if err != nil {
		t.Fatalf("ReadGRPCReport(absent) error = %v, want nil", err)
	}
	if rep != nil {
		t.Fatalf("ReadGRPCReport(absent) = %+v, want nil", rep)
	}
}

// TestGRPCOnlyServices checks the services reachable only over gRPC.
func TestGRPCOnlyServices(t *testing.T) {
	ops := conf.Enumerate()
	facts := GRPCFacts(conf.EnumerateGRPC(), &Overrides{}, nil)
	only := GRPCOnlyServices(ops, facts)

	got := map[string]bool{}
	for _, s := range only {
		got[s] = true
	}
	// After the Phase 1/2 REST additions, the long-running Operations service
	// and the Firestore Admin surface (REST under `firestore`, gRPC under the
	// distinct `firestoreadmin` wire service) remain gRPC-only.
	for _, want := range []string{"operations", "firestoreadmin"} {
		if !got[want] {
			t.Errorf("GRPCOnlyServices missing %q (got %v)", want, only)
		}
	}
	for _, notWant := range []string{"datastore", "logging", "monitoring", "workflowexecutions"} {
		if got[notWant] {
			t.Errorf("GRPCOnlyServices unexpectedly includes %q (got %v)", notWant, only)
		}
	}
	for i := 1; i < len(only); i++ {
		if only[i-1] >= only[i] {
			t.Errorf("GRPCOnlyServices not sorted/unique: %v", only)
			break
		}
	}
	t.Logf("gRPC-only services: %v", only)
}

// TestGRPCOnlyServicesDoesNotClaimRestServices guards against a service that
// has both transports being reported as gRPC-only.
func TestGRPCOnlyServicesDoesNotClaimRestServices(t *testing.T) {
	ops := conf.Enumerate()
	facts := GRPCFacts(conf.EnumerateGRPC(), &Overrides{}, nil)
	for _, s := range GRPCOnlyServices(ops, facts) {
		if s == "storage" || s == "pubsub" || s == "firestore" || s == "kms" || s == "secretmanager" || s == "iam" {
			t.Errorf("%q has REST operations but was reported gRPC-only", s)
		}
	}
}
