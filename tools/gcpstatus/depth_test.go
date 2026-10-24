package main

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// depthRows is the synthetic inventory the depth tests validate against.
func depthRows() []coverageRow {
	return []coverageRow{
		{Service: "storage", Canonical: "storage", GA: true, Covered: true, Suites: []string{"sdk", "sdk-gcs-grpc"}},
		{Service: "scheduler", Canonical: "scheduler", GA: true, Covered: true, Suites: []string{"sdk-scheduler"}},
		{Service: "compute", Canonical: "compute", GA: false, Covered: true, Suites: []string{"sdk-compute"}},
	}
}

func writeDepth(t *testing.T, name, body string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestDepthGateViolations(t *testing.T) {
	rows := depthRows()
	// no records: both ga services are unaccounted for.
	if got, want := depthGateViolations(rows, nil), []string{"storage (no depth record)", "scheduler (no depth record)"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("depthGateViolations(nil) = %v, want %v", got, want)
	}

	// storage fully recorded; scheduler missing p4.
	records := map[string]depthRecord{
		"storage": {Classes: map[string]depthClass{
			"p2": {State: "present"}, "p3": {State: "present"}, "p4": {State: "present"}, "p5": {State: "present"}}},
		"scheduler": {Classes: map[string]depthClass{
			"p2": {State: "exempt"}, "p3": {State: "exempt"}, "p5": {State: "exempt"}}},
	}
	if got, want := depthGateViolations(rows, records), []string{"scheduler p4"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("depthGateViolations = %v, want %v", got, want)
	}

	// Every ga cell present or exempt clears the gate; the non-ga service is not
	// required to have a record.
	records["scheduler"].Classes["p4"] = depthClass{State: "exempt"}
	if got := depthGateViolations(rows, records); len(got) != 0 {
		t.Fatalf("depthGateViolations = %v, want none", got)
	}
}

func TestLoadDepthDir(t *testing.T) {
	rows := depthRows()

	// Missing directory and empty path both yield no records without error.
	if got, err := loadDepthDir(filepath.Join(t.TempDir(), "nope"), rows); err != nil || len(got) != 0 {
		t.Fatalf("missing dir = %v, %v; want no records", got, err)
	}
	if got, err := loadDepthDir("", rows); err != nil || len(got) != 0 {
		t.Fatalf("empty path = %v, %v; want no records", got, err)
	}

	valid := `
service: storage
classes:
  p2: {state: present, suites: [sdk], note: "object preconditions + version generations"}
  p3: {state: present, suites: [sdk], note: "404 delete-of-missing"}
  p4: {state: exempt, reason: "no composed-paging suite yet", date: "2026-10-13", owner: DC3, review_by: "2099-01-01"}
  p5: {state: exempt, reason: "handled by the gRPC suite", date: "2026-10-13", owner: TD10, review_by: "2099-01-01"}
`
	got, err := loadDepthDir(writeDepth(t, "storage.yaml", valid), rows)
	if err != nil {
		t.Fatalf("valid record: %v", err)
	}
	if rec, ok := got["storage"]; !ok || len(rec.Classes) != 4 {
		t.Fatalf("loadDepthDir = %v, want storage with 4 classes", got)
	}

	// An unquoted date is accepted too (yaml.v3 does not coerce a plain
	// timestamp-looking scalar to time.Time for a string field).
	unquoted := "service: storage\nclasses:\n  p2: {state: exempt, reason: r, date: 2026-10-13, owner: DC3, review_by: 2099-01-01}\n"
	if _, err := loadDepthDir(writeDepth(t, "s.yaml", unquoted), rows); err != nil {
		t.Errorf("unquoted date: %v", err)
	}

	for _, tc := range []struct{ name, body string }{
		{"missing service", "classes:\n  p2: {state: present, suites: [sdk], note: n}\n"},
		{"unknown service", "service: rnu\nclasses:\n  p2: {state: exempt, reason: r, date: \"2026-10-13\", owner: X, review_by: \"2099-01-01\"}\n"},
		{"unknown class", "service: storage\nclasses:\n  p6: {state: exempt, reason: r, date: \"2026-10-13\", owner: X, review_by: \"2099-01-01\"}\n"},
		{"present without note", "service: storage\nclasses:\n  p2: {state: present, suites: [sdk]}\n"},
		{"present without suite", "service: storage\nclasses:\n  p2: {state: present, note: n}\n"},
		{"present unknown suite", "service: storage\nclasses:\n  p2: {state: present, suites: [sdk-nope], note: n}\n"},
		{"exempt without reason", "service: storage\nclasses:\n  p2: {state: exempt, date: \"2026-10-13\", owner: X, review_by: \"2099-01-01\"}\n"},
		{"exempt bad date", "service: storage\nclasses:\n  p2: {state: exempt, reason: r, date: \"yesterday\", owner: X, review_by: \"2099-01-01\"}\n"},
		{"exempt without owner", "service: storage\nclasses:\n  p2: {state: exempt, reason: r, date: \"2026-10-13\", review_by: \"2099-01-01\"}\n"},
		{"exempt without review_by", "service: storage\nclasses:\n  p2: {state: exempt, reason: r, date: \"2026-10-13\", owner: X}\n"},
		{"exempt bad review_by", "service: storage\nclasses:\n  p2: {state: exempt, reason: r, date: \"2026-10-13\", owner: X, review_by: \"soon\"}\n"},
		{"bad state", "service: storage\nclasses:\n  p2: {state: maybe, note: n, suites: [sdk]}\n"},
		{"malformed yaml", "service: storage\nclasses: [oops\n"},
	} {
		if _, err := loadDepthDir(writeDepth(t, "x.yaml", tc.body), rows); err == nil {
			t.Errorf("%s: want error, got nil", tc.name)
		}
	}

	// Two files recording the same service fail loud.
	dir := t.TempDir()
	for _, name := range []string{"a.yaml", "b.yaml"} {
		body := "service: storage\nclasses:\n  p2: {state: exempt, reason: r, date: \"2026-10-13\", owner: X, review_by: \"2099-01-01\"}\n"
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := loadDepthDir(dir, rows); err == nil {
		t.Error("duplicate service across files: want error, got nil")
	}
}

func TestDepthStaleExemptions(t *testing.T) {
	records := map[string]depthRecord{
		"storage": {Classes: map[string]depthClass{
			"p2": {State: "present"},
			"p3": {State: "exempt", ReviewBy: "2026-01-01"}, // overdue vs the probe date
			"p4": {State: "exempt", ReviewBy: "2026-10-16"}, // due today: not yet overdue
			"p5": {State: "exempt", ReviewBy: "2099-01-01"}, // future
		}},
		"scheduler": {Classes: map[string]depthClass{
			"p4": {State: "exempt", ReviewBy: "2020-06-30"}, // long overdue
		}},
	}
	// Probe date 2026-10-16: storage p3 + scheduler p4 are overdue (sorted by
	// service then class), storage p4/p5 are not.
	want := []string{"scheduler p4 (review_by 2020-06-30)", "storage p3 (review_by 2026-01-01)"}
	if got := depthStaleExemptions(records, "2026-10-16"); !reflect.DeepEqual(got, want) {
		t.Fatalf("depthStaleExemptions = %v, want %v", got, want)
	}
	// A present-only record contributes nothing.
	if got := depthStaleExemptions(map[string]depthRecord{"x": {Classes: map[string]depthClass{"p2": {State: "present"}}}}, "2099-01-01"); len(got) != 0 {
		t.Fatalf("depthStaleExemptions(present-only) = %v, want none", got)
	}
}

func TestDepthTotalsFor(t *testing.T) {
	rows := depthRows()
	records := map[string]depthRecord{
		"storage": {Classes: map[string]depthClass{"p2": {State: "present"}, "p3": {State: "exempt"}}},
		// A non-`ga` record is tolerated but must not inflate the `ga` counts.
		"compute": {Classes: map[string]depthClass{
			"p2": {State: "present"}, "p3": {State: "present"}, "p4": {State: "present"}, "p5": {State: "present"}}},
	}
	recorded, present, exempt := depthTotalsFor(rows, records)
	if recorded != 1 || present != 1 || exempt != 1 {
		t.Fatalf("depthTotalsFor = %d recorded, %d present, %d exempt; want 1, 1, 1", recorded, present, exempt)
	}
}
