//go:build gcp_conformance

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testUniverse() *Universe {
	return &Universe{Fields: []Field{
		{Service: "svc", Transport: "rest", Resource: "Thing", Name: "name", Behavior: ""},
		{Service: "svc", Transport: "rest", Resource: "Thing", Name: "spec.mode", Behavior: "read_only"},
	}}
}

func testRows() []Row {
	return []Row{
		{Service: "svc", Transport: "rest", Resource: "Thing", Field: "name", Coverage: CoverageRoundtrip, Evidence: "svc/thing_test.go#TestName"},
		{Service: "svc", Transport: "rest", Resource: "Thing", Field: "spec.mode", Behavior: "read_only", Coverage: CoverageDerived, Reason: "computed from name"},
	}
}

// testRoot creates a repo-root fixture containing the evidence file every valid
// row points at.
func testRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "svc"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "svc", "thing_test.go"), []byte("package svc\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return root
}

func requireProblem(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("Check() = nil error, want a problem containing %q", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("Check() error = %q, want it to contain %q", err, want)
	}
}

func TestCheckPasses(t *testing.T) {
	rep, err := CheckIn(testRoot(t), testUniverse(), testRows(), Exemptions{})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if rep.Total != 2 || rep.Resources != 1 {
		t.Fatalf("report = %+v, want 2 fields / 1 resource", rep)
	}
	if rep.ByCoverage[CoverageRoundtrip] != 1 || rep.ByCoverage[CoverageDerived] != 1 {
		t.Fatalf("coverage counts = %v", rep.ByCoverage)
	}
}

func TestCheckMissingRow(t *testing.T) {
	_, err := CheckIn(testRoot(t), testUniverse(), testRows()[:1], Exemptions{})
	requireProblem(t, err, "missing row svc/rest/Thing/spec.mode")
}

func TestCheckStaleRow(t *testing.T) {
	rows := append(testRows(), Row{Service: "svc", Transport: "rest", Resource: "Thing", Field: "gone", Coverage: CoverageRoundtrip, Evidence: "svc/thing_test.go#TestName"})
	_, err := CheckIn(testRoot(t), testUniverse(), rows, Exemptions{})
	requireProblem(t, err, "stale row svc/rest/Thing/gone")
}

func TestCheckBehaviorDrift(t *testing.T) {
	rows := testRows()
	rows[1].Behavior = ""
	_, err := CheckIn(testRoot(t), testUniverse(), rows, Exemptions{})
	requireProblem(t, err, "behavior drift svc/rest/Thing/spec.mode")
}

func TestCheckRoundtripWithoutEvidence(t *testing.T) {
	rows := testRows()
	rows[0].Evidence = ""
	_, err := CheckIn(testRoot(t), testUniverse(), rows, Exemptions{})
	requireProblem(t, err, "roundtrip row svc/rest/Thing/name has no evidence pointer")
}

func TestCheckMissingEvidenceFile(t *testing.T) {
	rows := testRows()
	rows[0].Evidence = "does/not/exist_test.go#TestName"
	_, err := CheckIn(testRoot(t), testUniverse(), rows, Exemptions{})
	requireProblem(t, err, "does not exist")
}

func TestCheckEvidenceExemption(t *testing.T) {
	rows := testRows()
	rows[0].Evidence = ""
	if _, err := CheckIn(testRoot(t), testUniverse(), rows, Exemptions{"svc/rest/Thing/name": "identifier"}); err != nil {
		t.Fatalf("exempted roundtrip row failed: %v", err)
	}
	// The exemption is reported so the PR body can state the count.
	rep, err := CheckIn(testRoot(t), testUniverse(), rows, Exemptions{"svc/rest/Thing/name": "identifier"})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Exempt != 1 {
		t.Fatalf("Exempt = %d, want 1", rep.Exempt)
	}
}

func TestCheckStaleExemption(t *testing.T) {
	_, err := CheckIn(testRoot(t), testUniverse(), testRows(), Exemptions{"svc/rest/Thing/nope": "old field"})
	requireProblem(t, err, "stale exemption svc/rest/Thing/nope")
}

func TestCheckInvalidCoverage(t *testing.T) {
	rows := testRows()
	rows[0].Coverage = "mostly"
	_, err := CheckIn(testRoot(t), testUniverse(), rows, Exemptions{})
	requireProblem(t, err, `invalid coverage "mostly"`)
}

func TestCheckUnsupportedContract(t *testing.T) {
	rows := testRows()
	rows[1] = Row{Service: "svc", Transport: "rest", Resource: "Thing", Field: "spec.mode", Behavior: "read_only", Coverage: CoverageUnsupported}
	_, err := CheckIn(testRoot(t), testUniverse(), rows, Exemptions{})
	requireProblem(t, err, "unsupported row svc/rest/Thing/spec.mode has no reason")

	rows[1].Reason = "no source for the value"
	_, err = CheckIn(testRoot(t), testUniverse(), rows, Exemptions{})
	requireProblem(t, err, "has no review_by date")

	rows[1].ReviewBy = "2026-13-01"
	_, err = CheckIn(testRoot(t), testUniverse(), rows, Exemptions{})
	requireProblem(t, err, "invalid review_by")

	rows[1].ReviewBy = "2027-06-30"
	if _, err := CheckIn(testRoot(t), testUniverse(), rows, Exemptions{}); err != nil {
		t.Fatalf("valid unsupported row failed: %v", err)
	}
	// An exemption may not waive the reason/review_by contract.
	if _, err := CheckIn(testRoot(t), testUniverse(), rows, Exemptions{"svc/rest/Thing/spec.mode": "later"}); err != nil {
		t.Fatalf("exempted unsupported row with reason+date failed: %v", err)
	}
}

func TestCheckDerivedNeedsFormula(t *testing.T) {
	rows := testRows()
	rows[1].Reason = ""
	_, err := CheckIn(testRoot(t), testUniverse(), rows, Exemptions{})
	requireProblem(t, err, "derived row svc/rest/Thing/spec.mode has no formula")
}

func TestCheckOutputOnlyNeedsEvidenceOrReason(t *testing.T) {
	rows := testRows()
	rows[1] = Row{Service: "svc", Transport: "rest", Resource: "Thing", Field: "spec.mode", Behavior: "read_only", Coverage: CoverageOutputOnly}
	_, err := CheckIn(testRoot(t), testUniverse(), rows, Exemptions{})
	requireProblem(t, err, "neither an evidence pointer nor a reason")

	rows[1].Reason = "never populated: no source for the value"
	if _, err := CheckIn(testRoot(t), testUniverse(), rows, Exemptions{}); err != nil {
		t.Fatalf("output-only row with a reason failed: %v", err)
	}
}

func TestCheckDuplicateRow(t *testing.T) {
	rows := append(testRows(), testRows()[0])
	_, err := CheckIn(testRoot(t), testUniverse(), rows, Exemptions{})
	requireProblem(t, err, "duplicate row svc/rest/Thing/name")
}

func TestCheckUnclassifiedRow(t *testing.T) {
	rows := testRows()
	rows[1].Coverage = ""
	_, err := CheckIn(testRoot(t), testUniverse(), rows, Exemptions{})
	requireProblem(t, err, "unclassified row svc/rest/Thing/spec.mode")
}

func TestGeneratePreservesClassificationsAndScaffoldsNewFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "field-coverage.csv")
	seed := []Row{
		{Service: "svc", Transport: "rest", Resource: "Thing", Field: "name", Behavior: "old_behavior", Coverage: CoverageRoundtrip, Evidence: "svc/thing_test.go#TestName"},
		{Service: "svc", Transport: "rest", Resource: "Thing", Field: "gone", Coverage: CoverageUnsupported, Reason: "removed field", ReviewBy: "2027-06-30"},
	}
	if err := writeCSV(path, seed); err != nil {
		t.Fatal(err)
	}
	universe := &Universe{Fields: []Field{
		{Service: "svc", Transport: "rest", Resource: "Thing", Name: "name", Behavior: ""},
		{Service: "svc", Transport: "rest", Resource: "Thing", Name: "added", Behavior: "read_only"},
	}}
	if err := Generate(path, universe); err != nil {
		t.Fatal(err)
	}
	rows, err := ReadCSV(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows = %d, want 3 (2 defined + 1 stale preserved)", len(rows))
	}
	byKey := map[string]Row{}
	for _, r := range rows {
		byKey[r.Key()] = r
	}
	if got := byKey["svc/rest/Thing/name"]; got.Coverage != CoverageRoundtrip || got.Evidence == "" {
		t.Fatalf("existing classification lost: %+v", got)
	}
	if got := byKey["svc/rest/Thing/name"]; got.Behavior != "" {
		t.Fatalf("definition behavior not refreshed: %q", got.Behavior)
	}
	if got := byKey["svc/rest/Thing/added"]; got.Coverage != "" || got.Behavior != "read_only" {
		t.Fatalf("new field scaffold = %+v, want empty coverage + read_only behavior", got)
	}
	if _, ok := byKey["svc/rest/Thing/gone"]; !ok {
		t.Fatal("stale row was dropped instead of preserved for the check to report")
	}
}
