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
	if err := os.WriteFile(filepath.Join(root, "svc", "thing_test.go"), []byte("package svc\n\nfunc TestName(t *testing.T) {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return root
}

func requireProblem(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("CheckIn() = nil error, want a problem containing %q", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("CheckIn() error = %q, want it to contain %q", err, want)
	}
}

func TestCheckPasses(t *testing.T) {
	rep, err := CheckIn(testRoot(t), testUniverse(), testRows(), Exemptions{})
	if err != nil {
		t.Fatalf("CheckIn: %v", err)
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
	// The exemption is reported so the PR body can state the count.
	rep, err := CheckIn(testRoot(t), testUniverse(), rows, Exemptions{"svc/rest/Thing/name": "identifier"})
	if err != nil {
		t.Fatalf("exempted roundtrip row failed: %v", err)
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
	// An exemption cannot waive a reason/review_by contract: it is misapplied
	// on an unsupported row.
	_, err = CheckIn(testRoot(t), testUniverse(), rows, Exemptions{"svc/rest/Thing/spec.mode": "later"})
	requireProblem(t, err, "misapplied exemption svc/rest/Thing/spec.mode")
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
		{Service: "svc", Transport: "rest", Resource: "Thing", Field: "name", Behavior: "", Coverage: CoverageRoundtrip, Evidence: "svc/thing_test.go#TestName"},
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

func TestCheckEvidenceSymbolMustExist(t *testing.T) {
	rows := testRows()
	rows[0].Evidence = "svc/thing_test.go#TestDoesNotExist"
	_, err := CheckIn(testRoot(t), testUniverse(), rows, Exemptions{})
	requireProblem(t, err, "names no such test")
}

func TestCheckEvidenceRequiresSymbol(t *testing.T) {
	rows := testRows()
	rows[0].Evidence = "svc/thing_test.go"
	_, err := CheckIn(testRoot(t), testUniverse(), rows, Exemptions{})
	requireProblem(t, err, "names no test function")
}

func TestCheckEvidenceEveryPointerResolves(t *testing.T) {
	rows := testRows()
	rows[0].Evidence = "svc/thing_test.go#TestName; svc/other_test.go#TestOther"
	_, err := CheckIn(testRoot(t), testUniverse(), rows, Exemptions{})
	requireProblem(t, err, `evidence file "svc/other_test.go" does not exist`)
}

func TestCheckOverdueReview(t *testing.T) {
	rows := testRows()
	rows[1] = Row{Service: "svc", Transport: "rest", Resource: "Thing", Field: "spec.mode", Behavior: "read_only", Coverage: CoverageUnsupported, Reason: "no source", ReviewBy: "2020-01-01"}
	_, err := CheckIn(testRoot(t), testUniverse(), rows, Exemptions{})
	requireProblem(t, err, "overdue for review")
}

func TestCheckUnusedExemption(t *testing.T) {
	_, err := CheckIn(testRoot(t), testUniverse(), testRows(), Exemptions{"svc/rest/Thing/name": "identifier"})
	requireProblem(t, err, "unused exemption svc/rest/Thing/name")
}

func TestCheckRoundtripContradictsDefinitionBehavior(t *testing.T) {
	rows := testRows()
	rows[0].Behavior = "output_only"
	_, err := CheckIn(testRoot(t), testUniverse(), rows, Exemptions{})
	requireProblem(t, err, "contradicts its definition behavior")

	// With a written reason the deviation is a recorded, reviewed exception.
	rows[0].Reason = "the emulator echoes a client value the API declares OUTPUT_ONLY (FC2-16)"
	universe := testUniverse()
	universe.Fields[0].Behavior = "output_only"
	if _, err := CheckIn(testRoot(t), universe, rows, Exemptions{}); err != nil {
		t.Fatalf("documented deviation failed: %v", err)
	}
}

func TestGenerateReclassifiesOnBehaviorChange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "field-coverage.csv")
	if err := writeCSV(path, []Row{
		{Service: "svc", Transport: "rest", Resource: "Thing", Field: "name", Behavior: "", Coverage: CoverageRoundtrip, Evidence: "svc/thing_test.go#TestName"},
	}); err != nil {
		t.Fatal(err)
	}
	universe := &Universe{Fields: []Field{
		{Service: "svc", Transport: "rest", Resource: "Thing", Name: "name", Behavior: "output_only"},
	}}
	if err := Generate(path, universe); err != nil {
		t.Fatal(err)
	}
	rows, err := ReadCSV(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	if rows[0].Behavior != "output_only" {
		t.Fatalf("behavior = %q, want output_only", rows[0].Behavior)
	}
	if rows[0].Coverage != "" {
		t.Fatalf("classification survived a definition behavior change: %+v", rows[0])
	}
}

func TestReadCSVRejectsBadHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.csv")
	if err := os.WriteFile(path, []byte("service,transport\nsvc,rest\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadCSV(path); err == nil {
		t.Fatal("ReadCSV(bad header) = nil error")
	}
}

func TestGenerateCreatesMissingCSV(t *testing.T) {
	path := filepath.Join(t.TempDir(), "new", "field-coverage.csv")
	if err := Generate(path, testUniverse()); err != nil {
		t.Fatal(err)
	}
	rows, err := ReadCSV(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}
	for _, r := range rows {
		if r.Coverage != "" {
			t.Fatalf("scaffolded row already classified: %+v", r)
		}
	}
}

func TestLoadExemptions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "exemptions.json")
	if err := os.WriteFile(path, []byte(`{"exemptions":[{"field":"svc/rest/Thing/name","reason":"identifier"}]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	ex, err := LoadExemptions(path)
	if err != nil {
		t.Fatalf("LoadExemptions: %v", err)
	}
	if ex["svc/rest/Thing/name"] != "identifier" {
		t.Fatalf("exemptions = %v", ex)
	}

	// A missing file means no exemptions.
	missing, err := LoadExemptions(filepath.Join(dir, "absent.json"))
	if err != nil || len(missing) != 0 {
		t.Fatalf("LoadExemptions(missing) = %v, %v", missing, err)
	}

	for name, body := range map[string]string{
		"missing reason": `{"exemptions":[{"field":"svc/rest/Thing/name"}]}`,
		"missing field":  `{"exemptions":[{"reason":"identifier"}]}`,
		"duplicate":      `{"exemptions":[{"field":"a/b/c/d","reason":"x"},{"field":"a/b/c/d","reason":"y"}]}`,
		"not json":       `{`,
	} {
		t.Run(name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "exemptions.json")
			if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadExemptions(p); err == nil {
				t.Fatalf("LoadExemptions(%s) = nil error", name)
			}
		})
	}
}
