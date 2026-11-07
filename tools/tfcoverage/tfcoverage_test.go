package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	providerschema "jaiscloud/tools/providerschema"
)

func schemaWith(types ...string) *providerschema.ProviderSchema {
	resources := map[string][]providerschema.Attribute{}
	for _, t := range types {
		resources[t] = nil
	}
	return &providerschema.ProviderSchema{Resources: resources}
}

func baseScope() *Scope {
	return &Scope{
		Provider:           "hashicorp/google",
		DefaultReviewAfter: "2027-06-30",
		Services: map[string]ServiceScope{
			"storage":         {MatrixState: "ga"},
			"bigquery":        {MatrixState: "ga"},
			"cloudsql":        {MatrixState: "limited"},
			"resourcemanager": {MatrixState: "ga"},
		},
	}
}

func TestComputeFirstMatchWinsAndOutOfScopePrecedesRules(t *testing.T) {
	scope := baseScope()
	scope.Rules = []Rule{
		{Match: "google_storage_bucket", Service: "storage", State: StateServed},
		{Match: "google_storage_*", Service: "storage", State: StateUnsupported, Reason: "storage-adjacent surface"},
		{Match: "google_resource_manager_*", Service: "resourcemanager", State: StateUnsupported, Reason: "liens"},
	}
	scope.OutOfScope = []OutOfScopeRule{
		{Match: "google_storage_transfer_*", Reason: "Storage Transfer Service is not served"},
	}
	inv, err := Compute(schemaWith(
		"google_storage_bucket",
		"google_storage_transfer_job",
		"google_storage_insights_report_config",
		"google_resource_manager_lien",
	), scope, Exercise{})
	if err != nil {
		t.Fatalf("Compute: %v", err)
	}
	if len(inv.Rows) != 3 {
		t.Fatalf("rows = %d, want 3 (transfer is out of scope): %+v", len(inv.Rows), inv.Rows)
	}
	if inv.OutOfScope != 1 {
		t.Fatalf("out of scope = %d, want 1", inv.OutOfScope)
	}
	byName := map[string]Row{}
	for _, r := range inv.Rows {
		byName[r.ProviderResource] = r
	}
	if got := byName["google_storage_bucket"].EmulatorState; got != StateServed {
		t.Errorf("bucket state = %q, want served (exact rule before the glob)", got)
	}
	if got := byName["google_storage_insights_report_config"].EmulatorState; got != StateUnsupported {
		t.Errorf("insights state = %q, want unsupported", got)
	}
	if got := byName["google_storage_transfer_job"]; got.ProviderResource != "" {
		t.Errorf("transfer job must be out of scope, got row %+v", got)
	}
}

func TestComputeClassificationAndReasons(t *testing.T) {
	scope := baseScope()
	scope.Rules = []Rule{
		{Match: "google_storage_bucket", Service: "storage", State: StateServed},
		{Match: "google_bigquery_dataset", Service: "bigquery", State: StateServed},
		{Match: "google_bigquery_routine", Service: "bigquery", State: StateServed},
		{Match: "google_sql_database", Service: "cloudsql", State: StateLimited},
	}
	exercise := Exercise{
		"google_storage_bucket":   {"fixture:compat"},
		"google_bigquery_dataset": {"module:bq"},
		"google_bigquery_routine": {"plan-only:bq-plan"},
	}
	inv, err := Compute(schemaWith(
		"google_storage_bucket", "google_bigquery_dataset", "google_bigquery_routine", "google_sql_database",
	), scope, exercise)
	if err != nil {
		t.Fatalf("Compute: %v", err)
	}
	byName := map[string]Row{}
	for _, r := range inv.Rows {
		byName[r.ProviderResource] = r
	}
	if got := byName["google_bigquery_routine"].Classification; got != ClassPlanOnly {
		t.Errorf("plan-only case classification = %q, want plan-only", got)
	}
	if got := byName["google_bigquery_dataset"].Classification; got != ClassServed {
		t.Errorf("applied case classification = %q, want served", got)
	}
	// An unexercised served resource gets a synthesized, non-empty reason.
	if row := byName["google_sql_database"]; row.Reason == "" || row.ReviewAfter != scope.DefaultReviewAfter {
		t.Errorf("unexercised row = %+v, want synthesized reason + default review date", row)
	}
}

func TestValidateRejectsUnmatchedAndStaleRules(t *testing.T) {
	scope := baseScope()
	scope.Rules = []Rule{
		{Match: "google_storage_bucket", Service: "storage", State: StateServed},
		{Match: "google_storage_gone", Service: "storage", State: StateServed},
	}
	inv, err := Compute(schemaWith("google_storage_bucket", "google_unclassified_thing"), scope, Exercise{})
	if err != nil {
		t.Fatalf("Compute: %v", err)
	}
	err = inv.Validate(scope, time.Date(2026, 11, 7, 0, 0, 0, 0, time.UTC), map[string]string{
		"storage": "ga", "bigquery": "ga", "cloudsql": "limited", "resourcemanager": "ga",
	})
	if err == nil {
		t.Fatal("Validate = nil, want unmatched-type and stale-rule errors")
	}
	for _, want := range []string{"google_unclassified_thing", "google_storage_gone"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

func TestValidateRejectsExpiredReviewAndServicePromotion(t *testing.T) {
	scope := baseScope()
	scope.DefaultReviewAfter = "2020-01-01"
	scope.Rules = []Rule{
		{Match: "google_storage_bucket", Service: "storage", State: StateServed},
	}
	inv, err := Compute(schemaWith("google_storage_bucket"), scope, Exercise{})
	if err != nil {
		t.Fatalf("Compute: %v", err)
	}
	// The service was recorded as limited but the matrix now grades it ga.
	err = inv.Validate(scope, time.Date(2026, 11, 7, 0, 0, 0, 0, time.UTC), map[string]string{
		"storage": "ga", "bigquery": "ga", "cloudsql": "ga", "resourcemanager": "ga",
	})
	if err == nil {
		t.Fatal("Validate = nil, want expired review + promotion errors")
	}
	for _, want := range []string{"2020-01-01", "cloudsql", "now ga"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}

func TestValidateAllowsFutureReviewForUnexercisedGARow(t *testing.T) {
	scope := baseScope()
	scope.Rules = []Rule{
		{Match: "google_storage_bucket", Service: "storage", State: StateServed},
	}
	inv, err := Compute(schemaWith("google_storage_bucket"), scope, Exercise{})
	if err != nil {
		t.Fatalf("Compute: %v", err)
	}
	matrix := map[string]string{"storage": "ga", "bigquery": "ga", "cloudsql": "limited", "resourcemanager": "ga"}
	if err := inv.Validate(scope, time.Date(2026, 11, 7, 0, 0, 0, 0, time.UTC), matrix); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestCSVRoundTrip(t *testing.T) {
	inv := &Inventory{Rows: []Row{
		{ProviderResource: "google_storage_bucket", Service: "storage", EmulatorState: "served",
			ExercisedBy: "fixture:compat,module:storage", Classification: "served",
			Reason: "a, b, c", ReviewAfter: "2027-06-30"},
	}}
	var buf bytes.Buffer
	if err := inv.WriteCSV(&buf); err != nil {
		t.Fatalf("WriteCSV: %v", err)
	}
	rows, err := ReadCSV(&buf)
	if err != nil {
		t.Fatalf("ReadCSV: %v", err)
	}
	if len(rows) != 1 || rows[0] != inv.Rows[0] {
		t.Fatalf("round trip = %+v, want %+v", rows, inv.Rows)
	}
}

func TestDiffRowsReportsMissingStaleAndDrift(t *testing.T) {
	want := []Row{
		{ProviderResource: "a", Service: "storage", EmulatorState: "served", ExercisedBy: "none", Classification: "served", Reason: "r", ReviewAfter: "2027-06-30"},
		{ProviderResource: "b", Service: "storage", EmulatorState: "served", ExercisedBy: "none", Classification: "served", Reason: "r", ReviewAfter: "2027-06-30"},
	}
	got := []Row{
		{ProviderResource: "a", Service: "storage", EmulatorState: "unsupported", ExercisedBy: "none", Classification: "unsupported", Reason: "r", ReviewAfter: "2027-06-30"},
		{ProviderResource: "stale", Service: "storage", EmulatorState: "served", ExercisedBy: "none", Classification: "served", Reason: "r", ReviewAfter: "2027-06-30"},
	}
	diffs := DiffRows(got, want)
	joined := strings.Join(diffs, "\n")
	for _, wantSub := range []string{"a: emulator_state", "missing row", "stale"} {
		if !strings.Contains(joined, wantSub) {
			t.Errorf("diffs %q do not mention %q", joined, wantSub)
		}
	}
}

func TestParseExerciseReadsFixtureCasesAndPlanOnly(t *testing.T) {
	dir := t.TempDir()
	fixture := filepath.Join(dir, "main.tf")
	if err := os.WriteFile(fixture, []byte(`
resource "google_storage_bucket" "compat" {
  name = "b"
}
resource "google_storage_bucket" "logs" {
  name = "b-logs"
}
# resource "google_storage_bucket" "commented" {}
`), 0o644); err != nil {
		t.Fatal(err)
	}
	cases := filepath.Join(dir, "module-cases")
	if err := os.MkdirAll(cases, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cases, "storage.sh"), []byte(`
cat > "$WORK/main.tf" <<EOF
resource "google_storage_bucket" "module" {}
resource "google_kms_key_ring" "kr" {}
EOF
`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cases, "gke.sh"), []byte(`
PLAN_ONLY=1
resource "google_container_cluster" "gke" {}
`), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := ParseExercise(Sources{Fixture: fixture, CasesDir: cases})
	if err != nil {
		t.Fatalf("ParseExercise: %v", err)
	}
	if by := got.ExercisedBy("google_storage_bucket"); by != "fixture:compat,fixture:logs,module:storage" {
		t.Errorf("storage bucket exercised_by = %q", by)
	}
	if by := got.ExercisedBy("google_kms_key_ring"); by != "module:storage" {
		t.Errorf("kms key ring exercised_by = %q", by)
	}
	if by := got.ExercisedBy("google_container_cluster"); by != "plan-only:gke" {
		t.Errorf("plan-only exercised_by = %q", by)
	}
	if by := got.ExercisedBy("google_unknown"); by != "none" {
		t.Errorf("unknown exercised_by = %q, want none", by)
	}
}

func TestLoadScopeYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "scope.yaml")
	if err := os.WriteFile(path, []byte(`
provider: hashicorp/google
default_review_after: "2027-06-30"
services:
  storage: { matrix_state: ga }
rules:
  - { match: google_storage_bucket, service: storage, state: served }
out_of_scope:
  - { match: google_app_engine_*, reason: "not served" }
`), 0o644); err != nil {
		t.Fatal(err)
	}
	scope, err := LoadScope(path)
	if err != nil {
		t.Fatalf("LoadScope: %v", err)
	}
	if _, ok := scope.MatchRule("google_storage_bucket"); !ok {
		t.Error("rule did not load")
	}
	if _, ok := scope.MatchOutOfScope("google_app_engine_application"); !ok {
		t.Error("out-of-scope rule did not load")
	}
}

// TestCommittedInventoryIsCurrent makes the unit test the same check the
// make target runs: the committed CSV must match a fresh computation from the
// committed snapshot, scope, fixture and module cases.
func TestCommittedInventoryIsCurrent(t *testing.T) {
	root := filepath.Join("..", "..")
	schema, err := providerschema.Load(filepath.Join(root, "docs/fidelity/terraform-provider-schema-full.json"))
	if err != nil {
		t.Fatalf("load committed snapshot: %v", err)
	}
	scope, err := LoadScope(filepath.Join(root, "tools/tfcoverage/scope.yaml"))
	if err != nil {
		t.Fatalf("load scope: %v", err)
	}
	exercise, err := ParseExercise(Sources{
		Fixture:  filepath.Join(root, "tests/integration/gcp/terraform/main.tf"),
		CasesDir: filepath.Join(root, "tests/integration/gcp/terraform/module-cases"),
	})
	if err != nil {
		t.Fatalf("parse exercise: %v", err)
	}
	matrix, err := LoadMatrix(filepath.Join(root, "docs/fidelity/fidelity-matrix.json"))
	if err != nil {
		t.Fatalf("load matrix: %v", err)
	}
	inv, err := Compute(schema, scope, exercise)
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	if err := inv.Validate(scope, time.Now().UTC().Truncate(24*time.Hour), matrix); err != nil {
		t.Fatalf("validate: %v", err)
	}
	f, err := os.Open(filepath.Join(root, "docs/fidelity/terraform-coverage.csv"))
	if err != nil {
		t.Fatalf("open committed inventory: %v", err)
	}
	defer f.Close()
	committed, err := ReadCSV(f)
	if err != nil {
		t.Fatalf("read committed inventory: %v", err)
	}
	if diffs := DiffRows(committed, inv.Rows); len(diffs) > 0 {
		t.Fatalf("committed inventory is stale (%d differences); run make gen-gcp-terraform-coverage:\n  %s",
			len(diffs), strings.Join(limit(diffs, 20), "\n  "))
	}
}
