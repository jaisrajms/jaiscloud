//go:build gcp_differential

package gcpdifferential

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// The SDK error/retry tour differential (demo/sdk-tour errors mode) records the
// canonical error responses the same operations produce against REAL GCP into
// testdata/golden-tour-errors, then replays them against the emulator offline
// and diffs the normalized error envelopes. It reuses the curated REST
// differential's target, normalizer, diff, triage and reporting plumbing; only
// the scenario list and the golden directory differ.

func errorGoldenDir() string {
	return envOr("GCP_DIFFERENTIAL_ERRORS_GOLDEN_DIR", "testdata/golden-tour-errors")
}
func errorReportDir() string {
	return envOr("GCP_DIFFERENTIAL_ERRORS_REPORT_DIR", "testdata/report-tour-errors")
}

// TestRecordErrors captures the REST error-tour goldens from real GCP. It is
// skipped unless -record (or GCP_DIFFERENTIAL_RECORD=1) is set, and requires
// ADC. It never prints the token and cleans up every created resource.
func TestRecordErrors(t *testing.T) {
	if !*recordFlag && os.Getenv("GCP_DIFFERENTIAL_RECORD") != "1" {
		t.Skip("set -record (or GCP_DIFFERENTIAL_RECORD=1) to capture error-tour goldens from real GCP")
	}
	project := envOr("GCP_DIFFERENTIAL_PROJECT", RealProjectDefault)
	projectNumber := envOr("GCP_DIFFERENTIAL_PROJECT_NUMBER", RealProjectNumberDefault)

	token, err := ADCToken()
	if err != nil {
		t.Fatalf("ADC: %v", err)
	}
	suffix := runSuffix()
	names := Names(suffix)
	target := RealTarget(project, projectNumber, suffix, names, token)
	scenarios := ErrorScenarios(project, suffix)

	defer func() {
		for _, line := range target.CleanupErrors() {
			t.Log(line)
		}
		for _, line := range target.VerifyErrorsAbsent() {
			t.Log(line)
		}
	}()

	exs, err := target.Run(scenarios)
	if err != nil {
		t.Fatalf("record error-tour ops against real GCP: %v", err)
	}

	dir := errorGoldenDir()
	if err := WriteGoldens(dir, exs); err != nil {
		t.Fatalf("write error-tour goldens: %v", err)
	}
	byService := map[string]int{}
	for _, ex := range exs {
		byService[ex.Service]++
	}
	t.Logf("recorded %d error-tour ops from real GCP project %q", len(exs), project)
	for _, svc := range sortedKeys(byService, nil) {
		t.Logf("  %-16s %d ops", svc, byService[svc])
	}
	files, _ := GoldenFiles(dir)
	t.Logf("wrote %d golden files under %s", len(files), dir)
}

// TestReplayErrors is the offline entry point for the error-tour differential:
// it replays the committed goldens against the local emulator, diffs, and
// writes report.{json,md}. Divergences are reported, not fatal, unless strict
// mode is on.
func TestReplayErrors(t *testing.T) {
	if *recordFlag {
		t.Skip("record mode: skipping error-tour replay")
	}
	goldensOrSkip(t, errorGoldenDir())
	goldens, err := ReadGoldens(errorGoldenDir())
	if err != nil {
		t.Fatalf("read error-tour goldens at %s: %v", errorGoldenDir(), err)
	}

	endpoint := strings.TrimRight(envOr("GCP_DIFFERENTIAL_ENDPOINT", "http://localhost:8080"), "/")
	emulatorProject := envOr("GCP_DIFFERENTIAL_EMULATOR_PROJECT", EmulatorProjectDefault)
	suffix := runSuffix()
	names := Names(suffix)
	target := EmulatorTarget(endpoint, emulatorProject, suffix, names)
	scenarios := ErrorScenarios(emulatorProject, suffix)

	matched, pending, orphans, duplicates := matchScenariosToGoldens(scenarios, goldens)
	if len(duplicates) > 0 {
		t.Fatalf("error-tour scenario/golden (Service, Op) collision: %v", duplicates)
	}
	if len(orphans) > 0 {
		t.Fatalf("orphan error-tour golden(s) with no scenario: %v (delete the stale golden file(s) or restore the scenario)", orphans)
	}
	for _, p := range pending {
		t.Logf("pending recording: %s", p)
	}

	runScenarios := make([]Scenario, 0, len(matched))
	for _, m := range matched {
		runScenarios = append(runScenarios, m.Scenario)
	}

	defer func() {
		for _, line := range target.CleanupErrors() {
			t.Log(line)
		}
	}()

	actual, err := target.Run(runScenarios)
	if err != nil {
		t.Fatalf("replay error-tour ops against emulator: %v", err)
	}
	actualByKey := make(map[string]Exchange, len(actual))
	for _, ex := range actual {
		actualByKey[scenarioKey(ex.Service, ex.Op)] = ex
	}

	var divs []Divergence
	for _, m := range matched {
		a, ok := actualByKey[scenarioKey(m.Scenario.Service, m.Scenario.Op)]
		if !ok {
			t.Errorf("replayed exchange missing for %s/%s", m.Scenario.Service, m.Scenario.Op)
			continue
		}
		divs = append(divs, DiffExchanges(m.Golden, a)...)
	}

	rep := BuildReport(target.Name, emulatorProject, len(actual), divs)
	if err := WriteReport(errorReportDir(), rep); err != nil {
		t.Errorf("write error-tour report: %v", err)
	}
	t.Logf("replayed %d error-tour ops against emulator (%d pending recording) -> %d divergences (%d open, %d accepted)",
		len(actual), len(pending), rep.Total, rep.OpenCount, rep.AcceptedCount)
	for _, s := range sortedKeys(rep.ByService, nil) {
		t.Logf("  %-16s %d open", s, rep.ByService[s])
	}
	t.Logf("report written to %s/report.{json,md}", errorReportDir())

	assertStrict(t, rep)
}

// TestErrorScenariosValid guards the error-tour list contract: every scenario
// is addressable, every Service maps to a real-GCP origin, and (Service, Op) is
// unique so goldens match by key rather than position.
func TestErrorScenariosValid(t *testing.T) {
	scenarios := ErrorScenarios("proj", "abc123")
	if len(scenarios) == 0 {
		t.Fatal("ErrorScenarios returned no scenarios")
	}
	seen := map[string]bool{}
	for _, sc := range scenarios {
		if sc.Service == "" || sc.Op == "" || sc.Method == "" || sc.Path == "" {
			t.Errorf("error-tour scenario %+v has an empty required field", sc)
		}
		if _, ok := serviceBaseURL[sc.Service]; !ok {
			t.Errorf("error-tour scenario %s/%s: service %q has no serviceBaseURL entry", sc.Service, sc.Op, sc.Service)
		}
		k := scenarioKey(sc.Service, sc.Op)
		if seen[k] {
			t.Errorf("duplicate error-tour (Service, Op): %s", displayKey(k))
		}
		seen[k] = true
	}
}

// TestErrorGoldensAreClean applies the shared credential/identifier guard.
func TestErrorGoldensAreClean(t *testing.T) { assertGoldensClean(t, errorGoldenDir()) }

// TestErrorGoldenManifest checks the error-tour manifest matches the files.
func TestErrorGoldenManifest(t *testing.T) {
	files := goldensOrSkip(t, errorGoldenDir())
	data, err := os.ReadFile(errorGoldenDir() + "/manifest.json")
	if err != nil {
		t.Fatalf("read error-tour manifest: %v", err)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("parse error-tour manifest: %v", err)
	}
	if m.Ops != len(files) {
		t.Errorf("manifest ops=%d but %d golden files present", m.Ops, len(files))
	}
}
