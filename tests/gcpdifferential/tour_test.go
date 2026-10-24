//go:build gcp_differential

package gcpdifferential

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// The SDK-tour differential (demo/sdk-tour) records the exact operations the
// official Go client tour issues against REAL GCP into testdata/golden-tour,
// then replays them against the emulator offline and diffs the normalized
// responses. It reuses the curated REST differential's target, normalizer,
// diff, triage and reporting plumbing; only the scenario list and the golden
// directory differ.

func tourGoldenDir() string {
	return envOr("GCP_DIFFERENTIAL_TOUR_GOLDEN_DIR", "testdata/golden-tour")
}
func tourReportDir() string {
	return envOr("GCP_DIFFERENTIAL_TOUR_REPORT_DIR", "testdata/report-tour")
}

// TestRecordTour captures the REST SDK-tour goldens from real GCP. It is skipped
// unless -record (or GCP_DIFFERENTIAL_RECORD=1) is set, and requires ADC. It
// never prints the token and cleans up every created resource (KMS excepted:
// fixed reusable keys).
func TestRecordTour(t *testing.T) {
	if !*recordFlag && os.Getenv("GCP_DIFFERENTIAL_RECORD") != "1" {
		t.Skip("set -record (or GCP_DIFFERENTIAL_RECORD=1) to capture SDK-tour goldens from real GCP")
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
	scenarios := TourScenarios(project, suffix)

	// The tour's KMS resources are non-deletable, so they are fixed reusable
	// names created (uncaptured) up front, like the curated differential's keys.
	if err := target.EnsureKMS(); err != nil {
		t.Fatalf("ensure KMS resources: %v", err)
	}
	if err := target.EnsureTourKMS(); err != nil {
		t.Fatalf("ensure tour KMS resources: %v", err)
	}

	defer func() {
		for _, line := range target.CleanupTour() {
			t.Log(line)
		}
		for _, line := range target.VerifyTourAbsent() {
			t.Log(line)
		}
	}()

	exs, err := target.Run(scenarios)
	if err != nil {
		t.Fatalf("record SDK-tour ops against real GCP: %v", err)
	}

	dir := tourGoldenDir()
	if err := WriteGoldens(dir, exs); err != nil {
		t.Fatalf("write SDK-tour goldens: %v", err)
	}
	byService := map[string]int{}
	for _, ex := range exs {
		byService[ex.Service]++
	}
	t.Logf("recorded %d SDK-tour ops from real GCP project %q", len(exs), project)
	for _, svc := range sortedKeys(byService, nil) {
		t.Logf("  %-16s %d ops", svc, byService[svc])
	}
	files, _ := GoldenFiles(dir)
	t.Logf("wrote %d golden files under %s", len(files), dir)
}

// TestReplayTour is the offline CI entry point for the REST SDK-tour
// differential: it replays the committed goldens against the local emulator,
// diffs, and writes report.{json,md}. Divergences are reported, not fatal,
// unless strict mode is on.
func TestReplayTour(t *testing.T) {
	if *recordFlag {
		t.Skip("record mode: skipping SDK-tour replay")
	}
	goldensOrSkip(t, tourGoldenDir())
	goldens, err := ReadGoldens(tourGoldenDir())
	if err != nil {
		t.Fatalf("read SDK-tour goldens at %s: %v", tourGoldenDir(), err)
	}

	endpoint := strings.TrimRight(envOr("GCP_DIFFERENTIAL_ENDPOINT", "http://localhost:8080"), "/")
	emulatorProject := envOr("GCP_DIFFERENTIAL_EMULATOR_PROJECT", EmulatorProjectDefault)
	suffix := runSuffix()
	names := Names(suffix)
	target := EmulatorTarget(endpoint, emulatorProject, suffix, names)
	scenarios := TourScenarios(emulatorProject, suffix)

	matched, pending, orphans, duplicates := matchScenariosToGoldens(scenarios, goldens)
	if len(duplicates) > 0 {
		t.Fatalf("SDK-tour scenario/golden (Service, Op) collision: %v", duplicates)
	}
	if len(orphans) > 0 {
		t.Fatalf("orphan SDK-tour golden(s) with no scenario: %v (delete the stale golden file(s) or restore the scenario)", orphans)
	}
	for _, p := range pending {
		t.Logf("pending recording: %s", p)
	}

	if err := target.EnsureTourKMS(); err != nil {
		t.Fatalf("ensure tour KMS on emulator (is it running at %s?): %v", endpoint, err)
	}

	runScenarios := make([]Scenario, 0, len(matched))
	for _, m := range matched {
		runScenarios = append(runScenarios, m.Scenario)
	}

	// Best-effort cleanup so a replay against a long-lived emulator leaves
	// nothing behind even if it aborts partway.
	defer func() {
		for _, line := range target.CleanupTour() {
			t.Log(line)
		}
	}()

	actual, err := target.Run(runScenarios)
	if err != nil {
		t.Fatalf("replay SDK-tour ops against emulator: %v", err)
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
	if err := WriteReport(tourReportDir(), rep); err != nil {
		t.Errorf("write SDK-tour report: %v", err)
	}
	t.Logf("replayed %d SDK-tour ops against emulator (%d pending recording) -> %d divergences (%d open, %d accepted)",
		len(actual), len(pending), rep.Total, rep.OpenCount, rep.AcceptedCount)
	for _, s := range sortedKeys(rep.ByService, nil) {
		t.Logf("  %-16s %d open", s, rep.ByService[s])
	}
	t.Logf("report written to %s/report.{json,md}", tourReportDir())

	assertStrict(t, rep)
}

// TestTourScenariosValid guards the REST SDK-tour list contract: every scenario
// is addressable, every Service maps to a real-GCP origin, and (Service, Op) is
// unique so goldens match by key rather than position.
func TestTourScenariosValid(t *testing.T) {
	scenarios := TourScenarios("proj", "abc123")
	if len(scenarios) == 0 {
		t.Fatal("TourScenarios returned no scenarios")
	}
	seen := map[string]bool{}
	for _, sc := range scenarios {
		if sc.Service == "" || sc.Op == "" || sc.Method == "" || sc.Path == "" {
			t.Errorf("SDK-tour scenario %+v has an empty required field", sc)
		}
		if _, ok := serviceBaseURL[sc.Service]; !ok {
			t.Errorf("SDK-tour scenario %s/%s: service %q has no serviceBaseURL entry", sc.Service, sc.Op, sc.Service)
		}
		k := scenarioKey(sc.Service, sc.Op)
		if seen[k] {
			t.Errorf("duplicate SDK-tour (Service, Op): %s", displayKey(k))
		}
		seen[k] = true
	}
}

// TestTourGoldensAreClean applies the shared credential/identifier guard to the
// SDK-tour goldens.
func TestTourGoldensAreClean(t *testing.T) { assertGoldensClean(t, tourGoldenDir()) }

// TestTourGoldenManifest checks the SDK-tour manifest matches the golden files.
func TestTourGoldenManifest(t *testing.T) {
	files := goldensOrSkip(t, tourGoldenDir())
	data, err := os.ReadFile(tourGoldenDir() + "/manifest.json")
	if err != nil {
		t.Fatalf("read SDK-tour manifest: %v", err)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("parse SDK-tour manifest: %v", err)
	}
	if m.Ops != len(files) {
		t.Errorf("manifest ops=%d but %d golden files present", m.Ops, len(files))
	}
}
