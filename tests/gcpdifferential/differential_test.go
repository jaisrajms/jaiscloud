//go:build gcp_differential

package gcpdifferential

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Flags are parsed by the test binary: `go test -tags gcp_differential ./... -record`.
var (
	recordFlag = flag.Bool("record", false, "record goldens from REAL GCP (requires ADC)")
	strictFlag = flag.Bool("strict", false, "fail the replay when any OPEN (real-bug) divergence is at/above the configured severity (env GCP_DIFFERENTIAL_STRICT=1; severity via GCP_DIFFERENTIAL_STRICT_SEVERITY, default high)")
)

const (
	defaultGoldenDir = "testdata/golden"
	defaultReportDir = "testdata/report"
)

func goldenDir() string { return envOr("GCP_DIFFERENTIAL_GOLDEN_DIR", defaultGoldenDir) }
func reportDir() string { return envOr("GCP_DIFFERENTIAL_REPORT_DIR", defaultReportDir) }

// goldensOrSkip returns the golden files in dir, or skips the test when none are
// present. When GCP_DIFFERENTIAL_REQUIRE_GOLDENS=1 (set by CI) a missing or empty
// golden set is a hard failure instead, so an untracked or dropped golden
// directory cannot make the offline gate pass vacuously.
func goldensOrSkip(t *testing.T, dir string) []string {
	t.Helper()
	files, err := GoldenFiles(dir)
	if err == nil && len(files) > 0 {
		return files
	}
	if os.Getenv("GCP_DIFFERENTIAL_REQUIRE_GOLDENS") == "1" {
		t.Fatalf("goldens required at %s but none found (err=%v)", dir, err)
	}
	t.Skipf("no goldens at %s (err=%v)", dir, err)
	return nil
}

// TestRecord captures goldens from real GCP. It is skipped unless -record (or
// GCP_DIFFERENTIAL_RECORD=1) is set, and requires ADC. It never prints the
// token, and it cleans up every created resource (KMS excepted: GCP cannot
// delete keyrings/keys, so fixed reusable names are used).
func TestRecord(t *testing.T) {
	if !*recordFlag && os.Getenv("GCP_DIFFERENTIAL_RECORD") != "1" {
		t.Skip("set -record (or GCP_DIFFERENTIAL_RECORD=1) to capture goldens from real GCP")
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
	scenarios := Scenarios(project, suffix)

	if err := target.EnsureKMS(); err != nil {
		t.Fatalf("ensure KMS resources: %v", err)
	}
	// Real GCP needs the Pub/Sub service agent to hold publisher/subscriber for
	// dead-letter forwarding; grant it before the DLQ scenario records.
	if err := target.EnsureDLQForwarding(); err != nil {
		t.Fatalf("ensure DLQ forwarding roles: %v", err)
	}

	// Cleanup always runs, even if the capture aborts partway.
	defer func() {
		for _, line := range target.Cleanup() {
			t.Log(line)
		}
		for _, line := range target.VerifyAbsent() {
			t.Log(line)
		}
	}()

	exs, err := target.Run(scenarios)
	if err != nil {
		t.Fatalf("record against real GCP: %v", err)
	}

	dir := goldenDir()
	if err := WriteGoldens(dir, exs); err != nil {
		t.Fatalf("write goldens: %v", err)
	}

	byService := map[string]int{}
	for _, ex := range exs {
		byService[ex.Service]++
	}
	t.Logf("recorded %d ops from real GCP project %q", len(exs), project)
	for _, svc := range sortedKeys(byService, nil) {
		t.Logf("  %-16s %d ops", svc, byService[svc])
	}
	files, _ := GoldenFiles(dir)
	t.Logf("wrote %d golden files under %s", len(files), dir)
}

// TestReplay is the offline CI entry point: it replays the committed goldens
// against the local emulator, diffs, and writes report.{json,md}. Divergences
// are reported, not fatal, unless -strict is passed.
func TestReplay(t *testing.T) {
	if *recordFlag {
		t.Skip("record mode: skipping replay")
	}
	goldensOrSkip(t, goldenDir())
	exs, err := ReadGoldens(goldenDir())
	if err != nil {
		t.Fatalf("read goldens at %s: %v", goldenDir(), err)
	}

	endpoint := strings.TrimRight(envOr("GCP_DIFFERENTIAL_ENDPOINT", "http://localhost:8080"), "/")
	emulatorProject := envOr("GCP_DIFFERENTIAL_EMULATOR_PROJECT", EmulatorProjectDefault)
	suffix := runSuffix()
	names := Names(suffix)
	target := EmulatorTarget(endpoint, emulatorProject, suffix, names)

	if err := target.EnsureKMS(); err != nil {
		t.Fatalf("ensure KMS on emulator (is it running at %s?): %v", endpoint, err)
	}

	scenarios := Scenarios(emulatorProject, suffix)

	// Match goldens to scenarios by (Service, Op) rather than by position. The
	// golden filename embeds an index that shifts whenever the curated list
	// grows, and the list is expected to grow ahead of the next real-GCP
	// recording, so positional matching would fail spuriously. Scenarios with
	// no committed golden are "pending recording" and skipped; a golden with no
	// scenario is an orphan (a stale file or a dropped scenario) and is fatal,
	// preserving the orphan-detection the positional count used to provide.
	matched, pending, orphans, duplicates := matchScenariosToGoldens(scenarios, exs)
	if len(duplicates) > 0 {
		t.Fatalf("scenario/golden (Service, Op) collision: %v", duplicates)
	}
	if len(orphans) > 0 {
		t.Fatalf("orphan golden(s) with no scenario: %v (delete the stale golden file(s) or restore the scenario)", orphans)
	}
	for _, p := range pending {
		t.Logf("pending recording: %s", p)
	}

	// Run the matched scenarios plus every Setup scenario, in scenario order so
	// a setup op precedes the golden that depends on it. Pending (unrecorded)
	// scenarios are skipped so the offline gate stays green until the user
	// records them.
	hasGolden := make(map[string]bool, len(matched))
	for _, m := range matched {
		hasGolden[scenarioKey(m.Scenario.Service, m.Scenario.Op)] = true
	}
	runScenarios := make([]Scenario, 0, len(matched))
	for _, sc := range scenarios {
		if sc.Setup || hasGolden[scenarioKey(sc.Service, sc.Op)] {
			runScenarios = append(runScenarios, sc)
		}
	}
	actual, err := target.Run(runScenarios)
	if err != nil {
		t.Fatalf("replay against emulator: %v", err)
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
	if err := WriteReport(reportDir(), rep); err != nil {
		t.Errorf("write report: %v", err)
	}

	t.Logf("replayed %d recorded ops against emulator (%d pending recording) -> %d divergences (%d open, %d accepted)",
		len(actual), len(pending), rep.Total, rep.OpenCount, rep.AcceptedCount)
	t.Logf("open by severity (real bugs):")
	for _, sev := range sortedKeys(rep.BySeverity, severityLess) {
		t.Logf("  %-8s %d", sev, rep.BySeverity[sev])
	}
	t.Logf("open by kind:")
	for _, k := range sortedKeys(rep.ByKind, nil) {
		t.Logf("  %-24s %d", k, rep.ByKind[k])
	}
	t.Logf("open by service:")
	for _, s := range sortedKeys(rep.ByService, nil) {
		t.Logf("  %-16s %d", s, rep.ByService[s])
	}
	t.Logf("report written to %s/report.{json,md}", reportDir())

	assertStrict(t, rep)
}

// assertStrict fails the test when strict mode is on (-strict or
// GCP_DIFFERENTIAL_STRICT=1) and any OPEN (real-bug) divergence is at/above the
// configured severity (GCP_DIFFERENTIAL_STRICT_SEVERITY, default high). It is
// shared by the REST and gRPC replays.
func assertStrict(t *testing.T, rep Report) {
	if !(*strictFlag || os.Getenv("GCP_DIFFERENTIAL_STRICT") == "1") {
		return
	}
	threshold := envOr("GCP_DIFFERENTIAL_STRICT_SEVERITY", "high")
	var failing []Divergence
	for _, d := range rep.Open {
		if SeverityRank(d.Severity) <= SeverityRank(threshold) {
			failing = append(failing, d)
		}
	}
	if len(failing) > 0 {
		t.Fatalf("strict mode: %d open divergence(s) at/above %q severity (%d open total)",
			len(failing), threshold, rep.OpenCount)
	}
	t.Logf("strict mode: no open divergence at/above %q severity", threshold)
}

// TestGoldensAreClean guards the hard requirement that committed goldens never
// contain credentials or project-specific identifiers.
func TestGoldensAreClean(t *testing.T) {
	assertGoldensClean(t, goldenDir())
}

// assertGoldensClean is the shared credential/identifier guard for a golden
// directory: it is applied to both the REST goldens and the AUD6-1 gRPC goldens.
func assertGoldensClean(t *testing.T, dir string) {
	files := goldensOrSkip(t, dir)
	forbidden := []struct{ name, needle string }{
		{"real project id", RealProjectDefault},
		{"real project number", RealProjectNumberDefault},
		{"emulator project id", EmulatorProjectDefault},
		{"bearer token", "Bearer "},
		{"oauth access token", "ya29."},
		{"private key marker", "PRIVATE KEY"},
		{"service account email", ".iam.gserviceaccount.com"},
	}
	// A real email address is forbidden, but a bare "@" is too broad: OAuth2
	// JSON uses "@type" discriminators (e.g. google.longrunning.Operation
	// metadata), which are not secrets. Match email shapes specifically.
	emailRE := regexp.MustCompile(`[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}`)
	for _, f := range files {
		data, err := os.ReadFile(filepath.Join(dir, f))
		if err != nil {
			t.Fatalf("read golden %s: %v", f, err)
		}
		for _, fb := range forbidden {
			if strings.Contains(string(data), fb.needle) {
				t.Errorf("golden %s contains forbidden %s (%q)", f, fb.name, fb.needle)
			}
		}
		if m := emailRE.FindString(string(data)); m != "" {
			t.Errorf("golden %s contains a forbidden email address (%q)", f, m)
		}
		var ex Exchange
		if err := json.Unmarshal(data, &ex); err != nil {
			t.Errorf("golden %s is not valid JSON: %v", f, err)
			continue
		}
		if ex.Op == "" || ex.Service == "" || ex.Method == "" || ex.Path == "" {
			t.Errorf("golden %s missing required fields: %+v", f, ex)
		}
	}
}

func TestGoldenManifest(t *testing.T) {
	files := goldensOrSkip(t, goldenDir())
	data, err := os.ReadFile(filepath.Join(goldenDir(), "manifest.json"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	if m.Ops != len(files) {
		t.Errorf("manifest ops=%d but %d golden files present", m.Ops, len(files))
	}
	if m.Project != "<project>" {
		t.Errorf("manifest project %q must be the normalized placeholder <project>", m.Project)
	}
}
