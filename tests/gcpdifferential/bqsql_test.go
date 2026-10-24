//go:build gcp_differential

package gcpdifferential

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
)

// The BigQuery SQL differential corpus (PLAN 5). It records a curated corpus of
// Standard SQL queries from REAL BigQuery into testdata/golden-bigquery, then
// replays them against the emulator offline and diffs rows, schema and error
// envelopes. It reuses the curated REST differential's target, normalizer, diff,
// triage and reporting plumbing; only the scenario list and the golden directory
// differ. Unlike the other sets it does not fail on every open divergence: each
// query declares an expectation (match / gap / bug), and only a `match` query
// that diverges — an unexpected regression — fails the gate. Known gaps and
// engine bugs are reported with real-vs-emulator evidence.

func bqsqlGoldenDir() string {
	return envOr("GCP_DIFFERENTIAL_BQSQL_GOLDEN_DIR", "testdata/golden-bigquery")
}
func bqsqlReportDir() string {
	return envOr("GCP_DIFFERENTIAL_BQSQL_REPORT_DIR", "testdata/report-bigquery")
}

// TestRecordBQSQL captures the BigQuery SQL corpus goldens from real GCP. It is
// skipped unless -record (or GCP_DIFFERENTIAL_RECORD=1) is set, and requires
// ADC and a project with BigQuery enabled. It never prints the token and deletes
// the fixture dataset afterwards.
func TestRecordBQSQL(t *testing.T) {
	if !*recordFlag && os.Getenv("GCP_DIFFERENTIAL_RECORD") != "1" {
		t.Skip("set -record (or GCP_DIFFERENTIAL_RECORD=1) to capture BigQuery SQL goldens from real GCP")
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
	scenarios := BQSQLScenarios(project, suffix)

	defer func() {
		for _, line := range target.CleanupBQSQL() {
			t.Log(line)
		}
		for _, line := range target.VerifyBQSQLAbsent() {
			t.Log(line)
		}
	}()

	exs, err := target.Run(scenarios)
	if err != nil {
		t.Fatalf("record BigQuery SQL corpus against real GCP: %v", err)
	}

	dir := bqsqlGoldenDir()
	if err := WriteGoldens(dir, exs); err != nil {
		t.Fatalf("write BigQuery SQL goldens: %v", err)
	}
	t.Logf("recorded %d BigQuery SQL queries from real GCP project %q", len(exs), project)
	files, _ := GoldenFiles(dir)
	t.Logf("wrote %d golden files under %s", len(files), dir)
}

// bqsqlResult is one corpus query's replay outcome, used to print the
// per-query match/mismatch table.
type bqsqlResult struct {
	Case         BQSQLCase
	Open         []Divergence
	Accepted     int
	Status       int
	OpenOverflow int // open divergences beyond the first few shown
}

// TestReplayBQSQL is the offline entry point: it replays the committed corpus
// goldens against the local emulator and classifies each query against its
// declared expectation. A `match` query that diverges is an unexpected
// regression and fails; a `gap`/`bug` query is reported with evidence. A
// `gap`/`bug` query that has started matching is flagged as a stale expectation
// (an improvement to fold in), not a failure.
func TestReplayBQSQL(t *testing.T) {
	if *recordFlag {
		t.Skip("record mode: skipping BigQuery SQL replay")
	}
	goldensOrSkip(t, bqsqlGoldenDir())
	goldens, err := ReadGoldens(bqsqlGoldenDir())
	if err != nil {
		t.Fatalf("read BigQuery SQL goldens at %s: %v", bqsqlGoldenDir(), err)
	}

	endpoint := strings.TrimRight(envOr("GCP_DIFFERENTIAL_ENDPOINT", "http://localhost:8080"), "/")
	// The corpus replays in its own emulator project: it creates 74 jobs.query
	// jobs, and the curated set's project-scoped BigQuery jobs.list /
	// datasets.list goldens would otherwise see them (the emulator's job store
	// is per project). The normalizer folds the project id to <project> either
	// way, so the committed goldens are unaffected.
	emulatorProject := envOr("GCP_DIFFERENTIAL_BQSQL_PROJECT", "jaiscloud-bqsql-diff")
	suffix := runSuffix()
	names := Names(suffix)
	target := EmulatorTarget(endpoint, emulatorProject, suffix, names)
	scenarios := BQSQLScenarios(emulatorProject, suffix)

	matched, pending, orphans, duplicates := matchScenariosToGoldens(scenarios, goldens)
	if len(duplicates) > 0 {
		t.Fatalf("BigQuery SQL scenario/golden (Service, Op) collision: %v", duplicates)
	}
	if len(orphans) > 0 {
		t.Fatalf("orphan BigQuery SQL golden(s) with no scenario: %v (delete the stale golden file(s) or restore the scenario)", orphans)
	}
	for _, p := range pending {
		t.Logf("pending recording: %s", p)
	}

	// Run the matched queries plus every fixture Setup scenario, in scenario
	// order so the fixture precedes the queries that depend on it.
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

	defer func() {
		for _, line := range target.CleanupBQSQL() {
			t.Log(line)
		}
	}()

	actual, err := target.Run(runScenarios)
	if err != nil {
		t.Fatalf("replay BigQuery SQL corpus against emulator: %v", err)
	}
	actualByKey := make(map[string]Exchange, len(actual))
	for _, ex := range actual {
		actualByKey[scenarioKey(ex.Service, ex.Op)] = ex
	}

	cases := bqsqlCaseByOp()
	var divs []Divergence
	var results []bqsqlResult
	openByOp := map[string][]Divergence{}
	var regressions, stale []string
	for _, m := range matched {
		op := m.Scenario.Op
		c, ok := cases[op]
		if !ok {
			t.Errorf("replayed BigQuery SQL golden %s has no corpus case", op)
			continue
		}
		a, ok := actualByKey[scenarioKey(m.Scenario.Service, op)]
		if !ok {
			t.Errorf("replayed exchange missing for bigquery/%s", op)
			continue
		}
		d := DiffExchanges(canonicalizeBQSQLExchange(m.Golden), canonicalizeBQSQLExchange(a))
		divs = append(divs, d...)
		open, accepted := ApplyTriage(d)
		openByOp[op] = open
		res := bqsqlResult{Case: c, Open: open, Accepted: len(accepted), Status: a.Status}
		if len(open) > 3 {
			res.OpenOverflow = len(open) - 3
			res.Open = open[:3]
		}
		results = append(results, res)

		switch c.Expect {
		case BQSQLMatch:
			if len(open) > 0 {
				regressions = append(regressions, op)
			}
		default:
			if len(open) == 0 {
				stale = append(stale, op)
			}
		}
	}

	rep := BuildReport(target.Name, emulatorProject, len(actual), divs)
	if err := WriteReport(bqsqlReportDir(), rep); err != nil {
		t.Errorf("write BigQuery SQL report: %v", err)
	}

	logBQSQLTable(t, results, pending)
	writeBQSQLBacklog(t, bqsqlReportDir(), results, openByOp, pending)
	t.Logf("replayed %d BigQuery SQL queries (%d pending recording) -> %d divergences (%d open, %d accepted); report at %s/report.{json,md}, frontier at %s/%s",
		len(results), len(pending), rep.Total, rep.OpenCount, rep.AcceptedCount, bqsqlReportDir(), bqsqlReportDir(), bqsqlBacklogFile)

	if len(stale) > 0 {
		sort.Strings(stale)
		t.Logf("STALE expectations (emulator now matches; update Expect): %v", stale)
	}
	if len(regressions) > 0 {
		sort.Strings(regressions)
		// Print the evidence for each regression before failing.
		for _, r := range regressions {
			t.Logf("REGRESSION bigquery/%s:", r)
			for _, d := range openByOp[r] {
				t.Logf("  %s %s: real=%s emulator=%s", d.Kind, d.Location, d.Expected, d.Actual)
			}
		}
		t.Errorf("%d previously-matching BigQuery SQL query(s) diverged: %v", len(regressions), regressions)
	}
}

// logBQSQLTable prints the per-query match/mismatch table.
func logBQSQLTable(t *testing.T, results []bqsqlResult, pending []string) {
	t.Helper()
	sort.SliceStable(results, func(i, j int) bool { return results[i].Case.ID < results[j].Case.ID })
	t.Logf("BigQuery SQL differential corpus (per query):")
	t.Logf("  %-24s %-6s %-10s %-6s %-5s %s", "id", "class", "expect", "result", "http", "detail")
	for _, r := range results {
		result := "match"
		if len(r.Open) > 0 {
			result = "diverges"
		}
		detail := fmt.Sprintf("%d open, %d accepted", len(r.Open)+r.OpenOverflow, r.Accepted)
		if len(r.Open) > 0 {
			detail = fmt.Sprintf("%s %s: real=%s emulator=%s", r.Open[0].Kind, r.Open[0].Location, r.Open[0].Expected, r.Open[0].Actual)
			if r.OpenOverflow > 0 {
				detail += fmt.Sprintf(" (+%d more)", r.OpenOverflow)
			}
		}
		t.Logf("  %-24s %-6s %-10s %-6s %-5d %s", r.Case.ID, r.Case.Class, r.Case.Expect, result, r.Status, detail)
	}
	if len(pending) > 0 {
		sort.Strings(pending)
		t.Logf("  pending recording (%d): %s", len(pending), strings.Join(pending, ", "))
	}
}

// TestBQSQLScenariosValid guards the corpus contract: every scenario is
// addressable, every Service maps to a real-GCP origin, (Service, Op) is unique
// and every query op has a declared corpus case.
func TestBQSQLScenariosValid(t *testing.T) {
	scenarios := BQSQLScenarios("proj", "abc123")
	if len(scenarios) == 0 {
		t.Fatal("BQSQLScenarios returned no scenarios")
	}
	cases := bqsqlCaseByOp()
	seen := map[string]bool{}
	for _, sc := range scenarios {
		if sc.Service == "" || sc.Op == "" || sc.Method == "" || sc.Path == "" {
			t.Errorf("BigQuery SQL scenario %+v has an empty required field", sc)
		}
		if _, ok := serviceBaseURL[sc.Service]; !ok {
			t.Errorf("BigQuery SQL scenario %s: service %q has no serviceBaseURL entry", sc.Op, sc.Service)
		}
		k := scenarioKey(sc.Service, sc.Op)
		if seen[k] {
			t.Errorf("duplicate BigQuery SQL (Service, Op): %s", displayKey(k))
		}
		seen[k] = true
		if strings.HasPrefix(sc.Op, "bqsql_") && !strings.HasPrefix(sc.Op, "bqsql_setup_") {
			if _, ok := cases[sc.Op]; !ok {
				t.Errorf("query scenario %s has no corpus case", sc.Op)
			}
		}
	}
	for _, c := range BQSQLCases() {
		if c.ID == "" || c.SQL == "" || c.Class == "" {
			t.Errorf("corpus case %+v has an empty required field", c)
		}
		switch c.Expect {
		case BQSQLMatch, BQSQLGap, BQSQLBug:
		default:
			t.Errorf("corpus case %s has an invalid expectation %q", c.ID, c.Expect)
		}
	}
}

// TestBQSQLGoldensAreClean applies the shared credential/identifier guard.
func TestBQSQLGoldensAreClean(t *testing.T) { assertGoldensClean(t, bqsqlGoldenDir()) }

// TestBQSQLGoldenManifest checks the corpus manifest matches the files.
func TestBQSQLGoldenManifest(t *testing.T) {
	files := goldensOrSkip(t, bqsqlGoldenDir())
	data, err := os.ReadFile(bqsqlGoldenDir() + "/manifest.json")
	if err != nil {
		t.Fatalf("read BigQuery SQL manifest: %v", err)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("parse BigQuery SQL manifest: %v", err)
	}
	if m.Ops != len(files) {
		t.Errorf("manifest ops=%d but %d golden files present", m.Ops, len(files))
	}
}
