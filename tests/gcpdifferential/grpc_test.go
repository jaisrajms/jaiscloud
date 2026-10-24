//go:build gcp_differential

package gcpdifferential

import (
	"context"
	"os"
	"strings"
	"testing"
)

// TestRecordGRPC captures the gRPC goldens from real GCP. It is skipped unless
// -record (or GCP_DIFFERENTIAL_RECORD=1) is set, and requires Application
// Default Credentials. The gRPC dialer obtains ADC itself; the one exception is
// the dead-letter forwarding IAM grant (a REST project-policy call, see
// EnsureDLQForwarding), for which a token is read but never printed. It cleans
// up every resource it creates.
func TestRecordGRPC(t *testing.T) {
	if !*recordFlag && os.Getenv("GCP_DIFFERENTIAL_RECORD") != "1" {
		t.Skip("set -record (or GCP_DIFFERENTIAL_RECORD=1) to capture gRPC goldens from real GCP")
	}
	ctx := context.Background()
	project := envOr("GCP_DIFFERENTIAL_PROJECT", RealProjectDefault)

	suffix := runSuffix()
	names := Names(suffix)
	target := RealGRPCTarget(project, suffix, names)
	scenarios := GRPCScenarios(project, suffix)

	// Real GCP requires the Pub/Sub service agent to hold publisher/subscriber
	// before it will forward to a dead-letter topic; grant them (REST surface)
	// before the dead-letter scenario records. ADC is only read here, never
	// printed.
	if token, err := ADCToken(); err != nil {
		t.Fatalf("ADC: %v", err)
	} else {
		projectNumber := envOr("GCP_DIFFERENTIAL_PROJECT_NUMBER", RealProjectNumberDefault)
		if err := RealTarget(project, projectNumber, suffix, names, token).EnsureDLQForwarding(); err != nil {
			t.Fatalf("ensure DLQ forwarding roles: %v", err)
		}
	}

	// Cleanup always runs, even if the capture aborts partway.
	defer func() {
		for _, line := range target.CleanupGRPC(ctx) {
			t.Log(line)
		}
	}()

	exs, err := target.RunGRPC(ctx, scenarios)
	if err != nil {
		t.Fatalf("record gRPC against real GCP: %v", err)
	}

	dir := grpcGoldenDir()
	if err := WriteGoldens(dir, exs); err != nil {
		t.Fatalf("write gRPC goldens: %v", err)
	}

	byService := map[string]int{}
	for _, ex := range exs {
		byService[ex.Service]++
	}
	t.Logf("recorded %d gRPC ops from real GCP project %q", len(exs), project)
	for _, svc := range sortedKeys(byService, nil) {
		t.Logf("  %-16s %d ops", svc, byService[svc])
	}
	files, _ := GoldenFiles(dir)
	t.Logf("wrote %d gRPC golden files under %s", len(files), dir)
}

// TestReplayGRPC is the offline CI entry point for the gRPC differential: it
// replays the committed gRPC goldens against the emulator's gRPC listener,
// diffs with the shared engine, and writes report.{json,md}. Divergences are
// reported, not fatal, unless strict mode is on.
func TestReplayGRPC(t *testing.T) {
	if *recordFlag {
		t.Skip("record mode: skipping gRPC replay")
	}
	goldensOrSkip(t, grpcGoldenDir())
	goldens, err := ReadGoldens(grpcGoldenDir())
	if err != nil {
		t.Fatalf("read gRPC goldens at %s: %v", grpcGoldenDir(), err)
	}

	ctx := context.Background()
	endpoint := strings.TrimRight(envOr("GCP_DIFFERENTIAL_GRPC_ENDPOINT", "localhost:8081"), "/")
	emulatorProject := envOr("GCP_DIFFERENTIAL_EMULATOR_PROJECT", EmulatorProjectDefault)
	suffix := runSuffix()
	names := Names(suffix)
	target := EmulatorGRPCTarget(endpoint, emulatorProject, suffix, names)

	scenarios := GRPCScenarios(emulatorProject, suffix)

	// Match goldens to scenarios by (Service, Op), exactly as the REST replay
	// does: scenarios without a golden are "pending recording" (skipped) and a
	// golden with no scenario is an orphan (fatal).
	keys := make([]string, len(scenarios))
	for i, sc := range scenarios {
		keys[i] = scenarioKey(sc.Service, sc.Op)
	}
	byKey, pending, orphans, duplicates := matchGoldenKeys(keys, goldens)
	if len(duplicates) > 0 {
		t.Fatalf("gRPC scenario/golden (Service, Op) collision: %v", duplicates)
	}
	if len(orphans) > 0 {
		t.Fatalf("orphan gRPC golden(s) with no scenario: %v (delete the stale golden file(s) or restore the scenario)", orphans)
	}
	for _, p := range pending {
		t.Logf("pending recording: %s", p)
	}

	runScenarios := make([]GRPCScenario, 0, len(byKey))
	for _, sc := range scenarios {
		if _, ok := byKey[scenarioKey(sc.Service, sc.Op)]; ok {
			runScenarios = append(runScenarios, sc)
		}
	}

	// Cleanup runs best-effort so a replay against a long-lived emulator leaves
	// nothing behind even if it aborts partway.
	defer func() {
		for _, line := range target.CleanupGRPC(ctx) {
			t.Log(line)
		}
	}()

	actual, err := target.RunGRPC(ctx, runScenarios)
	if err != nil {
		t.Fatalf("replay gRPC against emulator: %v", err)
	}
	actualByKey := make(map[string]Exchange, len(actual))
	for _, ex := range actual {
		actualByKey[scenarioKey(ex.Service, ex.Op)] = ex
	}

	var divs []Divergence
	for _, sc := range runScenarios {
		k := scenarioKey(sc.Service, sc.Op)
		a, ok := actualByKey[k]
		if !ok {
			t.Errorf("replayed gRPC exchange missing for %s", displayKey(k))
			continue
		}
		divs = append(divs, DiffExchanges(byKey[k], a)...)
	}

	rep := BuildReport(target.Name, emulatorProject, len(actual), divs)
	if err := WriteReport(grpcReportDir(), rep); err != nil {
		t.Errorf("write gRPC report: %v", err)
	}

	t.Logf("replayed %d recorded gRPC ops against emulator (%d pending recording) -> %d divergences (%d open, %d accepted)",
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
	t.Logf("report written to %s/report.{json,md}", grpcReportDir())

	assertStrict(t, rep)
}

// TestGRPCScenariosValid exercises the curated gRPC list contract: every
// scenario is addressable (non-empty Service/Op/Method and a Call), every
// Service has a real-GCP endpoint, and (Service, Op) is unique so goldens match
// by key instead of position. Every gRPC-only surface must stay represented.
func TestGRPCScenariosValid(t *testing.T) {
	const project = "proj"
	const suffix = "abc123"
	scenarios := GRPCScenarios(project, suffix)
	if len(scenarios) == 0 {
		t.Fatal("GRPCScenarios returned no scenarios")
	}

	seen := map[string]bool{}
	have := map[string]bool{}
	for _, sc := range scenarios {
		if sc.Service == "" || sc.Op == "" || sc.Method == "" || sc.Call == nil {
			t.Errorf("gRPC scenario %+v has an empty required field", sc)
		}
		if _, ok := grpcServiceEndpoint[sc.Service]; !ok {
			t.Errorf("gRPC scenario %s/%s: service %q has no grpcServiceEndpoint entry", sc.Service, sc.Op, sc.Service)
		}
		k := scenarioKey(sc.Service, sc.Op)
		if seen[k] {
			t.Errorf("duplicate gRPC (Service, Op): %s", displayKey(k))
		}
		seen[k] = true
		have[sc.Service] = true
	}
	for _, svc := range []string{"datastore", "firestore", "logging", "monitoring", "dataproc"} {
		if !have[svc] {
			t.Errorf("expected at least one %s gRPC scenario", svc)
		}
	}
}

// TestGRPCNormalizerFoldsVolatile guards that the gRPC-only volatile fields are
// folded, so a committed golden carries no run-specific cursor, transaction,
// version, timestamp or index-update count.
func TestGRPCNormalizerFoldsVolatile(t *testing.T) {
	names := Names("abc123")
	norm := NewNormalizer("differential-proj", "", "abc123", names)

	raw := `{
		"readTime": "2026-10-08T06:48:47.577489Z",
		"indexUpdates": 3,
		"batch": {"endCursor": "CjsSNWoVY2lhbHM=", "snapshotVersion": "1791442127577489"},
		"missing": [{"version": "1791442127218230"}],
		"entries": [{"insertId": "abc123", "timestamp": "2026-10-08T06:48:47.218230Z"}]
	}`
	got := string(norm.Bytes([]byte(raw)))
	for _, want := range []string{`"readTime":"<time>"`, `"indexUpdates":"<indexUpdates>"`, `"endCursor":"<cursor>"`, `"snapshotVersion":"<snapshotVersion>"`, `"version":"<version>"`, `"insertId":"<insertId>"`, `"timestamp":"<time>"`} {
		if !strings.Contains(got, want) {
			t.Errorf("normalized gRPC body missing %s: %s", want, got)
		}
	}

	// Response-only keys must NOT be folded in a request body, where a
	// client-authored value (BigQuery's insertId dedup key) must survive
	// verbatim. Request cursors and transaction ids are the exception: a
	// startCursor/endCursor/transaction a client sends back always originated in
	// a previous response, so they fold on both sides.
	req := string(norm.RequestBytes([]byte(`{"insertId":"1","transaction":"abc","startCursor":"abc","endCursor":"def","indexUpdates":3}`)))
	for _, want := range []string{`"insertId":"1"`, `"transaction":"<transaction>"`, `"indexUpdates":3`, `"startCursor":"<cursor>"`, `"endCursor":"<cursor>"`} {
		if !strings.Contains(req, want) {
			t.Errorf("normalized request body missing %s: %s", want, req)
		}
	}
}
