//go:build gcp_differential

package gcpdifferential

import (
	"context"
	"os"
	"strings"
	"testing"
)

// The gRPC SDK-tour differential records the official-client gRPC operations the
// tour drives (KMS, Secret Manager, Firestore, Logging, Pub/Sub + IAM policy)
// against real GCP into testdata/golden-tour-grpc, then replays them against the
// emulator's single gRPC listener offline and diffs the normalized responses.

func tourGRPCGoldenDir() string {
	return envOr("GCP_DIFFERENTIAL_TOUR_GRPC_GOLDEN_DIR", "testdata/golden-tour-grpc")
}
func tourGRPCReportDir() string {
	return envOr("GCP_DIFFERENTIAL_TOUR_GRPC_REPORT_DIR", "testdata/report-tour-grpc")
}

// TestRecordTourGRPC captures the gRPC SDK-tour goldens from real GCP. It is
// skipped unless -record (or GCP_DIFFERENTIAL_RECORD=1) is set, and requires
// ADC. The official gRPC dialer obtains ADC; no token is read or printed here.
func TestRecordTourGRPC(t *testing.T) {
	if !*recordFlag && os.Getenv("GCP_DIFFERENTIAL_RECORD") != "1" {
		t.Skip("set -record (or GCP_DIFFERENTIAL_RECORD=1) to capture gRPC SDK-tour goldens")
	}
	ctx := context.Background()
	project := envOr("GCP_DIFFERENTIAL_PROJECT", RealProjectDefault)
	projectNumber := envOr("GCP_DIFFERENTIAL_PROJECT_NUMBER", RealProjectNumberDefault)

	token, err := ADCToken()
	if err != nil {
		t.Fatalf("ADC: %v", err)
	}
	suffix := runSuffix()
	names := Names(suffix)

	// The KMS keys the tour uses are fixed and non-deletable, so they are
	// ensured (uncaptured) through the REST target before the gRPC run.
	restTarget := RealTarget(project, projectNumber, suffix, names, token)
	if err := restTarget.EnsureKMS(); err != nil {
		t.Fatalf("ensure KMS resources: %v", err)
	}
	if err := restTarget.EnsureTourKMS(); err != nil {
		t.Fatalf("ensure tour KMS resources: %v", err)
	}

	target := RealGRPCTargetFor(project, projectNumber, suffix, names)
	scenarios := TourGRPCScenarios(project, suffix)

	defer func() {
		for _, line := range target.CleanupTourGRPC(ctx) {
			t.Log(line)
		}
	}()

	exs, err := target.RunGRPC(ctx, scenarios)
	if err != nil {
		t.Fatalf("record SDK-tour gRPC ops against real GCP: %v", err)
	}
	dir := tourGRPCGoldenDir()
	if err := WriteGoldens(dir, exs); err != nil {
		t.Fatalf("write SDK-tour gRPC goldens: %v", err)
	}
	byService := map[string]int{}
	for _, ex := range exs {
		byService[ex.Service]++
	}
	t.Logf("recorded %d SDK-tour gRPC ops from real GCP project %q", len(exs), project)
	for _, svc := range sortedKeys(byService, nil) {
		t.Logf("  %-16s %d ops", svc, byService[svc])
	}
	files, _ := GoldenFiles(dir)
	t.Logf("wrote %d gRPC golden files under %s", len(files), dir)
}

// TestReplayTourGRPC is the offline CI entry point for the gRPC SDK-tour
// differential.
func TestReplayTourGRPC(t *testing.T) {
	if *recordFlag {
		t.Skip("record mode: skipping SDK-tour gRPC replay")
	}
	goldensOrSkip(t, tourGRPCGoldenDir())
	goldens, err := ReadGoldens(tourGRPCGoldenDir())
	if err != nil {
		t.Fatalf("read SDK-tour gRPC goldens at %s: %v", tourGRPCGoldenDir(), err)
	}

	ctx := context.Background()
	endpoint := strings.TrimRight(envOr("GCP_DIFFERENTIAL_GRPC_ENDPOINT", "localhost:8081"), "/")
	emulatorProject := envOr("GCP_DIFFERENTIAL_EMULATOR_PROJECT", EmulatorProjectDefault)
	suffix := runSuffix()
	names := Names(suffix)
	target := EmulatorGRPCTarget(endpoint, emulatorProject, suffix, names)
	scenarios := TourGRPCScenarios(emulatorProject, suffix)

	keys := make([]string, len(scenarios))
	for i, sc := range scenarios {
		keys[i] = scenarioKey(sc.Service, sc.Op)
	}
	byKey, pending, orphans, duplicates := matchGoldenKeys(keys, goldens)
	if len(duplicates) > 0 {
		t.Fatalf("SDK-tour gRPC scenario/golden (Service, Op) collision: %v", duplicates)
	}
	if len(orphans) > 0 {
		t.Fatalf("orphan SDK-tour gRPC golden(s) with no scenario: %v", orphans)
	}
	for _, p := range pending {
		t.Logf("pending recording: %s", p)
	}

	// The tour's KMS keys must exist on the emulator too.
	restEndpoint := strings.TrimRight(envOr("GCP_DIFFERENTIAL_ENDPOINT", "http://localhost:8080"), "/")
	restTarget := EmulatorTarget(restEndpoint, emulatorProject, suffix, names)
	if err := restTarget.EnsureKMS(); err != nil {
		t.Fatalf("ensure KMS on emulator (is it running at %s?): %v", restEndpoint, err)
	}
	if err := restTarget.EnsureTourKMS(); err != nil {
		t.Fatalf("ensure tour KMS on emulator: %v", err)
	}

	runScenarios := make([]GRPCScenario, 0, len(byKey))
	for _, sc := range scenarios {
		if _, ok := byKey[scenarioKey(sc.Service, sc.Op)]; ok {
			runScenarios = append(runScenarios, sc)
		}
	}

	defer func() {
		for _, line := range target.CleanupTourGRPC(ctx) {
			t.Log(line)
		}
	}()

	actual, err := target.RunGRPC(ctx, runScenarios)
	if err != nil {
		t.Fatalf("replay SDK-tour gRPC ops against emulator: %v", err)
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
			t.Errorf("replayed SDK-tour gRPC exchange missing for %s", displayKey(k))
			continue
		}
		divs = append(divs, DiffExchanges(byKey[k], a)...)
	}

	rep := BuildReport(target.Name, emulatorProject, len(actual), divs)
	if err := WriteReport(tourGRPCReportDir(), rep); err != nil {
		t.Errorf("write SDK-tour gRPC report: %v", err)
	}
	t.Logf("replayed %d SDK-tour gRPC ops against emulator (%d pending recording) -> %d divergences (%d open, %d accepted)",
		len(actual), len(pending), rep.Total, rep.OpenCount, rep.AcceptedCount)
	for _, s := range sortedKeys(rep.ByService, nil) {
		t.Logf("  %-16s %d open", s, rep.ByService[s])
	}
	t.Logf("report written to %s/report.{json,md}", tourGRPCReportDir())

	assertStrict(t, rep)
}

// TestTourGRPCScenariosValid guards the gRPC SDK-tour list contract: every
// scenario is addressable, every Service maps to a real-GCP gRPC endpoint, and
// (Service, Op) is unique.
func TestTourGRPCScenariosValid(t *testing.T) {
	scenarios := TourGRPCScenarios("proj", "abc123")
	if len(scenarios) == 0 {
		t.Fatal("TourGRPCScenarios returned no scenarios")
	}
	seen := map[string]bool{}
	for _, sc := range scenarios {
		if sc.Service == "" || sc.Op == "" || sc.Method == "" || sc.Call == nil {
			t.Errorf("SDK-tour gRPC scenario %+v has an empty required field", sc)
		}
		if _, ok := grpcServiceEndpoint[sc.Service]; !ok {
			t.Errorf("SDK-tour gRPC scenario %s/%s: service %q has no grpcServiceEndpoint entry", sc.Service, sc.Op, sc.Service)
		}
		k := scenarioKey(sc.Service, sc.Op)
		if seen[k] {
			t.Errorf("duplicate SDK-tour gRPC (Service, Op): %s", displayKey(k))
		}
		seen[k] = true
	}
}

// TestTourGRPCGoldensAreClean applies the shared credential/identifier guard to
// the gRPC SDK-tour goldens.
func TestTourGRPCGoldensAreClean(t *testing.T) { assertGoldensClean(t, tourGRPCGoldenDir()) }
