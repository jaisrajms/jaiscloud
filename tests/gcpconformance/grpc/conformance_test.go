package grpcconformance

import (
	"context"
	"os"
	"testing"
	"time"
)

const defaultReportDir = "testdata/report"

// TestGRPCConformance drives the emulator's gRPC endpoint with the official
// Google client libraries, records pass/fail/unimplemented per service/RPC in
// testdata/report/report.{json,md}, and fails the build on any failing or
// unimplemented check (an Unimplemented RPC is a real fidelity gap).
//
// It skips (rather than fails) when no emulator is listening, so `go test ./...`
// on a developer machine without the emulator running stays green. Set
// GCP_GRPC_CONFORMANCE_SKIP=1 or run with -short to skip explicitly.
func TestGRPCConformance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping gRPC conformance in -short mode")
	}
	if os.Getenv("GCP_GRPC_CONFORMANCE_SKIP") == "1" {
		t.Skip("GCP_GRPC_CONFORMANCE_SKIP=1")
	}

	cfg := ConfigFromEnv()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	if err := Probe(ctx, cfg); err != nil {
		t.Skipf("emulator gRPC endpoint %s not reachable: %v", cfg.Endpoint, err)
	}

	results := RunAll(ctx, cfg, Registry())
	report := BuildReport(cfg, results)

	reportDir := defaultReportDir
	if d := os.Getenv("GCP_GRPC_CONFORMANCE_REPORT_DIR"); d != "" {
		reportDir = d
	}
	if err := WriteReport(reportDir, report); err != nil {
		t.Fatalf("WriteReport: %v", err)
	}

	for _, r := range results {
		if r.Error != "" {
			t.Logf("%-14s %-34s %-14s %s", r.Service, r.RPC, r.Status, r.Error)
			continue
		}
		t.Logf("%-14s %-34s %-14s (key field %s)", r.Service, r.RPC, r.Status, r.KeyField)
	}
	t.Logf("rollup: %d checks, %d pass, %d fail, %d unimplemented (%.1f%% pass)",
		report.Rollup.Checks, report.Rollup.Passed, report.Rollup.Failed,
		report.Rollup.Unimplemented, report.Rollup.PassRate)
	t.Logf("report written to %s/report.{json,md}", reportDir)

	var gate []Result
	for _, r := range results {
		if r.Status == StatusFail || r.Status == StatusUnimplemented {
			gate = append(gate, r)
		}
	}
	if len(gate) > 0 {
		for _, r := range gate {
			t.Errorf("[%s] %s %s: %s", r.Status, r.Service, r.RPC, r.Error)
		}
		t.Fatalf("gRPC conformance gate failed: %d check(s)", len(gate))
	}
}
