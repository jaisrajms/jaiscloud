//go:build gcp_parity && gcp_conformance

package gcpparity

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	"jaiscloud/tests/gcpconformance"
)

// TestRESTGRPCParity drives every registered scenario against a live emulator,
// reading each resource back over both transports and diffing the normalized
// bodies. It skips (rather than fails) when no emulator is listening so a bare
// `go test ./...` stays green.
func TestRESTGRPCParity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping REST↔gRPC parity in -short mode")
	}
	if os.Getenv("GCP_PARITY_SKIP") == "1" {
		t.Skip("GCP_PARITY_SKIP=1")
	}

	cfg := ConfigFromEnv()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if err := probeREST(ctx, cfg.REST); err != nil {
		t.Skipf("emulator REST endpoint %s not reachable: %v", cfg.REST, err)
	}
	if err := probeGRPC(ctx, cfg.GRPCAddr()); err != nil {
		t.Skipf("emulator gRPC endpoint %s not reachable: %v", cfg.GRPC, err)
	}

	allowances, err := loadAllowances()
	if err != nil {
		t.Fatalf("load allowances: %v", err)
	}

	env, err := NewEnv(ctx, cfg)
	if err != nil {
		t.Fatalf("new env: %v", err)
	}
	defer env.Close()

	var results []ServiceResult
	for _, sc := range Registry() {
		results = append(results, runScenario(ctx, env, sc, allowances))
	}

	rep := buildReport(cfg, results)
	reportDir := defaultReportDir
	if d := os.Getenv("GCP_PARITY_REPORT_DIR"); d != "" {
		reportDir = d
	}
	if err := writeReport(reportDir, rep); err != nil {
		t.Errorf("write report: %v", err)
	}

	for _, s := range rep.Services {
		t.Logf("%-18s steps=%d compared=%d covered=%s findings=%d%s",
			s.Service, s.Steps, s.Compared, yesNo(s.Covered), len(s.Findings), abortedNote(s.Aborted))
	}
	t.Logf("parity: %d steps, %d comparisons, %d findings (%d failing, %d allowed)",
		rep.Steps, rep.Compared, rep.Findings, rep.Failures, rep.Allowed)
	t.Logf("report written to %s/parity.{json,md}", reportDir)

	for _, s := range rep.Services {
		for _, f := range s.Findings {
			if !f.failing() {
				continue
			}
			t.Errorf("parity divergence [%s] %s/%s %s at %s: rest=%s grpc=%s",
				f.Kind, s.Service, f.Op, f.Severity, f.Location, f.Expected, f.Actual)
		}
	}
}

func abortedNote(b bool) string {
	if b {
		return " ABORTED"
	}
	return ""
}

// TestParityCoverage is the AUD3 coverage gate: every service the emulator
// serves over BOTH transports must have a parity scenario, or an explicit,
// reasoned exemption. The dual set is derived mechanically from the emulator's
// own registries so it cannot drift as services gain transports.
func TestParityCoverage(t *testing.T) {
	dual := dualServices(t)
	covered := map[string]bool{}
	for _, sc := range Registry() {
		covered[sc.Service] = true
	}
	exempt := loadExemptions(t)

	var missing, stale []string
	for _, svc := range dual {
		if covered[svc] {
			if _, ok := exempt[svc]; ok {
				stale = append(stale, svc)
			}
			continue
		}
		if _, ok := exempt[svc]; ok {
			continue
		}
		missing = append(missing, svc)
	}
	if len(stale) > 0 {
		sort.Strings(stale)
		t.Fatalf("parity coverage: %d service(s) have both a scenario and an exemption: %v\n"+
			"(remove the stale entry from %s)", len(stale), stale, exemptionsPath)
	}
	if len(missing) == 0 {
		t.Logf("parity coverage: all %d dual services have a scenario or an exemption", len(dual))
		return
	}
	sort.Strings(missing)
	t.Fatalf("parity coverage: %d dual service(s) have no scenario and no exemption: %v\n"+
		"(add a scenario in cases_*.go, or an entry to %s with a reason)",
		len(missing), missing, exemptionsPath)
}

// dualServices intersects the REST and gRPC registries: a service is dual when
// the emulator registers at least one REST operation and at least one gRPC
// method for it.
func dualServices(t *testing.T) []string {
	t.Helper()
	rest := map[string]bool{}
	for _, op := range gcpconformance.Enumerate() {
		if op.Service != "" {
			rest[op.Service] = true
		}
	}
	grpc := map[string]bool{}
	for _, s := range gcpconformance.EnumerateGRPC() {
		grpc[s.Service] = true
	}
	var dual []string
	for svc := range rest {
		if grpc[svc] {
			dual = append(dual, svc)
		}
	}
	sort.Strings(dual)
	return dual
}

type parityExemption struct {
	Service string `json:"service"`
	Reason  string `json:"reason"`
}

type exemptionFile struct {
	Exemptions []parityExemption `json:"exemptions"`
}

const exemptionsPath = "testdata/parity-exemptions.json"

func loadExemptions(t *testing.T) map[string]string {
	t.Helper()
	raw, err := os.ReadFile(exemptionsPath)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]string{}
		}
		t.Fatalf("read %s: %v", exemptionsPath, err)
	}
	var f exemptionFile
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatalf("parse %s: %v", exemptionsPath, err)
	}
	out := map[string]string{}
	for _, e := range f.Exemptions {
		if e.Service == "" || e.Reason == "" {
			t.Fatalf("%s: an exemption is missing service or reason: %+v", exemptionsPath, e)
		}
		out[e.Service] = e.Reason
	}
	return out
}

func probeREST(ctx context.Context, endpoint string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/_jaiscloud/health", nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health returned %d", resp.StatusCode)
	}
	return nil
}

func probeGRPC(ctx context.Context, addr string) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	conn.Connect()
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return nil
		}
		if !conn.WaitForStateChange(waitCtx, state) {
			return fmt.Errorf("gRPC endpoint %s not ready (state %s)", addr, state)
		}
	}
}
