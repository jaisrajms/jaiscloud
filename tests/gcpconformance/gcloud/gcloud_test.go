//go:build gcloud_conformance

package gcloud

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestGcloudCLIConformance drives the real gcloud binary against a running
// jaiscloud GCP emulator and records pass/fail/unsupported for a curated
// command table. It fails only when a command expected to pass does not — i.e.
// a regression attributable to the emulator. Documented incompatibilities
// (Command.Gap) are recorded, never fatal.
func TestGcloudCLIConformance(t *testing.T) {
	bin, err := exec.LookPath("gcloud")
	if err != nil {
		t.Skip("gcloud not found on PATH; skipping gcloud CLI conformance")
	}

	endpoint := envOr("GCP_EMULATOR_ENDPOINT", EmulatorEndpointDefault)
	project := envOr("GCP_EMULATOR_PROJECT", EmulatorProjectDefault)

	if !emulatorReachable(endpoint) {
		t.Skipf("emulator at %s is not reachable (GET /_jaiscloud/health); skipping", endpoint)
	}

	cfgDir := t.TempDir()
	workDir := t.TempDir()
	uploadFile := filepath.Join(workDir, "upload.txt")
	if err := os.WriteFile(uploadFile, []byte(ConformanceText+"\n"), 0o644); err != nil {
		t.Fatalf("write upload fixture: %v", err)
	}
	wfFile := filepath.Join(workDir, "workflow.yaml")
	if err := os.WriteFile(wfFile, []byte(WorkflowSource), 0o644); err != nil {
		t.Fatalf("write workflow fixture: %v", err)
	}
	// A minimal Cloud Functions source dir for `gcloud functions deploy --gen2`.
	fnSrcDir := filepath.Join(workDir, "fn-src")
	if err := os.MkdirAll(fnSrcDir, 0o755); err != nil {
		t.Fatalf("write function source dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(fnSrcDir, "main.py"), []byte("def handler(request):\n    return \"ok\"\n"), 0o644); err != nil {
		t.Fatalf("write function source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(fnSrcDir, "requirements.txt"), nil, 0o644); err != nil {
		t.Fatalf("write function requirements: %v", err)
	}

	fx := fixtures{
		sid:   "gc" + strconv.FormatInt(time.Now().UnixNano(), 36),
		tmp:   uploadFile,
		wf:    wfFile,
		fnSrc: fnSrcDir,
	}
	env := mergeEnv(os.Environ(), BuildEnv(endpoint, project, cfgDir, "dummy"))
	runner := &Runner{Gcloud: bin, Env: env}

	version := gcloudVersion(bin, env)
	t.Logf("gcloud: %s", version)
	t.Logf("endpoint: %s  project: %s  run-id: %s", endpoint, project, fx.sid)

	table := commands(fx)
	results := make([]CommandResult, 0, len(table))
	for _, c := range table {
		runner.Timeout = commandTimeout(c.Name)
		res := runner.Run(c.Args)

		status, regression, why := classify(c, res)
		results = append(results, CommandResult{
			Name:       c.Name,
			Service:    c.Service,
			Args:       c.Args,
			Status:     status,
			Expected:   c.Expect,
			ExitCode:   res.ExitCode,
			DurationMS: res.DurationMS,
			Regression: regression,
			Error:      why,
			Gap:        c.Gap,
			Stdout:     excerpt(res.Stdout),
			Stderr:     excerpt(res.Stderr),
		})

		t.Logf("[%-10s] %-11s (want %-11s) exit=%-3d gcloud %s",
			c.Service, status, c.Expect, res.ExitCode, strings.Join(c.Args, " "))
		switch {
		case regression:
			t.Errorf("REGRESSION %s: %s", c.Name, why)
		case status == StatusUnsupported && c.Expect == ExpectPass:
			t.Logf("  gcloud rejected an expected-pass command (tolerated, not an emulator regression): %s", why)
		case status == StatusFail && c.Gap == "":
			t.Logf("  unexpected fail (non-fatal, no documented gap): %s", why)
		}
	}

	base := strings.TrimRight(endpoint, "/")
	overrides := map[string]string{}
	for _, svc := range endpointOverrideServices {
		overrides["api_endpoint_overrides/"+strings.ToLower(svc)] = base + "/"
	}
	overrides["api_endpoint_overrides/bigquery"] = base + "/bigquery/v2/"

	rep := Report{
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		GcloudVersion: version,
		Endpoint:      endpoint,
		Project:       project,
		AuthRecipe: []string{
			"CLOUDSDK_AUTH_ACCESS_TOKEN=dummy",
			"CLOUDSDK_CORE_PROJECT=" + project,
			"CLOUDSDK_CORE_DISABLE_PROMPTS=1",
			"CLOUDSDK_CORE_DISABLE_USAGE_REPORTING=true",
			"CLOUDSDK_CONFIG=<throwaway tempdir>",
		},
		EndpointOverrides: overrides,
		Results:           results,
	}
	rep.Summary, rep.ByService = Summarize(results)

	reportDir := filepath.Join("testdata", "report")
	if err := WriteReport(reportDir, rep); err != nil {
		t.Errorf("write report: %v", err)
	} else {
		t.Logf("report: %s", filepath.Join(reportDir, "report.json"))
	}

	// Print the full matrix as a single block for CI readability.
	var b strings.Builder
	b.WriteString("\nGCLOUD CLI CONFORMANCE MATRIX\n")
	fmt.Fprintf(&b, "%-10s %-11s %-11s %-5s %s\n", "SERVICE", "STATUS", "EXPECTED", "EXIT", "COMMAND")
	for _, r := range results {
		fmt.Fprintf(&b, "%-10s %-11s %-11s %-5d gcloud %s\n",
			r.Service, r.Status, r.Expected, r.ExitCode, strings.Join(r.Args, " "))
	}
	fmt.Fprintf(&b, "summary: total=%d pass=%d fail=%d unsupported=%d regressions=%d\n",
		rep.Summary.Total, rep.Summary.Pass, rep.Summary.Fail, rep.Summary.Unsupported, rep.Summary.Regressions)
	t.Log(b.String())
}

// classify maps a raw run to a status and decides whether it is a regression.
// A regression is an expected-pass command that did not pass; documented gaps
// (ExpectFail/ExpectUnsupported) never are.
func classify(c Command, res Result) (status Status, regression bool, why string) {
	if res.Err != nil {
		return StatusFail, c.Expect == ExpectPass, fmt.Sprintf("could not run gcloud: %v", res.Err)
	}
	if res.TimedOut {
		return StatusFail, c.Expect == ExpectPass, "timed out"
	}
	if res.ExitCode == 0 {
		if c.Assert != nil {
			if err := c.Assert(res.Stdout); err != nil {
				return StatusFail, c.Expect == ExpectPass,
					fmt.Sprintf("exit 0 but assertion failed: %v", err)
			}
		}
		return StatusPass, false, ""
	}
	// Non-zero exit: usage/flag errors are a gcloud-side limitation, not an
	// emulator regression, so they surface as unsupported.
	if c.Expect == ExpectUnsupported || looksUnsupported(res.Stderr) {
		return StatusUnsupported, false, firstErrorLine(res.Stderr)
	}
	return StatusFail, c.Expect == ExpectPass, firstErrorLine(res.Stderr)
}

// looksUnsupported reports whether stderr indicates gcloud itself rejects the
// invocation (unknown command/flag) rather than the emulator answering badly.
func looksUnsupported(stderr string) bool {
	markers := []string{
		"unrecognized arguments",
		"Invalid choice",
		"Maybe you meant",
		"Unknown command",
		"unknown command",
	}
	for _, m := range markers {
		if strings.Contains(stderr, m) {
			return true
		}
	}
	return false
}

// firstErrorLine extracts the most informative one-line error from stderr.
func firstErrorLine(stderr string) string {
	lines := strings.Split(strings.TrimSpace(stderr), "\n")
	for _, ln := range lines {
		ln = strings.TrimSpace(ln)
		if strings.Contains(ln, "ERROR:") {
			return ln
		}
	}
	for _, ln := range lines {
		if ln = strings.TrimSpace(ln); ln != "" {
			return ln
		}
	}
	return "command failed with no stderr"
}

// mergeEnv returns base with every CLOUDSDK_*/Google credential variable
// removed and the overrides appended, so ambient shell configuration cannot
// leak into the conformance run.
func mergeEnv(base, overrides []string) []string {
	drop := func(key string) bool {
		switch {
		case strings.HasPrefix(key, "CLOUDSDK_"):
			return true
		case key == "GOOGLE_APPLICATION_CREDENTIALS",
			key == "GOOGLE_CLOUD_PROJECT",
			key == "GOOGLE_CLOUD_QUOTA_PROJECT",
			key == "STORAGE_EMULATOR_HOST":
			return true
		default:
			return false
		}
	}
	out := make([]string, 0, len(base)+len(overrides))
	for _, kv := range base {
		key := kv
		if i := strings.IndexByte(kv, '='); i >= 0 {
			key = kv[:i]
		}
		if !drop(key) {
			out = append(out, kv)
		}
	}
	return append(out, overrides...)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// emulatorReachable probes the emulator's health endpoint. A non-2xx or a
// connection error means "skip", not "fail".
func emulatorReachable(endpoint string) bool {
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(strings.TrimRight(endpoint, "/") + "/_jaiscloud/health")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

func gcloudVersion(bin string, env []string) string {
	cmd := exec.Command(bin, "--version")
	cmd.Env = env
	out, err := cmd.Output()
	if err != nil {
		return "unknown"
	}
	if i := strings.IndexByte(string(out), '\n'); i >= 0 {
		return strings.TrimSpace(string(out[:i]))
	}
	return strings.TrimSpace(string(out))
}
