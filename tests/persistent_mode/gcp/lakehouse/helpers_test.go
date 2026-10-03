//go:build lakehouse_e2e

package lakehouse_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	jobName         = "lakehouse-pipeline"
	publishedBucket = "lakehouse-published"
)

func namespace() string {
	if v := os.Getenv("K8S_NAMESPACE"); v != "" {
		return v
	}
	return "jaiscloud"
}

// repoRoot resolves the checkout root from this source file, so the test can
// apply the pipeline manifest regardless of the working directory.
func repoRoot() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", ".."))
}

func manifestPath() string {
	if v := os.Getenv("LAKEHOUSE_MANIFEST"); v != "" {
		return v
	}
	return filepath.Join(repoRoot(), "deploy", "k8s", "lakehouse", "pipeline.yaml")
}

// kubectl runs kubectl and returns stdout (stderr folded into the error).
func kubectl(args ...string) (string, error) {
	cmd := exec.Command("kubectl", args...)
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		return out.String(), fmt.Errorf("kubectl %s: %w\n%s", strings.Join(args, " "), err, strings.TrimSpace(errb.String()))
	}
	return out.String(), nil
}

// requireK3d skips the test unless kubectl is present and the emulator Service
// is reachable in the target namespace. This keeps the suite inert on machines
// without a k3d cluster (CI, plain `go test ./...`).
func requireK3d(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("kubectl"); err != nil {
		t.Skip("kubectl not found — skipping k3d Lakehouse e2e")
	}
	if _, err := kubectl("-n", namespace(), "get", "svc", "jaiscloud-gcp"); err != nil {
		t.Skipf("svc/jaiscloud-gcp not reachable in namespace %q (apply deploy/k8s/jaiscloud-gcp.yaml): %v", namespace(), err)
	}
}

// deleteJob removes a prior run so `kubectl wait` observes this run's status.
func deleteJob(t *testing.T) {
	t.Helper()
	if out, err := kubectl("-n", namespace(), "delete", "job", jobName, "--ignore-not-found"); err != nil {
		t.Fatalf("delete job: %v\n%s", err, out)
	}
}

// records is the seed size the run should use. Overridable via
// LAKEHOUSE_RECORDS (the Makefile forwards it from its LAKEHOUSE_RECORDS var).
func records() int {
	if v := os.Getenv("LAKEHOUSE_RECORDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 100
}

// recordsEnv matches the Job's RECORDS env value so the manifest can be
// rendered with the requested count before it is applied.
var recordsEnv = regexp.MustCompile(`(?m)(- name: RECORDS\n\s+value: )"[0-9]+"`)

// applyPipeline applies the manifest with the RECORDS env rendered to
// records(), so the pipeline ingests exactly what the assertions expect.
func applyPipeline(t *testing.T) {
	t.Helper()
	raw, err := os.ReadFile(manifestPath())
	if err != nil {
		t.Fatalf("read manifest %s: %v", manifestPath(), err)
	}
	if !recordsEnv.Match(raw) {
		t.Fatalf("RECORDS env not found in %s", manifestPath())
	}
	want := fmt.Sprintf(`${1}"%d"`, records())
	rendered := recordsEnv.ReplaceAll(raw, []byte(want))
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = bytes.NewReader(rendered)
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		t.Fatalf("apply pipeline manifest: %v\n%s%s", err, out.String(), errb.String())
	}
}

// waitJobComplete polls until the Job succeeds, failing fast (with logs) once
// it enters a terminal failure. `kubectl wait --for=condition=complete` would
// instead block for the full timeout on a BackoffLimitExceeded Job.
func waitJobComplete(t *testing.T, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for {
		out, err := kubectl("-n", namespace(), "get", "job", jobName,
			"-o", "jsonpath={.status.succeeded}|{.status.failed}")
		if err == nil {
			last = strings.TrimSpace(out)
			fields := strings.Split(last, "|")
			failed := ""
			if len(fields) > 1 {
				failed = fields[1]
			}
			if fields[0] == "1" {
				return
			}
			if failed != "" && failed != "0" {
				logs, _ := kubectl("-n", namespace(), "logs", "job/"+jobName, "--tail=80")
				t.Fatalf("pipeline job failed (failed=%s)\n--- job logs ---\n%s", failed, logs)
			}
		}
		if !time.Now().Before(deadline) {
			logs, _ := kubectl("-n", namespace(), "logs", "job/"+jobName, "--tail=80")
			t.Fatalf("pipeline job did not complete within %s (last status: %s)\n--- job logs ---\n%s",
				timeout, last, logs)
		}
		time.Sleep(3 * time.Second)
	}
}

func jobLogs(t *testing.T) string {
	t.Helper()
	out, err := kubectl("-n", namespace(), "logs", "job/"+jobName)
	if err != nil {
		t.Fatalf("read job logs: %v", err)
	}
	return out
}

// summary is the JSON line the pipeline prints on success.
type summary struct {
	OK                 bool           `json:"PIPELINE_OK"`
	Pipeline           string         `json:"pipeline"`
	Stages             []string       `json:"stages"`
	InputRows          int            `json:"input_rows"`
	IngestRegionCounts map[string]int `json:"ingest_region_counts"`
	CuratedObjects     []string       `json:"curated_objects"`
	PublishedObjects   []string       `json:"published_objects"`
	Cluster            string         `json:"cluster"`
	MetastoreService   string         `json:"metastore_service"`
	// JobTypes maps each Spark stage to the Dataproc job type it ran as, so the
	// test can prove the Derive hop took the sparkSqlJob path.
	JobTypes      map[string]string `json:"job_types"`
	DriverOutputs map[string]struct {
		URI   string `json:"uri"`
		Bytes int    `json:"bytes"`
	} `json:"driver_outputs"`
}

// testProject/testRegion mirror the PROJECT/REGION env the pipeline manifest
// renders (deploy/k8s/lakehouse/pipeline.yaml).
const (
	testProject = "jaiscloud-project"
	testRegion  = "us-central1"
)

// parseSummary extracts the single {"PIPELINE_OK": ...} JSON object from logs.
func parseSummary(t *testing.T, logs string) summary {
	t.Helper()
	for _, line := range strings.Split(logs, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "{") || !strings.Contains(line, "PIPELINE_OK") {
			continue
		}
		var s summary
		if err := json.Unmarshal([]byte(line), &s); err != nil {
			t.Fatalf("parse pipeline summary %q: %v", line, err)
		}
		return s
	}
	t.Fatalf("no PIPELINE_OK summary in job logs:\n%s", logs)
	return summary{}
}

// ─── emulator access (host-side, via port-forward) ──────────────────────────

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// startPortForward forwards the emulator Service to a free local port and
// returns its base URL plus a stop function.
func startPortForward(t *testing.T) (string, func()) {
	t.Helper()
	port := freePort(t)
	cmd := exec.Command("kubectl", "-n", namespace(), "port-forward",
		"svc/jaiscloud-gcp", fmt.Sprintf("%d:8080", port))
	var errb bytes.Buffer
	cmd.Stderr = &errb
	if err := cmd.Start(); err != nil {
		t.Fatalf("start port-forward: %v", err)
	}
	stop := func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	}
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if resp, err := http.Get(base + "/_jaiscloud/health"); err == nil {
			resp.Body.Close()
			return base, stop
		}
		time.Sleep(300 * time.Millisecond)
	}
	stop()
	t.Fatalf("port-forward to svc/jaiscloud-gcp never became ready: %s", strings.TrimSpace(errb.String()))
	return "", func() {}
}

// getObject reads a GCS object (JSON API alt=media) through the emulator.
func getObject(t *testing.T, base, bucket, name string) []byte {
	t.Helper()
	u := fmt.Sprintf("%s/storage/v1/b/%s/o/%s?alt=media", base, bucket, url.PathEscape(name))
	resp, err := http.Get(u)
	if err != nil {
		t.Fatalf("get %s/%s: %v", bucket, name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get %s/%s: HTTP %d", bucket, name, resp.StatusCode)
	}
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s/%s: %v", bucket, name, err)
	}
	return b
}

// getJSON fetches a JSON object from the emulator via the port-forward.
func getJSON(t *testing.T, base, path string) map[string]any {
	t.Helper()
	resp, err := http.Get(base + path)
	if err != nil {
		t.Fatalf("get %s: %v", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get %s: HTTP %d", path, resp.StatusCode)
	}
	var m map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return m
}
