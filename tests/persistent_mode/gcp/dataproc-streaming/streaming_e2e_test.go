//go:build dataproc_streaming_e2e

// Package dataprocstreaming_test is the real-Kubernetes Structured Streaming
// smoke for the emulator's Dataproc API. It is the cluster-level complement to
// the deterministic fake-clientset gate (`make test-dataproc-streaming`):
//
//   - create a bucket and stage a PySpark streaming driver to gs://
//   - create a Dataproc cluster (the deployed emulator runs client-mode Spark
//     pods on the k3d cluster)
//   - submit a pysparkJob whose driver runs readStream.format("rate") with a
//     gs:// checkpointLocation and a gs:// file sink
//   - assert the job stays RUNNING (the driver never exits) and micro-batches
//     advance under the checkpoint/sink
//   - cancel the job, assert it reaches CANCELLED, and assert the driver
//     k8s Job/pod is actually reaped (not left running)
//
// Run with:
//
//	make test-dataproc-streaming-k8s
//
// It is inert without a k3d cluster: requireK3d skips when kubectl or the
// jaiscloud-gcp Service is absent.
package dataprocstreaming_test

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
	"strings"
	"testing"
	"time"
)

const (
	testProject = "jaiscloud-project"
	testRegion  = "us-central1"
)

func namespace() string {
	if v := os.Getenv("K8S_NAMESPACE"); v != "" {
		return v
	}
	return "jaiscloud"
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
// is reachable in the target namespace.
func requireK3d(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("kubectl"); err != nil {
		t.Skip("kubectl not found — skipping k3d Dataproc streaming smoke")
	}
	if _, err := kubectl("-n", namespace(), "get", "svc", "jaiscloud-gcp"); err != nil {
		t.Skipf("svc/jaiscloud-gcp not reachable in namespace %q (apply deploy/k8s/jaiscloud-gcp.yaml): %v", namespace(), err)
	}
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

var httpClient = &http.Client{Timeout: 60 * time.Second}

// api performs a JSON request and returns the status code and decoded body
// (empty map for an empty body). Non-2xx is not fatal; callers assert.
func api(t *testing.T, method, rawURL string, body any) (int, map[string]any) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		rd = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, rawURL, rd)
	if err != nil {
		t.Fatalf("build request %s %s: %v", method, rawURL, err)
	}
	if rd != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, rawURL, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	out := map[string]any{}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &out)
	}
	return resp.StatusCode, out
}

func mustOK(t *testing.T, verb string, code int, body map[string]any) map[string]any {
	t.Helper()
	if code < 200 || code >= 300 {
		t.Fatalf("%s: HTTP %d: %v", verb, code, body)
	}
	return body
}

// ─── GCS staging (JSON API) ────────────────────────────────────────────────

func ensureBucket(t *testing.T, base, bucket string) {
	t.Helper()
	code, body := api(t, http.MethodPost, base+"/storage/v1/b?project="+url.QueryEscape(testProject),
		map[string]any{"name": bucket})
	// 409 == already exists (idempotent).
	if code >= 300 && code != http.StatusConflict {
		t.Fatalf("create bucket %s: HTTP %d: %v", bucket, code, body)
	}
}

func uploadText(t *testing.T, base, bucket, name, content string) {
	t.Helper()
	q := url.Values{"uploadType": {"media"}, "name": {name}}.Encode()
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/upload/storage/v1/b/%s/o?%s", base, bucket, q),
		strings.NewReader(content))
	if err != nil {
		t.Fatalf("build upload: %v", err)
	}
	req.Header.Set("Content-Type", "text/x-python")
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("upload %s/%s: %v", bucket, name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("upload %s/%s: HTTP %d: %s", bucket, name, resp.StatusCode, raw)
	}
}

// listObjects returns the object names under bucket/prefix.
func listObjects(t *testing.T, base, bucket, prefix string) []string {
	t.Helper()
	u := fmt.Sprintf("%s/storage/v1/b/%s/o", base, bucket)
	if prefix != "" {
		u += "?prefix=" + url.QueryEscape(prefix)
	}
	code, body := api(t, http.MethodGet, u, nil)
	if code >= 300 {
		t.Fatalf("list %s/%s: HTTP %d: %v", bucket, prefix, code, body)
	}
	items, _ := body["items"].([]any)
	names := make([]string, 0, len(items))
	for _, it := range items {
		if m, ok := it.(map[string]any); ok {
			if n, ok := m["name"].(string); ok {
				names = append(names, n)
			}
		}
	}
	return names
}

func deleteBucketObjects(t *testing.T, base, bucket string) {
	t.Helper()
	for _, name := range listObjects(t, base, bucket, "") {
		code, _ := api(t, http.MethodDelete,
			fmt.Sprintf("%s/storage/v1/b/%s/o/%s", base, bucket, url.PathEscape(name)), nil)
		if code >= 300 && code != http.StatusNotFound {
			t.Logf("cleanup: delete %s/%s: HTTP %d", bucket, name, code)
		}
	}
}

// ─── Dataproc wiring (REST) ────────────────────────────────────────────────

func clusterPath(name string) string {
	return fmt.Sprintf("/v1/projects/%s/regions/%s/clusters/%s", testProject, testRegion, name)
}

func jobPath(id string) string {
	return fmt.Sprintf("/v1/projects/%s/regions/%s/jobs/%s", testProject, testRegion, id)
}

// createCluster submits a cluster create and polls its async LRO to RUNNING.
func createCluster(t *testing.T, base, name string) {
	t.Helper()
	// Best-effort delete of a prior run so create does not 409.
	code, body := api(t, http.MethodDelete, base+clusterPath(name), nil)
	if code == http.StatusOK {
		pollOperation(t, base, body)
	}
	code, body = api(t, http.MethodPost, base+fmt.Sprintf("/v1/projects/%s/regions/%s/clusters", testProject, testRegion),
		map[string]any{
			"clusterName": name,
			"config":      map[string]any{"gceClusterConfig": map[string]any{"zoneUri": testRegion + "-a"}},
		})
	mustOK(t, "create cluster "+name, code, body)
	if done, _ := body["done"].(bool); !done {
		pollOperation(t, base, body)
	}
	code, cl := api(t, http.MethodGet, base+clusterPath(name), nil)
	mustOK(t, "get cluster "+name, code, cl)
	if got := strField(cl, "status", "state"); got != "RUNNING" {
		t.Fatalf("cluster %s state = %q, want RUNNING: %v", name, got, cl)
	}
}

func deleteCluster(t *testing.T, base, name string) {
	t.Helper()
	code, body := api(t, http.MethodDelete, base+clusterPath(name), nil)
	if code >= 300 && code != http.StatusNotFound {
		t.Logf("cleanup: delete cluster %s: HTTP %d: %v", name, code, body)
		return
	}
	if code == http.StatusOK {
		if done, _ := body["done"].(bool); !done {
			pollOperation(t, base, body)
		}
	}
}

// pollOperation polls a Dataproc LRO (by resource name) until done.
func pollOperation(t *testing.T, base string, op map[string]any) map[string]any {
	t.Helper()
	name, _ := op["name"].(string)
	if name == "" {
		// Already-completed inline response with no operation to poll.
		return op
	}
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		code, cur := api(t, http.MethodGet, base+"/v1/"+name, nil)
		if code >= 300 {
			t.Fatalf("poll operation %s: HTTP %d: %v", name, code, cur)
		}
		if done, _ := cur["done"].(bool); done {
			resp, _ := cur["response"].(map[string]any)
			if resp == nil {
				return cur
			}
			return resp
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("operation %s did not complete within 120s", name)
	return nil
}

// strField navigates a nested map path returning the string value, or "".
func strField(m map[string]any, path ...string) string {
	var cur any = m
	for _, k := range path {
		mm, ok := cur.(map[string]any)
		if !ok {
			return ""
		}
		cur = mm[k]
	}
	s, _ := cur.(string)
	return s
}

// streamingScript is the PySpark Structured Streaming driver staged to GCS. It
// reads the built-in rate source (no external connector) and writes JSON parts
// to a gs:// file sink with a gs:// checkpoint, blocking forever in
// awaitTermination so the Dataproc job stays RUNNING until cancelled.
func streamingScript(bucket string) string {
	return fmt.Sprintf(`from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.readStream.format("rate").option("rowsPerSecond", 5).load()
out = df.withColumn("v", F.col("value").cast("string"))
(
    out.writeStream.format("json")
    .option("path", "gs://%s/sink")
    .option("checkpointLocation", "gs://%s/checkpoint")
    .trigger(processingTime="2 seconds")
    .start()
    .awaitTermination()
)
`, bucket, bucket)
}

// submitStreamingJob submits the pysparkJob and returns the job id.
func submitStreamingJob(t *testing.T, base, bucket, cluster, jobID string) {
	t.Helper()
	body := map[string]any{
		"job": map[string]any{
			"reference": map[string]any{"jobId": jobID},
			"placement": map[string]any{"clusterName": cluster},
			"pysparkJob": map[string]any{
				"mainPythonFileUri": fmt.Sprintf("gs://%s/stream.py", bucket),
				// A spark.*.streaming.* property marks the job long-running for
				// the emulator's lifecycle model; the stream itself blocks in
				// awaitTermination, which is what actually keeps it RUNNING.
				"properties": map[string]any{
					"spark.sql.streaming.checkpointLocation": fmt.Sprintf("gs://%s/checkpoint", bucket),
					"spark.driver.memory":                    "512m",
					"spark.executor.memory":                  "512m",
					"spark.executor.instances":               "1",
					"spark.sql.shuffle.partitions":           "1",
				},
			},
		},
	}
	code, resp := api(t, http.MethodPost, base+fmt.Sprintf("/v1/projects/%s/regions/%s/jobs:submit", testProject, testRegion), body)
	mustOK(t, "submit job "+jobID, code, resp)
	if _, ok := resp["pysparkJob"]; !ok {
		t.Fatalf("submit response has no pysparkJob: %v", resp)
	}
}

func jobState(t *testing.T, base, jobID string) string {
	t.Helper()
	code, resp := api(t, http.MethodGet, base+jobPath(jobID), nil)
	mustOK(t, "get job "+jobID, code, resp)
	return strField(resp, "status", "state")
}

// driverJobNames returns the names of the client-mode k8s Jobs for a job id. A
// listing error (kubectl/API unavailable) is returned, never folded into an
// empty result, so the reap assertion cannot pass spuriously.
func driverJobNames(t *testing.T, jobID string) ([]string, error) {
	t.Helper()
	out, err := kubectl("-n", namespace(), "get", "jobs",
		"-l", "jaiscloud.io/job-id="+jobID,
		"-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}")
	if err != nil {
		return nil, err
	}
	var names []string
	for _, l := range strings.Split(strings.TrimSpace(out), "\n") {
		if l = strings.TrimSpace(l); l != "" {
			names = append(names, l)
		}
	}
	return names, nil
}

// driverPodNames returns the driver/executor pod names labeled for a job id.
func driverPodNames(t *testing.T, jobID string) []string {
	t.Helper()
	out, err := kubectl("-n", namespace(), "get", "pods",
		"-l", "jaiscloud.io/job-id="+jobID,
		"-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}")
	if err != nil {
		return nil
	}
	var names []string
	for _, l := range strings.Split(strings.TrimSpace(out), "\n") {
		if l = strings.TrimSpace(l); l != "" {
			names = append(names, l)
		}
	}
	return names
}

func TestDataprocStreamingK3d(t *testing.T) {
	requireK3d(t)

	base, stop := startPortForward(t)
	// Register the port-forward teardown first so it runs last (t.Cleanup is
	// LIFO): the resource cleanups below still need the forward.
	t.Cleanup(stop)

	// Unique per run so re-runs never collide with a prior run's bucket,
	// cluster or job id in the shared Postgres-backed store.
	run := fmt.Sprintf("%d", time.Now().Unix())
	bucket := "stream-smoke-" + run
	cluster := "stream-cluster-" + run
	jobID := "stream-job-" + run

	ensureBucket(t, base, bucket)
	t.Cleanup(func() { deleteBucketObjects(t, base, bucket) })
	uploadText(t, base, bucket, "stream.py", streamingScript(bucket))

	createCluster(t, base, cluster)
	t.Cleanup(func() { deleteCluster(t, base, cluster) })

	submitStreamingJob(t, base, bucket, cluster, jobID)

	// The driver pod starts, so the job walks PENDING -> SETUP_DONE -> RUNNING
	// and then stays RUNNING (the driver never exits).
	waitForState(t, base, jobID, "RUNNING", 5*time.Minute)

	// Assert it does not auto-settle, and that micro-batches advance under the
	// gs:// checkpoint and sink while it runs.
	var commitObjects, sinkObjects []string
	deadline := time.Now().Add(6 * time.Minute)
	for time.Now().Before(deadline) {
		if got := jobState(t, base, jobID); got != "RUNNING" {
			t.Fatalf("streaming job left RUNNING for %q before cancel", got)
		}
		commitObjects = listObjects(t, base, bucket, "checkpoint/commits/")
		sinkObjects = listObjects(t, base, bucket, "sink/")
		if len(commitObjects) > 0 && hasCommittedPartFile(sinkObjects) {
			break
		}
		time.Sleep(3 * time.Second)
	}
	if len(commitObjects) == 0 {
		t.Fatalf("no checkpoint commits under gs://%s/checkpoint/commits/ — micro-batches did not advance", bucket)
	}
	if !hasCommittedPartFile(sinkObjects) {
		t.Fatalf("no committed sink part files under gs://%s/sink/ — streaming output did not land (objects=%v)", bucket, sinkObjects)
	}
	t.Logf("streaming advanced: checkpoint commits=%v sink=%v", commitObjects, sinkObjects)

	// The job must still be RUNNING now that output exists (a batch job would
	// have reached DONE).
	if got := jobState(t, base, jobID); got != "RUNNING" {
		t.Fatalf("job state = %q after micro-batches, want RUNNING", got)
	}

	// Cancel: the store settles CANCEL_PENDING -> CANCEL_STARTED -> CANCELLED,
	// and the driver k8s Job/pod must be reaped (not left running).
	code, resp := api(t, http.MethodPost, base+jobPath(jobID)+":cancel", nil)
	mustOK(t, "cancel job "+jobID, code, resp)
	waitForState(t, base, jobID, "CANCELLED", 2*time.Minute)

	waitDriverReaped(t, jobID, 90*time.Second)
}

func hasCommittedPartFile(names []string) bool {
	for _, n := range names {
		if strings.Contains(n, "part-") && !strings.Contains(n, "_temporary/") {
			return true
		}
	}
	return false
}

// waitForState polls a job until it reports want (failing fast on a terminal
// mismatch).
func waitForState(t *testing.T, base, jobID, want string, timeout time.Duration) {
	t.Helper()
	terminal := map[string]bool{"DONE": true, "ERROR": true, "CANCELLED": true}
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		last = jobState(t, base, jobID)
		if last == want {
			return
		}
		if terminal[last] && last != want {
			t.Fatalf("job %s reached terminal state %q, want %q", jobID, last, want)
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("job %s did not reach %q within %s (last %q)", jobID, want, timeout, last)
}

// waitDriverReaped asserts the client-mode k8s Job for jobID is gone after
// cancel. A listing error is fatal: "could not list" must never be mistaken for
// "reaped". k8shelpers.Cancel uses foreground deletion, so the Job (and its pod)
// is only removed once the driver has actually stopped; any remaining pod is
// logged.
func waitDriverReaped(t *testing.T, jobID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		names, err := driverJobNames(t, jobID)
		if err != nil {
			lastErr = err
			time.Sleep(2 * time.Second)
			continue
		}
		lastErr = nil
		if len(names) == 0 {
			if pods := driverPodNames(t, jobID); len(pods) > 0 {
				t.Logf("driver Job reaped but pod(s) still present (may be terminating): %v", pods)
			}
			return
		}
		time.Sleep(2 * time.Second)
	}
	if lastErr != nil {
		t.Fatalf("could not confirm the driver k8s Job reap for job %s: %v", jobID, lastErr)
	}
	t.Fatalf("cancel did not reap the driver k8s Job for job %s", jobID)
}
