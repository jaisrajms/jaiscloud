//go:build gcp_persistence

// Package dataproc_test verifies that Cloud Dataproc clusters and jobs survive
// an end-to-end export → import round trip through the /_jaiscloud admin
// endpoints when jaiscloud-gcp is backed by PostgreSQL (--dsn).
//
// Required env:
//
//	JAISCLOUD_DSN — PostgreSQL DSN
//
// Optional env:
//
//	JAISCLOUD_GCP_BIN                  — path to the jaiscloud-gcp binary
//	JAISCLOUD_GCP_DATAPROC_PERSIST_PORT — port for the managed server (default 8098)
package dataproc_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"jaiscloud/internal/clock"
)

func gcpBin() string {
	if b := os.Getenv("JAISCLOUD_GCP_BIN"); b != "" {
		return b
	}
	const rel = "../../../../jaiscloud-gcp"
	if _, err := os.Stat(rel); err == nil {
		return rel
	}
	return "jaiscloud-gcp"
}

func persistPort() int {
	if v := os.Getenv("JAISCLOUD_GCP_DATAPROC_PERSIST_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			return p
		}
	}
	return 8098
}

type testWriter struct {
	t      *testing.T
	prefix string
	buf    string
}

func (w *testWriter) Write(p []byte) (int, error) {
	w.buf += string(p)
	for {
		idx := strings.IndexByte(w.buf, '\n')
		if idx < 0 {
			break
		}
		w.t.Log(w.prefix + w.buf[:idx])
		w.buf = w.buf[idx+1:]
	}
	return len(p), nil
}

func startGCPProcess(t *testing.T, port int, dsn, blobDir string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(gcpBin(),
		"start",
		"--port", strconv.Itoa(port),
		"--dsn", dsn,
		"--blob-dir", blobDir,
		"--log-level", "warn",
	)
	cmd.Stdout = &testWriter{t: t, prefix: fmt.Sprintf("jaiscloud-gcp[%d]: ", port)}
	cmd.Stderr = &testWriter{t: t, prefix: fmt.Sprintf("jaiscloud-gcp[%d] ERR: ", port)}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start jaiscloud-gcp on port %d: %v", port, err)
	}
	return cmd
}

func waitForHealth(t *testing.T, host string) {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := clock.RealNow().Add(30 * time.Second)
	for clock.RealNow().Before(deadline) {
		resp, err := client.Get(host + "/_jaiscloud/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("jaiscloud-gcp at %s did not become healthy within 30s", host)
}

// doRequest performs an HTTP request and returns the status code and raw body
// bytes (binary-safe, unlike a string reader).
func doRequest(t *testing.T, host, method, path string, body []byte, contentType string) (int, []byte) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, host+path, rd)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("%s %s read: %v", method, path, err)
	}
	return resp.StatusCode, data
}

func stopProcess(t *testing.T, cmd *exec.Cmd, port int) {
	t.Helper()
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill jaiscloud-gcp: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		c, err := http.Get(fmt.Sprintf("http://localhost:%d/_jaiscloud/health", port))
		if err != nil {
			return
		}
		c.Body.Close()
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("jaiscloud-gcp did not release the port after kill")
}

// jsonObj decodes a JSON response body into a map.
func jsonObj(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("decode JSON %q: %v", body, err)
	}
	return m
}

// strField navigates a nested map path (e.g. "status", "state") returning the
// string value, or "" when any segment is absent.
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

// pollJobTerminal polls a job until it reaches a terminal state, driving the
// emulator's lazy job state machine one hop per read.
func pollJobTerminal(t *testing.T, host, base, jobID string) map[string]any {
	t.Helper()
	deadline := clock.RealNow().Add(15 * time.Second)
	for clock.RealNow().Before(deadline) {
		code, body := doRequest(t, host, "GET", base+"/jobs/"+jobID, nil, "")
		if code != http.StatusOK {
			t.Fatalf("get job %s: got HTTP %d body %s", jobID, code, body)
		}
		obj := jsonObj(t, body)
		switch strField(obj, "status", "state") {
		case "DONE", "ERROR", "CANCELLED":
			return obj
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("job %s did not reach a terminal state", jobID)
	return nil
}

// TestDataprocExportImportRoundTrip creates a cluster and two jobs (a mock-mode
// sparkJob polled to DONE and an unsupported hiveJob that errors), exports the
// full instance state, and imports it back — asserting the terminal job state
// and status.details survive byte-for-byte.
func TestDataprocExportImportRoundTrip(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping dataproc export/import persistence test")
	}

	port := persistPort()
	host := fmt.Sprintf("http://localhost:%d", port)
	blobDir := t.TempDir()

	proc := startGCPProcess(t, port, dsn, blobDir)
	defer stopProcess(t, proc, port)
	waitForHealth(t, host)

	// Start from a clean slate (the shared DSN may carry state from prior runs).
	if code, body := doRequest(t, host, "POST", "/_jaiscloud/reset", nil, "application/json"); code != http.StatusOK {
		t.Fatalf("reset: got HTTP %d body %s", code, body)
	}

	const project = "proj"
	const region = "us-central1"
	const clusterName = "exp-cluster"
	const doneJob = "exp-job-done"
	const sqlJob = "exp-job-sql"
	const errJob = "exp-job-error"
	base := "/v1/projects/" + project + "/regions/" + region

	// ── Create cluster (async LRO: the create returns with done=false and the
	// lazy state machine settles it to RUNNING when the operation is polled) ──
	code, body := doRequest(t, host, "POST", base+"/clusters",
		[]byte(`{"projectId":"`+project+`","clusterName":"`+clusterName+`","config":{"gceClusterConfig":{"zoneUri":"us-central1-a"}}}`),
		"application/json")
	if code != http.StatusOK {
		t.Fatalf("create cluster: got HTTP %d body %s", code, body)
	}
	createOp := jsonObj(t, body)
	if done, _ := createOp["done"].(bool); done {
		t.Fatalf("create cluster completed inline; want an in-flight LRO: %s", body)
	}
	if cl := pollOperationDone(t, host, createOp); cl == nil {
		t.Fatalf("create operation packed no cluster response: %s", body)
	} else if state := strField(cl, "status", "state"); state != "RUNNING" {
		t.Fatalf("cluster state after create LRO = %q, want RUNNING", state)
	}

	// Delete is asynchronous too: create a throwaway cluster and poll its delete
	// operation to completion, then assert the record is gone (NotFound).
	const delClusterName = "exp-del"
	code, body = doRequest(t, host, "POST", base+"/clusters",
		[]byte(`{"projectId":"`+project+`","clusterName":"`+delClusterName+`","config":{}}`),
		"application/json")
	if code != http.StatusOK {
		t.Fatalf("create delete-target cluster: got HTTP %d body %s", code, body)
	}
	pollOperationDone(t, host, jsonObj(t, body))
	code, body = doRequest(t, host, "DELETE", base+"/clusters/"+delClusterName, nil, "")
	if code != http.StatusOK {
		t.Fatalf("delete cluster: got HTTP %d body %s", code, body)
	}
	delOp := jsonObj(t, body)
	if done, _ := delOp["done"].(bool); done {
		t.Fatalf("delete cluster completed inline; want an in-flight LRO: %s", body)
	}
	pollOperationDone(t, host, delOp)
	if code, body = doRequest(t, host, "GET", base+"/clusters/"+delClusterName, nil, ""); code != http.StatusNotFound {
		t.Fatalf("get deleted cluster: got HTTP %d body %s, want 404", code, body)
	}

	// ── Create the Metastore service, then a GKE-backed cluster
	// (virtualClusterConfig) that attaches it ────────────────────────────────
	const metastoreService = "exp-hms"
	msName := "projects/" + project + "/locations/" + region + "/services/" + metastoreService
	code, body = doRequest(t, host, "POST",
		"/v1/projects/"+project+"/locations/"+region+"/services?serviceId="+metastoreService,
		[]byte(`{"hiveMetastoreConfig":{"endpointProtocol":"THRIFT"}}`), "application/json")
	if code != http.StatusOK {
		t.Fatalf("create metastore service: got HTTP %d body %s", code, body)
	}

	const gkeClusterName = "exp-gke"
	vccBody := `{"projectId":"` + project + `","clusterName":"` + gkeClusterName + `",` +
		`"virtualClusterConfig":{"kubernetesClusterConfig":{"gkeClusterConfig":{"gkeClusterTarget":"projects/proj/locations/us-central1/clusters/gke-1"}},` +
		`"auxiliaryServicesConfig":{"metastoreConfig":{"dataprocMetastoreService":"` + msName + `"}}}}`
	code, body = doRequest(t, host, "POST", base+"/clusters", []byte(vccBody), "application/json")
	if code != http.StatusOK {
		t.Fatalf("create GKE cluster: got HTTP %d body %s", code, body)
	}
	if cl := pollOperationDone(t, host, jsonObj(t, body)); cl == nil {
		t.Fatalf("GKE create operation packed no cluster response")
	} else if state := strField(cl, "status", "state"); state != "RUNNING" {
		t.Fatalf("GKE cluster state after create LRO = %q, want RUNNING", state)
	}

	// ── Submit a mock-mode Spark job (walks to DONE as it is polled) ─────────
	code, body = doRequest(t, host, "POST", base+"/jobs:submit",
		[]byte(`{"job":{"reference":{"projectId":"`+project+`","jobId":"`+doneJob+`"},"placement":{"clusterName":"`+clusterName+`"},"sparkJob":{"mainJarFileUri":"gs://b/a.jar","mainClass":"Main"}}}`),
		"application/json")
	if code != http.StatusOK {
		t.Fatalf("submit spark job: got HTTP %d body %s", code, body)
	}
	if state := strField(jsonObj(t, body), "status", "state"); state != "PENDING" {
		t.Fatalf("expected PENDING spark job, got state %q body %s", state, body)
	}
	if state := strField(pollJobTerminal(t, host, base, doneJob), "status", "state"); state != "DONE" {
		t.Fatalf("expected DONE spark job, got state %q", state)
	}

	// ── The terminal job's driver output resolves to a real GCS object ───────
	code, body = doRequest(t, host, "GET", base+"/jobs/"+doneJob, nil, "")
	if code != http.StatusOK {
		t.Fatalf("get done job: got HTTP %d body %s", code, body)
	}
	doneObj := jsonObj(t, body)
	if uri := strField(doneObj, "driverControlFilesUri"); uri == "" {
		t.Fatalf("done job has no driverControlFilesUri: %s", body)
	}
	driverOutputObject(t, host, strField(doneObj, "driverOutputResourceUri"), doneJob)

	// ── Submit a mock-mode sparkSqlJob (now supported; walks to DONE) ─────────
	code, body = doRequest(t, host, "POST", base+"/jobs:submit",
		[]byte(`{"job":{"reference":{"projectId":"`+project+`","jobId":"`+sqlJob+`"},"placement":{"clusterName":"`+clusterName+`"},"sparkSqlJob":{"queryList":{"queries":["SELECT 1"]}}}}`),
		"application/json")
	if code != http.StatusOK {
		t.Fatalf("submit sparkSql job: got HTTP %d body %s", code, body)
	}
	if state := strField(jsonObj(t, body), "status", "state"); state != "PENDING" {
		t.Fatalf("expected PENDING sparkSql job, got state %q body %s", state, body)
	}
	if state := strField(pollJobTerminal(t, host, base, sqlJob), "status", "state"); state != "DONE" {
		t.Fatalf("expected DONE sparkSql job, got state %q", state)
	}

	// ── Submit an unsupported job type → ERROR with terminal details ─────────
	code, body = doRequest(t, host, "POST", base+"/jobs:submit",
		[]byte(`{"job":{"reference":{"projectId":"`+project+`","jobId":"`+errJob+`"},"placement":{"clusterName":"`+clusterName+`"},"hiveJob":{"queryFileUri":"gs://b/q.hql"}}}`),
		"application/json")
	if code != http.StatusOK {
		t.Fatalf("submit hive job: got HTTP %d body %s", code, body)
	}
	if state := strField(jsonObj(t, body), "status", "state"); state != "ERROR" {
		t.Fatalf("expected ERROR hive job, got state %q body %s", state, body)
	}
	detailsBefore := strField(jsonObj(t, body), "status", "details")
	if detailsBefore == "" {
		t.Fatalf("expected non-empty status.details for failed job, got body %s", body)
	}

	// ── Export the full instance state ────────────────────────────────────────
	code, tarball := doRequest(t, host, "GET", "/_jaiscloud/export", nil, "")
	if code != http.StatusOK {
		t.Fatalf("export: got HTTP %d body %s", code, tarball)
	}
	if len(tarball) < 2 || tarball[0] != 0x1f || tarball[1] != 0x8b {
		t.Fatalf("export did not return a gzip tarball (first bytes %x)", tarball[:min(len(tarball), 2)])
	}

	// ── Import without reset_first: existing state → 409 non_empty_state ──────
	code, body = doRequest(t, host, "POST", "/_jaiscloud/import", tarball, "application/gzip")
	if code != http.StatusConflict {
		t.Fatalf("import without reset_first: expected 409, got HTTP %d body %s", code, body)
	}
	if !strings.Contains(string(body), "non_empty_state") {
		t.Fatalf("expected non_empty_state error, got body %s", body)
	}

	// ── Import with reset_first=true: clears state then restores ──────────────
	code, body = doRequest(t, host, "POST", "/_jaiscloud/import?reset_first=true", tarball, "application/gzip")
	if code != http.StatusOK {
		t.Fatalf("import with reset_first: got HTTP %d body %s", code, body)
	}

	// ── Verify restored cluster ───────────────────────────────────────────────
	code, body = doRequest(t, host, "GET", base+"/clusters/"+clusterName, nil, "")
	if code != http.StatusOK {
		t.Fatalf("get cluster after import: got HTTP %d body %s", code, body)
	}
	if state := strField(jsonObj(t, body), "status", "state"); state != "RUNNING" {
		t.Fatalf("cluster state after import: got %q body %s", state, body)
	}

	// ── Verify the restored GKE cluster's virtualClusterConfig ───────────────
	code, body = doRequest(t, host, "GET", base+"/clusters/"+gkeClusterName, nil, "")
	if code != http.StatusOK {
		t.Fatalf("get GKE cluster after import: got HTTP %d body %s", code, body)
	}
	gkeVCC, _ := jsonObj(t, body)["virtualClusterConfig"].(map[string]any)
	if gkeVCC == nil {
		t.Fatalf("virtualClusterConfig lost after import: body %s", body)
	}
	if target := strField(gkeVCC, "kubernetesClusterConfig", "gkeClusterConfig", "gkeClusterTarget"); target != "projects/proj/locations/us-central1/clusters/gke-1" {
		t.Fatalf("gkeClusterTarget lost after import: got %q body %s", target, body)
	}
	// The Metastore attachment round-trips too.
	msRef := strField(gkeVCC, "auxiliaryServicesConfig", "metastoreConfig", "dataprocMetastoreService")
	if msRef != msName {
		t.Fatalf("metastore attachment lost after import: got %q want %q body %s", msRef, msName, body)
	}

	// ── Verify restored jobs ──────────────────────────────────────────────────
	code, body = doRequest(t, host, "GET", base+"/jobs/"+doneJob, nil, "")
	if code != http.StatusOK {
		t.Fatalf("get done job after import: got HTTP %d body %s", code, body)
	}
	if state := strField(jsonObj(t, body), "status", "state"); state != "DONE" {
		t.Fatalf("done job state after import: got %q body %s", state, body)
	}

	// placement.clusterUuid is captured at submit and must survive export/import.
	code, body = doRequest(t, host, "GET", base+"/clusters/"+clusterName, nil, "")
	if code != http.StatusOK {
		t.Fatalf("get cluster for uuid: got HTTP %d body %s", code, body)
	}
	clusterUUID := strField(jsonObj(t, body), "clusterUuid")
	if clusterUUID == "" {
		t.Fatalf("cluster has no clusterUuid: body %s", body)
	}
	code, body = doRequest(t, host, "GET", base+"/jobs/"+doneJob, nil, "")
	if code != http.StatusOK {
		t.Fatalf("get done job for placement: got HTTP %d body %s", code, body)
	}
	if got := strField(jsonObj(t, body), "placement", "clusterUuid"); got != clusterUUID {
		t.Fatalf("placement.clusterUuid lost after import: got %q want %q", got, clusterUUID)
	}

	code, body = doRequest(t, host, "GET", base+"/jobs/"+errJob, nil, "")
	if code != http.StatusOK {
		t.Fatalf("get error job after import: got HTTP %d body %s", code, body)
	}
	if state := strField(jsonObj(t, body), "status", "state"); state != "ERROR" {
		t.Fatalf("error job state after import: got %q body %s", state, body)
	}
	if detailsAfter := strField(jsonObj(t, body), "status", "details"); detailsAfter != detailsBefore {
		t.Fatalf("status.details lost after import: got %q want %q", detailsAfter, detailsBefore)
	}

	// The sparkSqlJob must round-trip as a DONE sparkSqlJob (its type-job body
	// and terminal state survive export/import like any other job).
	code, body = doRequest(t, host, "GET", base+"/jobs/"+sqlJob, nil, "")
	if code != http.StatusOK {
		t.Fatalf("get sql job after import: got HTTP %d body %s", code, body)
	}
	if state := strField(jsonObj(t, body), "status", "state"); state != "DONE" {
		t.Fatalf("sql job state after import: got %q body %s", state, body)
	}
	if _, ok := jsonObj(t, body)["sparkSqlJob"]; !ok {
		t.Fatalf("sparkSqlJob body lost after import: %s", body)
	}

	// ── Verify lists return the restored entries ──────────────────────────────
	code, body = doRequest(t, host, "GET", base+"/clusters", nil, "")
	if code != http.StatusOK {
		t.Fatalf("list clusters after import: got HTTP %d body %s", code, body)
	}
	clusters, _ := jsonObj(t, body)["clusters"].([]any)
	names := map[string]bool{}
	for _, cl := range clusters {
		names[strField(cl.(map[string]any), "clusterName")] = true
	}
	if len(clusters) != 2 || !names[clusterName] || !names[gkeClusterName] {
		t.Fatalf("list clusters after import: expected [%s %s], got %v", clusterName, gkeClusterName, clusters)
	}

	code, body = doRequest(t, host, "GET", base+"/jobs", nil, "")
	if code != http.StatusOK {
		t.Fatalf("list jobs after import: got HTTP %d body %s", code, body)
	}
	jobs, _ := jsonObj(t, body)["jobs"].([]any)
	if len(jobs) != 3 {
		t.Fatalf("list jobs after import: expected 3 jobs, got %d", len(jobs))
	}
}

// pollOperationDone polls a Dataproc long-running operation (by its resource
// name) until it reports done, driving the emulator's lazy cluster state
// machine, and returns the packed response object (nil when the operation packs
// none, e.g. a delete's Empty response).
func pollOperationDone(t *testing.T, host string, op map[string]any) map[string]any {
	t.Helper()
	name, _ := op["name"].(string)
	if name == "" {
		t.Fatalf("operation has no name: %v", op)
	}
	deadline := clock.RealNow().Add(15 * time.Second)
	for clock.RealNow().Before(deadline) {
		code, body := doRequest(t, host, "GET", "/v1/"+name, nil, "")
		if code != http.StatusOK {
			t.Fatalf("get operation %s: got HTTP %d body %s", name, code, body)
		}
		cur := jsonObj(t, body)
		if done, _ := cur["done"].(bool); done {
			resp, _ := cur["response"].(map[string]any)
			return resp
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("operation %s did not complete", name)
	return nil
}

// driverOutputObject fetches a terminal job's driver-output object from the
// emulated GCS, proving the advertised driverOutputResourceUri resolves.
func driverOutputObject(t *testing.T, host, uri, jobID string) {
	t.Helper()
	if !strings.HasPrefix(uri, "gs://") {
		t.Fatalf("job %s driverOutputResourceUri = %q, want a gs:// URI", jobID, uri)
	}
	rest := strings.TrimPrefix(uri, "gs://")
	slash := strings.IndexByte(rest, '/')
	if slash < 0 {
		t.Fatalf("job %s driverOutputResourceUri = %q, want a gs://bucket/object URI", jobID, uri)
	}
	bucket, object := rest[:slash], rest[slash+1:]
	// The advertised URI is a prefix; readers fetch <uri>.000000000.
	path := "/storage/v1/b/" + bucket + "/o/" + url.PathEscape(object+".000000000") + "?alt=media"
	code, body := doRequest(t, host, "GET", path, nil, "")
	if code != http.StatusOK || len(body) == 0 {
		t.Fatalf("read driver output %s: got HTTP %d with %d byte(s) body %q", path, code, len(body), body)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
