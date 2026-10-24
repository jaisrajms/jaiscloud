//go:build dataproc_namespace_e2e

// Package dataproc_test is the real-Kubernetes gate for Dataproc per-cluster
// namespace isolation (KNS3). KNS1 added the shared namespace lifecycle and KNS2
// wired Dataproc to provision one Kubernetes namespace per cluster (KNS2's units
// cover the seams with a fake clientset); this proves the contract against the
// deployed k3d emulator and real Spark pods:
//
//   - create two Dataproc clusters with distinct placements — a GKE-virtual
//     cluster with an explicit virtualClusterConfig kubernetesNamespace, and a
//     GCE-shaped cluster with the derived deterministic name — and assert both
//     namespaces exist and carry the emulator ownership labels
//   - submit a long-running PySpark job to each, and assert the driver Job/pod
//     for a job appears ONLY in its own cluster's namespace
//   - delete one cluster and assert its namespace and workloads are gone while
//     the other cluster's namespace and its running job survive
//
// Run with:
//
//	make test-dataproc-namespace-k8s
//
// It is inert without a k3d cluster: requireK3d skips when kubectl or the
// jaiscloud-gcp Service is absent. The target applies deploy/k8s/rbac.yaml
// first, because cluster-scoped namespace create/delete needs the
// jaiscloud-namespace-admin ClusterRoleBinding from that manifest.
//
// Required env:
//
//	DATAPROC_NAMESPACE_E2E — set to a non-empty value to run (else skipped)
//
// Optional env:
//
//	K8S_NAMESPACE — default jaiscloud
package dataproc_test

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

	"jaiscloud/internal/k8shelpers"
)

const (
	testProject = "jaiscloud-project"
	testRegion  = "us-central1"

	// clusterReadyDelay in the deployed emulator is 0, so a cluster/namespace
	// settles on the first read; the generous timeouts only guard against a
	// slow controller/scheduler.
	namespaceGoneTimeout = 3 * time.Minute
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

// kubectlIn runs kubectl scoped to a namespace.
func kubectlIn(ns string, args ...string) (string, error) {
	return kubectl(append([]string{"-n", ns}, args...)...)
}

// requireK3d skips the test unless kubectl is present and the emulator Service
// is reachable in the target namespace.
func requireK3d(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("kubectl"); err != nil {
		t.Skip("kubectl not found — skipping k3d Dataproc namespace isolation e2e")
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
// (empty map for an empty body).
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

// ─── GCS staging (JSON API) ────────────────────────────────────────────────

func ensureBucket(t *testing.T, base, bucket string) {
	t.Helper()
	code, body := api(t, http.MethodPost, base+"/storage/v1/b?project="+url.QueryEscape(testProject),
		map[string]any{"name": bucket})
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

// deleteBucket removes the test bucket's objects and then the bucket itself.
// It is best-effort: the test's final /_jaiscloud/reset clears the GCS store
// (objects and buckets), so a 404 here is expected cleanup state, not a
// failure, and no cleanup error may fail an otherwise-passing test.
func deleteBucket(t *testing.T, base, bucket string) {
	t.Helper()
	code, body := api(t, http.MethodGet, fmt.Sprintf("%s/storage/v1/b/%s/o", base, bucket), nil)
	switch {
	case code == http.StatusNotFound:
		// Already gone (e.g. the final reset). Nothing to delete.
	case code >= 300:
		t.Logf("cleanup: list %s: HTTP %d: %v", bucket, code, body)
	default:
		items, _ := body["items"].([]any)
		for _, it := range items {
			m, ok := it.(map[string]any)
			if !ok {
				continue
			}
			name, _ := m["name"].(string)
			if name == "" {
				continue
			}
			dcode, _ := api(t, http.MethodDelete,
				fmt.Sprintf("%s/storage/v1/b/%s/o/%s", base, bucket, url.PathEscape(name)), nil)
			if dcode >= 300 && dcode != http.StatusNotFound {
				t.Logf("cleanup: delete %s/%s: HTTP %d", bucket, name, dcode)
			}
		}
		dcode, dbody := api(t, http.MethodDelete, fmt.Sprintf("%s/storage/v1/b/%s", base, bucket), nil)
		if dcode >= 300 && dcode != http.StatusNotFound {
			t.Logf("cleanup: delete bucket %s: HTTP %d: %v", bucket, dcode, dbody)
		}
	}
}

// checkpointCommits returns the committed micro-batch marker objects under a
// job's gs:// checkpoint, or nil when the listing fails.
func checkpointCommits(t *testing.T, base, bucket, jobID string) []string {
	t.Helper()
	prefix := jobID + "/checkpoint/commits/"
	u := fmt.Sprintf("%s/storage/v1/b/%s/o?prefix=%s", base, bucket, url.QueryEscape(prefix))
	code, body := api(t, http.MethodGet, u, nil)
	if code >= 300 {
		return nil
	}
	items, _ := body["items"].([]any)
	var names []string
	for _, it := range items {
		if m, ok := it.(map[string]any); ok {
			if n, ok := m["name"].(string); ok {
				names = append(names, n)
			}
		}
	}
	return names
}

// waitStreamingProgress waits until the job has committed at least one
// micro-batch. Dataproc marks a job RUNNING as soon as the driver Job is
// submitted, so RUNNING alone does not prove the driver actually initialised;
// a committed checkpoint does (a driver that dies at SparkContext init, e.g.
// with too little spark.driver.memory, never commits).
func waitStreamingProgress(t *testing.T, base, bucket, jobID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last []string
	for time.Now().Before(deadline) {
		last = checkpointCommits(t, base, bucket, jobID)
		for _, n := range last {
			if strings.Contains(n, "commits/") && !strings.Contains(n, ".tmp") {
				return
			}
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("job %s never committed a micro-batch under gs://%s/%s/checkpoint/commits/ (objects: %v); the driver may not be running", jobID, bucket, jobID, last)
}

// ─── Dataproc wiring (REST) ────────────────────────────────────────────────

func clusterPath(name string) string {
	return fmt.Sprintf("/v1/projects/%s/regions/%s/clusters/%s", testProject, testRegion, name)
}

func jobPath(id string) string {
	return fmt.Sprintf("/v1/projects/%s/regions/%s/jobs/%s", testProject, testRegion, id)
}

// createCluster submits a cluster create and polls until the cluster reports
// RUNNING. It deletes any prior record of the same name first so a re-run (or a
// leaked record) never 409s.
func createCluster(t *testing.T, base, name string, body map[string]any) {
	t.Helper()
	code, prior := api(t, http.MethodDelete, base+clusterPath(name), nil)
	if code == http.StatusOK {
		pollOperation(t, base, prior)
	}
	code, op := api(t, http.MethodPost,
		base+fmt.Sprintf("/v1/projects/%s/regions/%s/clusters", testProject, testRegion), body)
	mustOK(t, "create cluster "+name, code, op)
	if done, _ := op["done"].(bool); !done {
		pollOperation(t, base, op)
	}
	waitClusterRunning(t, base, name, 2*time.Minute)
}

// waitClusterRunning polls GET cluster until it reports RUNNING (the lazy state
// machine settles CREATING -> RUNNING on read).
func waitClusterRunning(t *testing.T, base, name string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last map[string]any
	for time.Now().Before(deadline) {
		code, cl := api(t, http.MethodGet, base+clusterPath(name), nil)
		if code == http.StatusOK {
			last = cl
			switch strField(cl, "status", "state") {
			case "RUNNING":
				return
			case "ERROR":
				t.Fatalf("cluster %s reached ERROR: %v", name, cl)
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("cluster %s did not reach RUNNING within %s (last: %v)", name, timeout, last)
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
		return op
	}
	deadline := time.Now().Add(2 * time.Minute)
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
	t.Fatalf("operation %s did not complete within 2m", name)
	return nil
}

// gkeVirtualBody builds a Dataproc-on-GKE create body. The emulator models the
// GKE control plane as metadata; kubernetesNamespace is the caller-supplied
// workload placement KNS2 provisions.
func gkeVirtualBody(name, ns string) map[string]any {
	return map[string]any{
		"clusterName": name,
		"virtualClusterConfig": map[string]any{
			"kubernetesClusterConfig": map[string]any{
				"kubernetesNamespace": ns,
				"gkeClusterConfig": map[string]any{
					"gkeClusterTarget": fmt.Sprintf("projects/%s/locations/%s/clusters/gke-target", testProject, testRegion),
				},
			},
		},
	}
}

// gceBody builds a GCE-shaped create body (no virtualClusterConfig), so KNS2
// derives a deterministic namespace name.
func gceBody(name string) map[string]any {
	return map[string]any{
		"clusterName": name,
		"config":      map[string]any{"gceClusterConfig": map[string]any{"zoneUri": testRegion + "-a"}},
	}
}

// streamingScript is a PySpark Structured Streaming driver that reads the
// built-in rate source and blocks in awaitTermination, keeping the Dataproc job
// RUNNING (and its driver/executor pods alive) until the cluster is deleted.
func streamingScript(bucket, prefix string) string {
	return fmt.Sprintf(`from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.readStream.format("rate").option("rowsPerSecond", 2).load()
out = df.withColumn("v", F.col("value").cast("string"))
(
    out.writeStream.format("json")
    .option("path", "gs://%s/%s/sink")
    .option("checkpointLocation", "gs://%s/%s/checkpoint")
    .trigger(processingTime="2 seconds")
    .start()
    .awaitTermination()
)
`, bucket, prefix, bucket, prefix)
}

// submitStreamingJob submits a pysparkJob to a cluster and returns the job id.
func submitStreamingJob(t *testing.T, base, bucket, cluster, jobID string) {
	t.Helper()
	body := map[string]any{
		"job": map[string]any{
			"reference": map[string]any{"jobId": jobID},
			"placement": map[string]any{"clusterName": cluster},
			"pysparkJob": map[string]any{
				"mainPythonFileUri": fmt.Sprintf("gs://%s/%s-stream.py", bucket, jobID),
				"properties": map[string]any{
					"spark.sql.streaming.checkpointLocation": fmt.Sprintf("gs://%s/%s/checkpoint", bucket, jobID),
					"spark.driver.memory":                    "512m",
					"spark.executor.memory":                  "512m",
					"spark.executor.instances":               "1",
					"spark.sql.shuffle.partitions":           "1",
				},
			},
		},
	}
	code, resp := api(t, http.MethodPost,
		base+fmt.Sprintf("/v1/projects/%s/regions/%s/jobs:submit", testProject, testRegion), body)
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

func cancelJob(t *testing.T, base, jobID string) {
	t.Helper()
	code, resp := api(t, http.MethodPost, base+jobPath(jobID)+":cancel", nil)
	mustOK(t, "cancel job "+jobID, code, resp)
}

// ─── namespace / workload assertions (kubectl) ─────────────────────────────

// namespaceExists reports whether a namespace currently exists.
func namespaceExists(t *testing.T, ns string) bool {
	t.Helper()
	_, err := kubectl("get", "namespace", ns, "-o", "name")
	return err == nil
}

// namespaceLabels returns a namespace's labels, or nil when it is absent.
func namespaceLabels(t *testing.T, ns string) map[string]string {
	t.Helper()
	out, err := kubectl("get", "namespace", ns, "-o", "jsonpath={.metadata.labels}")
	if err != nil {
		return nil
	}
	labels := map[string]string{}
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &labels); err != nil {
		t.Fatalf("decode labels of namespace %s: %v (%q)", ns, err, out)
	}
	return labels
}

// waitNamespaceExists polls until a namespace appears.
func waitNamespaceExists(t *testing.T, ns string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if namespaceExists(t, ns) {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("namespace %s never appeared within %s", ns, timeout)
}

// waitNamespaceGone polls until a namespace is deleted.
func waitNamespaceGone(t *testing.T, ns string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !namespaceExists(t, ns) {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("namespace %s still exists after %s (teardown did not delete the owned namespace)", ns, timeout)
}

// objectNamesInNamespace lists resource names in ns, scoped by label selector.
// A listing error is returned, never folded into an empty result, so a
// "not present" assertion cannot pass because kubectl itself failed.
func objectNamesInNamespace(ns, resource, selector string) ([]string, error) {
	out, err := kubectlIn(ns, "get", resource, "-l", selector,
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

// waitObjectGone polls until no resource matches the selector in ns.
func waitObjectGone(t *testing.T, ns, resource, selector string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		// A deleted namespace takes its objects with it, and listing in it then
		// errors (NotFound) rather than returning an empty list: that is gone.
		if !namespaceExists(t, ns) {
			return
		}
		names, err := objectNamesInNamespace(ns, resource, selector)
		if err != nil {
			lastErr = err
			time.Sleep(time.Second)
			continue
		}
		lastErr = nil
		if len(names) == 0 {
			return
		}
		time.Sleep(time.Second)
	}
	if lastErr != nil {
		t.Fatalf("could not list %s in %s: %v", resource, ns, lastErr)
	}
	t.Fatalf("%s matching %q still present in %s after %s", resource, selector, ns, timeout)
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

// assertPlaced asserts jobID's driver Job exists only in wantNS, and that the
// other cluster's namespace does not hold it (per-cluster isolation).
func assertPlaced(t *testing.T, jobID, wantNS, otherNS string) {
	t.Helper()
	sel := "jaiscloud.io/job-id=" + jobID
	deadline := time.Now().Add(4 * time.Minute)
	for time.Now().Before(deadline) {
		names, err := objectNamesInNamespace(wantNS, "jobs", sel)
		if err != nil {
			t.Fatalf("list jobs in %s: %v", wantNS, err)
		}
		if len(names) > 0 {
			break
		}
		time.Sleep(time.Second)
	}
	names, err := objectNamesInNamespace(wantNS, "jobs", sel)
	if err != nil {
		t.Fatalf("list jobs in %s: %v", wantNS, err)
	}
	if len(names) == 0 {
		t.Fatalf("job %s driver Job never appeared in cluster namespace %s", jobID, wantNS)
	}
	other, err := objectNamesInNamespace(otherNS, "jobs", sel)
	if err != nil {
		t.Fatalf("list jobs in %s: %v", otherNS, err)
	}
	if len(other) > 0 {
		t.Fatalf("job %s leaked into the other cluster's namespace %s: %v", jobID, otherNS, other)
	}
	// The driver pod (and, once scheduled, executors) share the job-id label;
	// log what landed to make the placement visible in the report.
	if pods, err := objectNamesInNamespace(wantNS, "pods", sel); err == nil {
		t.Logf("job %s pods in %s: %v", jobID, wantNS, pods)
	}
}

func TestDataprocNamespaceIsolationK3d(t *testing.T) {
	if os.Getenv("DATAPROC_NAMESPACE_E2E") == "" {
		t.Skip("DATAPROC_NAMESPACE_E2E not set — skipping Dataproc per-cluster namespace e2e")
	}
	requireK3d(t)

	base, stop := startPortForward(t)
	// Register the port-forward teardown first so it runs last (t.Cleanup is
	// LIFO): the resource cleanups below still need the forward.
	t.Cleanup(stop)

	if code, body := api(t, http.MethodPost, base+"/_jaiscloud/reset", nil); code != http.StatusOK {
		t.Fatalf("reset: HTTP %d: %v", code, body)
	}

	// Unique per run so re-runs never collide with a prior run's bucket,
	// cluster, job id, or explicit namespace in the shared Postgres-backed store.
	run := fmt.Sprintf("%d", time.Now().UnixNano())
	bucket := "namespace-smoke-" + run
	clusterA := "ns-virtual-" + run
	clusterB := "ns-gce-" + run
	jobA := "ns-job-a-" + run
	jobB := "ns-job-b-" + run
	nsA := "dp-ns-a-" + run
	// The GCE cluster has no virtualClusterConfig, so KNS2 derives the name
	// deterministically from the service, project and cluster name.
	nsB := k8shelpers.NamespaceName("dataproc", testProject, clusterB)

	ensureBucket(t, base, bucket)
	t.Cleanup(func() { deleteBucket(t, base, bucket) })
	uploadText(t, base, bucket, jobA+"-stream.py", streamingScript(bucket, jobA))
	uploadText(t, base, bucket, jobB+"-stream.py", streamingScript(bucket, jobB))

	// ── provision two clusters in distinct namespaces ─────────────────────────
	createCluster(t, base, clusterA, gkeVirtualBody(clusterA, nsA))
	t.Cleanup(func() { deleteCluster(t, base, clusterA) })
	createCluster(t, base, clusterB, gceBody(clusterB))
	t.Cleanup(func() { deleteCluster(t, base, clusterB) })

	waitNamespaceExists(t, nsA, 2*time.Minute)
	waitNamespaceExists(t, nsB, 2*time.Minute)
	for _, ns := range []string{nsA, nsB} {
		labels := namespaceLabels(t, ns)
		if labels["jaiscloud.io/managed-by"] != "jaiscloud" || labels["jaiscloud.io/service"] != "dataproc" {
			t.Fatalf("namespace %s is not labelled as an emulator-owned Dataproc namespace: %v", ns, labels)
		}
	}
	t.Logf("cluster %s -> namespace %s; cluster %s -> namespace %s", clusterA, nsA, clusterB, nsB)

	// ── a job to each cluster, sequentially ──────────────────────────────────
	// Two concurrent Spark drivers OOM a small single-node k3d cluster, and a
	// starved driver delays the submitter's state writes. So each placement is
	// proven with exactly one driver alive at a time: run A's job, cancel it
	// (which must reap its driver), then run B's job and delete B while it runs.

	// Cluster A: the caller-supplied virtualClusterConfig namespace is honored.
	submitStreamingJob(t, base, bucket, clusterA, jobA)
	waitForState(t, base, jobA, "RUNNING", 8*time.Minute)
	waitStreamingProgress(t, base, bucket, jobA, 4*time.Minute)
	assertPlaced(t, jobA, nsA, nsB)
	cancelJob(t, base, jobA)
	waitForState(t, base, jobA, "CANCELLED", 2*time.Minute)
	waitObjectGone(t, nsA, "jobs", "jaiscloud.io/job-id="+jobA, namespaceGoneTimeout)
	if !namespaceExists(t, nsA) {
		t.Fatalf("cancelling job %s must not delete its cluster %s namespace %s", jobA, clusterA, nsA)
	}

	// Cluster B: the derived deterministic namespace holds its job.
	submitStreamingJob(t, base, bucket, clusterB, jobB)
	waitForState(t, base, jobB, "RUNNING", 8*time.Minute)
	waitStreamingProgress(t, base, bucket, jobB, 4*time.Minute)
	assertPlaced(t, jobB, nsB, nsA)

	// ── delete B: its namespace and running workload go, A's namespace stays ──
	deleteCluster(t, base, clusterB)
	waitNamespaceGone(t, nsB, namespaceGoneTimeout)
	waitObjectGone(t, nsB, "jobs", "jaiscloud.io/job-id="+jobB, namespaceGoneTimeout)

	if !namespaceExists(t, nsA) {
		t.Fatalf("deleting cluster %s removed the unrelated cluster %s namespace %s", clusterB, clusterA, nsA)
	}
	t.Logf("isolation holds: %s deleted; %s (%s) survived", clusterB, clusterA, nsA)

	// ── reset reclaims every emulator-owned namespace ────────────────────────
	if code, body := api(t, http.MethodPost, base+"/_jaiscloud/reset", nil); code != http.StatusOK {
		t.Fatalf("reset: HTTP %d: %v", code, body)
	}
	waitNamespaceGone(t, nsA, namespaceGoneTimeout)
}
