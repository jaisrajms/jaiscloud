//go:build iceberg_k3d_e2e

// Package icebergk3d_test holds the real-Kubernetes Dataproc Iceberg gate
// (SPK1): a Dataproc-submitted Spark job that uses the emulator's Hive
// Metastore as its Iceberg catalog and writes a real table through the wired
// GCS connector.
//
// It is the join of two existing gates: `test-e2e-iceberg-gcp` proves the
// Iceberg + HiveCatalog + HadoopFileIO recipe but runs external Docker
// `spark-sql` (not a Dataproc job), and `test-e2e-lakehouse-k3d` proves a
// Dataproc job on a GKE virtualClusterConfig cluster with a Metastore
// *attachment* (but never opens the catalog). This gate submits the Iceberg job
// through the Dataproc API and asserts GCS bytes plus a read-back.
//
// Run with:
//
//	make test-e2e-iceberg-k3d
//
// It is inert without a k3d cluster: requireK3d skips when kubectl or the
// jaiscloud-gcp Service is absent.
package icebergk3d_test

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
	"strings"
	"testing"
	"time"
)

const (
	testProject = "jaiscloud-project"
	testRegion  = "us-central1"

	// hmsServiceID is the Dataproc Metastore service the cluster attaches. The
	// emulator serves a single global Hive Thrift catalog, so the service is
	// metadata-only; the reachable thrift address comes from the deployment
	// override JAISCLOUD_DATAPROC_HMS_ENDPOINT (deploy/k8s/jaiscloud-gcp.yaml).
	hmsServiceID = "iceberg-k3d-hms"

	// The deployed spark-gcs image carries only the GCS connector, so the
	// self-contained Iceberg Spark runtime (the same jar the Docker iceberg gate
	// uses) is staged on the emulator's GCS and passed through the Dataproc
	// job's jarFileUris (-> spark-submit --jars).
	icebergRuntimeJarName = "iceberg-spark-runtime-3.5_2.12-1.5.2.jar"
	icebergRuntimeJarURL  = "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar"
)

var (
	httpClient     = &http.Client{Timeout: 60 * time.Second}
	downloadClient = &http.Client{Timeout: 3 * time.Minute}
)

func namespace() string {
	if v := os.Getenv("K8S_NAMESPACE"); v != "" {
		return v
	}
	return "jaiscloud"
}

// hmsAddress is the pod-reachable Hive Metastore thrift address, matching the
// deployment's JAISCLOUD_DATAPROC_HMS_ENDPOINT. Overridable for a non-default
// namespace.
func hmsAddress() string {
	if v := os.Getenv("ICEBERG_K3D_HMS_ADDRESS"); v != "" {
		return v
	}
	return fmt.Sprintf("jaiscloud-gcp.%s.svc.cluster.local:9083", namespace())
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
		t.Skip("kubectl not found — skipping k3d Dataproc Iceberg e2e")
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

// uploadFileObject uploads a local file as a GCS object (media upload, JSON
// API).
func uploadFileObject(t *testing.T, base, bucket, name, path string) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	q := url.Values{"uploadType": {"media"}, "name": {name}}.Encode()
	req, err := http.NewRequest(http.MethodPost,
		fmt.Sprintf("%s/upload/storage/v1/b/%s/o?%s", base, bucket, q), f)
	if err != nil {
		t.Fatalf("build upload %s: %v", name, err)
	}
	req.Header.Set("Content-Type", "application/java-archive")
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("upload %s: %v", name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("upload %s/%s: HTTP %d: %s", bucket, name, resp.StatusCode, raw)
	}
}

func downloadFile(t *testing.T, url, dest string) {
	t.Helper()
	resp, err := downloadClient.Get(url)
	if err != nil {
		t.Fatalf("download %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("download %s: HTTP %d", url, resp.StatusCode)
	}
	f, err := os.Create(dest)
	if err != nil {
		t.Fatalf("create %s: %v", dest, err)
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		t.Fatalf("download %s: write: %v", url, err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close %s: %v", dest, err)
	}
}

// stageIcebergJar downloads the Iceberg Spark runtime (host-side, cached
// through ICEBERG_K3D_JARS_DIR for offline runs) and uploads it into the run's
// bucket, returning the gs:// URI for pysparkJob.jarFileUris.
func stageIcebergJar(t *testing.T, base, bucket string) string {
	t.Helper()
	dir := os.Getenv("ICEBERG_K3D_JARS_DIR")
	if dir == "" {
		dir = t.TempDir()
	}
	path := filepath.Join(dir, icebergRuntimeJarName)
	if _, err := os.Stat(path); err != nil {
		downloadFile(t, icebergRuntimeJarURL, path)
	}
	uploadFileObject(t, base, bucket, "jars/"+icebergRuntimeJarName, path)
	return fmt.Sprintf("gs://%s/jars/%s", bucket, icebergRuntimeJarName)
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

// readObjectText fetches a GCS object's bytes through the JSON API (alt=media).
func readObjectText(t *testing.T, base, bucket, name string) string {
	t.Helper()
	u := fmt.Sprintf("%s/storage/v1/b/%s/o/%s?alt=media", base, bucket, url.PathEscape(name))
	resp, err := httpClient.Get(u)
	if err != nil {
		t.Fatalf("read %s/%s: %v", bucket, name, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		t.Fatalf("read %s/%s: HTTP %d: %s", bucket, name, resp.StatusCode, raw)
	}
	return string(raw)
}

// ─── Dataproc wiring (REST) ────────────────────────────────────────────────

func clusterPath(name string) string {
	return fmt.Sprintf("/v1/projects/%s/regions/%s/clusters/%s", testProject, testRegion, name)
}

func jobPath(id string) string {
	return fmt.Sprintf("/v1/projects/%s/regions/%s/jobs/%s", testProject, testRegion, id)
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

// createMetastoreService ensures the Metastore service the cluster attaches
// exists (409 is idempotent) and returns its fully-qualified name.
func createMetastoreService(t *testing.T, base string) string {
	t.Helper()
	code, body := api(t, http.MethodPost,
		base+fmt.Sprintf("/v1/projects/%s/locations/%s/services?serviceId=%s", testProject, testRegion, hmsServiceID),
		map[string]any{"hiveMetastoreConfig": map[string]any{"endpointProtocol": "THRIFT"}})
	if code >= 300 && code != http.StatusConflict {
		t.Fatalf("create metastore service %s: HTTP %d: %v", hmsServiceID, code, body)
	}
	return fmt.Sprintf("projects/%s/locations/%s/services/%s", testProject, testRegion, hmsServiceID)
}

// createGkeCluster recreates the cluster as a GKE-backed Dataproc-on-GKE
// cluster (virtualClusterConfig) attached to the Metastore service and polls
// the async create operation to RUNNING. Both placements run the same
// client-mode Spark pods; the GKE config is metadata only.
func createGkeCluster(t *testing.T, base, name, msRef string) {
	t.Helper()
	code, body := api(t, http.MethodDelete, base+clusterPath(name), nil)
	if code == http.StatusOK {
		if done, _ := body["done"].(bool); !done {
			pollOperation(t, base, body)
		}
	}
	code, body = api(t, http.MethodPost, base+fmt.Sprintf("/v1/projects/%s/regions/%s/clusters", testProject, testRegion),
		map[string]any{
			"clusterName": name,
			"virtualClusterConfig": map[string]any{
				"kubernetesClusterConfig": map[string]any{
					"gkeClusterConfig": map[string]any{
						"gkeClusterTarget": fmt.Sprintf("projects/%s/locations/%s/clusters/iceberg-gke", testProject, testRegion),
					},
				},
				"auxiliaryServicesConfig": map[string]any{
					"metastoreConfig": map[string]any{"dataprocMetastoreService": msRef},
				},
			},
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
	vcc, _ := cl["virtualClusterConfig"].(map[string]any)
	if vcc == nil {
		t.Fatalf("cluster %s lost virtualClusterConfig: %v", name, cl)
	}
	aux, _ := vcc["auxiliaryServicesConfig"].(map[string]any)
	mc, _ := aux["metastoreConfig"].(map[string]any)
	if got := strField(mc, "dataprocMetastoreService"); got != msRef {
		t.Fatalf("cluster %s metastore = %q, want %q", name, got, msRef)
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

// submitPySparkJob submits a pysparkJob (with the Iceberg runtime in
// jarFileUris and the catalog properties) and returns the submit response.
func submitPySparkJob(t *testing.T, base, cluster, jobID, scriptURI, jarURI string, props map[string]any) map[string]any {
	t.Helper()
	body := map[string]any{
		"job": map[string]any{
			"reference": map[string]any{"jobId": jobID},
			"placement": map[string]any{"clusterName": cluster},
			"pysparkJob": map[string]any{
				"mainPythonFileUri": scriptURI,
				"jarFileUris":       []string{jarURI},
				"properties":        props,
			},
		},
	}
	code, resp := api(t, http.MethodPost, base+fmt.Sprintf("/v1/projects/%s/regions/%s/jobs:submit", testProject, testRegion), body)
	mustOK(t, "submit job "+jobID, code, resp)
	if _, ok := resp["pysparkJob"]; !ok {
		t.Fatalf("submit response has no pysparkJob: %v", resp)
	}
	return resp
}

// icebergCatalogProps returns the Spark conf that points Iceberg's SparkCatalog
// at the emulator's Hive Metastore (HiveCatalog over Thrift) with the
// gcs-connector-backed HadoopFileIO data path. These are caller job
// `properties`, passed through verbatim to `--conf` (the emulator's GCS
// connector wiring is protected and cannot be stripped).
func icebergCatalogProps(bucket, run string) map[string]any {
	return map[string]any{
		"spark.sql.catalog.hms":              "org.apache.iceberg.spark.SparkCatalog",
		"spark.sql.catalog.hms.catalog-impl": "org.apache.iceberg.hive.HiveCatalog",
		"spark.sql.catalog.hms.uri":          "thrift://" + hmsAddress(),
		"spark.sql.catalog.hms.warehouse":    fmt.Sprintf("gs://%s/%s/", bucket, run),
		"spark.sql.catalog.hms.io-impl":      "org.apache.iceberg.hadoop.HadoopFileIO",
		// The k3d node is memory-tight (a co-located workload plus the emulator
		// leave little headroom), so keep both JVMs at Spark 3.5's 450m minimum
		// heap. Only one small executor is needed for a 3-row write.
		"spark.driver.memory":          "450m",
		"spark.executor.memory":        "450m",
		"spark.executor.instances":     "1",
		"spark.sql.shuffle.partitions": "1",
	}
}

func jobState(t *testing.T, base, jobID string) string {
	t.Helper()
	code, resp := api(t, http.MethodGet, base+jobPath(jobID), nil)
	mustOK(t, "get job "+jobID, code, resp)
	return strField(resp, "status", "state")
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
			t.Fatalf("job %s reached terminal state %q, want %q\n%s", jobID, last, want, driverOutput(t, base, jobID))
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("job %s did not reach %q within %s (last %q)", jobID, want, timeout, last)
}

// driverOutput resolves a terminal job's driverOutputResourceUri to the staged
// GCS object (<uri>.000000000) and returns its text.
func driverOutput(t *testing.T, base, jobID string) string {
	t.Helper()
	code, job := api(t, http.MethodGet, base+jobPath(jobID), nil)
	if code >= 300 {
		t.Fatalf("get job %s for driver output: HTTP %d: %v", jobID, code, job)
	}
	uri := strField(job, "driverOutputResourceUri")
	rest, ok := strings.CutPrefix(uri, "gs://")
	if !ok {
		t.Fatalf("job %s driverOutputResourceUri = %q, want a gs:// URI", jobID, uri)
	}
	bucket, object, ok := strings.Cut(rest, "/")
	if !ok {
		t.Fatalf("job %s driverOutputResourceUri = %q, want gs://bucket/object", jobID, uri)
	}
	return readObjectText(t, base, bucket, object+".000000000")
}

// parseMetric extracts an integer printed as `<key>=<n>` from driver output.
func parseMetric(t *testing.T, output, key string) int {
	t.Helper()
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if rest, ok := strings.CutPrefix(line, key+"="); ok {
			var n int
			if _, err := fmt.Sscanf(rest, "%d", &n); err != nil {
				t.Fatalf("parse %s from %q: %v", key, line, err)
			}
			return n
		}
	}
	t.Fatalf("driver output has no %s= line:\n%s", key, output)
	return 0
}
