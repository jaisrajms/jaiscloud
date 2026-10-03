//go:build iceberg_e2e

package iceberg_test

import (
	"context"
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

// ─── env / config helpers ────────────────────────────────────────────────────

func jaiscloudHost() string {
	if h := os.Getenv("JAISCLOUD_HOST"); h != "" {
		return h
	}
	return "http://localhost:8080"
}

func icebergImage() string {
	return os.Getenv("SPARK_E2E_ICEBERG_GCP_IMAGE")
}

func requireIcebergEnv(t *testing.T) {
	t.Helper()
	if icebergImage() == "" {
		t.Skip("SPARK_E2E_ICEBERG_GCP_IMAGE not set — skipping Iceberg e2e test")
	}
}

// hmsEndpoint returns the Thrift Hive Metastore endpoint Spark's HiveCatalog
// connects to (Phase 4's :9083 listener). Overridable via SPARK_E2E_HMS_ENDPOINT
// (without a scheme — the harness prefixes it with thrift://).
func hmsEndpoint() string {
	if v := os.Getenv("SPARK_E2E_HMS_ENDPOINT"); v != "" {
		return v
	}
	return "host.docker.internal:9083"
}

// gcsProjectID returns the GCS project id used for anonymous auth
// (fs.gs.project.id). Overridable via GCP_PROJECT_ID.
func gcsProjectID() string {
	if v := os.Getenv("GCP_PROJECT_ID"); v != "" {
		return v
	}
	return "test-project"
}

func jobTimeout() time.Duration {
	if v := os.Getenv("SPARK_E2E_JOB_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return 10 * time.Minute
}

// ─── run-scoped naming helpers ────────────────────────────────────────────────

// icebergDB returns the Hive database name for this test run.
// Each invocation of `go test` gets a unique 6-hex-char suffix so that
// concurrent or back-to-back runs never conflict on the same database name.
func icebergDB() string { return "iceberg_test_" + testRunID }

// tableLocation returns the gs:// LOCATION clause value for an Iceberg table in
// this run.
func tableLocation(table string) string {
	return fmt.Sprintf("gs://iceberg-warehouse/%s/%s", testRunID, table)
}

// outputLoc returns the gs:// URI used in INSERT OVERWRITE DIRECTORY for this run.
func outputLoc(dir string) string {
	return fmt.Sprintf("gs://iceberg-warehouse/%s/%s/", testRunID, dir)
}

// outputPrefix returns the GCS object prefix for assertions on output
// directories (bucket is always "iceberg-warehouse").
func outputPrefix(dir string) string {
	return fmt.Sprintf("%s/%s/", testRunID, dir)
}

// tablePrefix returns the GCS object prefix for a table's sub-directory (e.g.
// "metadata/" or "data/") — used with countGCSObjects / hasGCSObjects.
func tablePrefix(table, subdir string) string {
	return fmt.Sprintf("%s/%s/%s", testRunID, table, subdir)
}

// ensureDatabaseSQL returns the SQL to create the Hive database for this run.
//
// Divergence from the AWS harness (which pre-creates the Glue DB via the Go
// Glue SDK in TestMain): there is no Go-side Hive metastore client, and
// HiveCatalog (unlike GlueCatalog) does not auto-create the namespace. So the
// run-scoped database is created from Spark SQL instead (idempotent).
func ensureDatabaseSQL() string {
	return fmt.Sprintf("CREATE DATABASE IF NOT EXISTS hms.%s;", icebergDB())
}

// ─── Spark SQL job runner ─────────────────────────────────────────────────────

// SparkJob holds parameters for a Docker-based spark-sql job.
type SparkJob struct {
	// Name is used for logging.
	Name string
	// SQL is the Spark SQL script to execute.
	SQL string
	// ExtraConf holds additional --conf flags (e.g. "spark.driver.memory=1g").
	ExtraConf []string
}

// runSparkSQL submits a Spark SQL job via Docker using the default JaisCloud host.
func runSparkSQL(t *testing.T, job SparkJob) {
	t.Helper()
	runSparkSQLOnHost(t, jaiscloudHost(), job)
}

// runSparkSQLOnHost submits a Spark SQL job via Docker pointing at a specific
// JaisCloud host. The SQL is passed inline via spark-sql -e.
//
// -e is deliberate: it keeps the harness daemon-location-agnostic. A bind mount
// (-v tmp.sql:/tmp/job.sql) resolves on the Docker daemon's filesystem, so it
// silently becomes an empty directory against a remote daemon. Passing the SQL
// on the command line avoids the daemon needing to see the test process's
// filesystem at all.
func runSparkSQLOnHost(t *testing.T, host string, job SparkJob) {
	t.Helper()
	requireIcebergEnv(t)

	args := []string{
		"docker", "run", "--rm",
		"--add-host=host.docker.internal:host-gateway",
		// No GCP env vars are required: the harness uses anonymous auth
		// (fs.gs.auth.null.enable=true + fs.gs.project.id) and the explicit
		// fs.gs.storage.root.url — no Workload Identity, no
		// GOOGLE_APPLICATION_CREDENTIALS.
		icebergImage(),
		"/opt/spark/bin/spark-sql",
		"--master", "local[2]",
	}
	for _, c := range icebergSparkConfForHost(host) {
		args = append(args, "--conf", c)
	}
	for _, c := range job.ExtraConf {
		args = append(args, "--conf", c)
	}
	args = append(args, "-e", job.SQL)

	t.Logf("running spark-sql job %q on %s", job.Name, host)
	ctx, cancel := context.WithTimeout(context.Background(), jobTimeout())
	defer cancel()

	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	cmd.Stdout = &testWriter{t: t, prefix: job.Name + ": "}
	cmd.Stderr = &testWriter{t: t, prefix: job.Name + " ERR: "}
	if err := cmd.Run(); err != nil {
		t.Fatalf("spark-sql job %q: %v", job.Name, err)
	}
}

// testWriter streams command output to t.Log line by line.
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

// icebergSparkConf returns the standard Spark conf for Iceberg on the default
// JaisCloud host.
func icebergSparkConf() []string {
	return icebergSparkConfForHost(jaiscloudHost())
}

// icebergSparkConfForHost returns Spark conf for Iceberg pointing at a specific
// JaisCloud host. Replaces localhost with host.docker.internal so the Docker
// container can reach the host's GCS HTTP surface (:8080).
//
// Two destinations are wired:
//   - Hive Metastore (Thrift, :9083) — Iceberg's HiveCatalog via SparkCatalog.
//   - GCS (JSON API, :8080) — both Iceberg's HadoopFileIO data path AND Spark's
//     INSERT OVERWRITE DIRECTORY use the gcs-connector's gs:// filesystem.
//
// The conf block below deliberately omits one setting: lock-enabled is left at
// its default (true) so Spark runs on the real jc_hms_locks path.
func icebergSparkConfForHost(host string) []string {
	dockerHost := strings.Replace(host, "localhost", "host.docker.internal", 1)
	return []string{
		"spark.sql.catalog.hms=org.apache.iceberg.spark.SparkCatalog",
		"spark.sql.catalog.hms.catalog-impl=org.apache.iceberg.hive.HiveCatalog",
		"spark.sql.catalog.hms.uri=thrift://" + hmsEndpoint(),
		fmt.Sprintf("spark.sql.catalog.hms.warehouse=gs://iceberg-warehouse/%s/", testRunID),
		"spark.sql.catalog.hms.io-impl=org.apache.iceberg.hadoop.HadoopFileIO",
		// NOTE: no lock-enabled override. Running on the default
		// iceberg.engine.hive.lock-enabled=true path, backed by the real
		// jc_hms_locks state machine. lock-enabled=false is a fallback only.

		// GCS Hadoop connector (gs:// filesystem) — HadoopFileIO data path AND
		// INSERT OVERWRITE DIRECTORY both flow through here.
		"spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
		"spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
		"spark.hadoop.fs.gs.project.id=" + gcsProjectID(),
		"spark.hadoop.fs.gs.auth.service.account.enable=false",
		"spark.hadoop.fs.gs.auth.null.enable=true",
		// The gcs-connector's gcsio layer does not honour STORAGE_EMULATOR_HOST;
		// it needs the explicit JSON-API root URL.
		"spark.hadoop.fs.gs.storage.root.url=" + dockerHost,
	}
}

// ─── GCS REST assertion helpers ───────────────────────────────────────────────
//
// Modeled on tests/persistent_mode/gcp/storage/persistence_test.go (raw HTTP
// against /storage/v1/...) rather than the AWS S3 SDK. There is deliberately no
// Go-side Hive metastore client: metadata_location/table_type assertions are
// replaced by GCS object-count assertions.

// gcsRequest performs a raw HTTP request against the emulator's GCS REST surface.
func gcsRequest(host, method, path, body string) (int, []byte, error) {
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, host+path, rd)
	if err != nil {
		return 0, nil, err
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, data, nil
}

// gcsObjectPath percent-encodes each path segment of an object name while
// leaving the "/" separators literal (the codec splits on "/" and re-joins).
func gcsObjectPath(object string) string {
	segs := strings.Split(object, "/")
	for i, s := range segs {
		segs[i] = url.PathEscape(s)
	}
	return strings.Join(segs, "/")
}

// listGCSObjects returns the "items" array from the GCS JSON list endpoint for
// a bucket+prefix.
func listGCSObjects(t *testing.T, host, bucket, prefix string) []map[string]any {
	t.Helper()
	path := fmt.Sprintf("/storage/v1/b/%s/o?prefix=%s",
		url.PathEscape(bucket), url.QueryEscape(prefix))
	status, data, err := gcsRequest(host, http.MethodGet, path, "")
	if err != nil {
		t.Fatalf("list gs://%s/%s: %v", bucket, prefix, err)
	}
	if status != http.StatusOK {
		t.Fatalf("list gs://%s/%s: HTTP %d: %s", bucket, prefix, status, data)
	}
	var parsed struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("parse list gs://%s/%s: %v", bucket, prefix, err)
	}
	return parsed.Items
}

// countGCSObjects counts objects under a prefix.
func countGCSObjects(t *testing.T, host, bucket, prefix string) int {
	t.Helper()
	return len(listGCSObjects(t, host, bucket, prefix))
}

// hasGCSObjects returns true if any object exists under the given prefix.
func hasGCSObjects(t *testing.T, host, bucket, prefix string) bool {
	return countGCSObjects(t, host, bucket, prefix) > 0
}

// readGCSBytes fetches an object's bytes via alt=media.
func readGCSBytes(t *testing.T, host, bucket, object string) []byte {
	t.Helper()
	path := fmt.Sprintf("/storage/v1/b/%s/o/%s?alt=media",
		url.PathEscape(bucket), gcsObjectPath(object))
	status, data, err := gcsRequest(host, http.MethodGet, path, "")
	if err != nil {
		t.Fatalf("get gs://%s/%s: %v", bucket, object, err)
	}
	if status != http.StatusOK {
		t.Fatalf("get gs://%s/%s: HTTP %d: %s", bucket, object, status, data)
	}
	return data
}

// findGCSObjectName lists objects under prefix and returns the name of the first
// data object (skipping _SUCCESS and hidden files). Spark's INSERT OVERWRITE
// DIRECTORY writes UUID-named files (e.g. part-00000-<uuid>.c000.json), so this
// is more robust than a hardcoded key.
func findGCSObjectName(t *testing.T, host, bucket, prefix string) string {
	t.Helper()
	items := listGCSObjects(t, host, bucket, prefix)
	for _, obj := range items {
		name, _ := obj["name"].(string)
		lastSlash := strings.LastIndex(name, "/")
		base := name[lastSlash+1:]
		if strings.HasPrefix(base, "_") || strings.HasPrefix(base, ".") || base == "" {
			continue
		}
		return name
	}
	t.Fatalf("no data objects found at gs://%s/%s (found %d total)", bucket, prefix, len(items))
	return ""
}

// findGCSText lists objects under prefix and returns the first data file's
// content as a trimmed string.
func findGCSText(t *testing.T, host, bucket, prefix string) string {
	t.Helper()
	name := findGCSObjectName(t, host, bucket, prefix)
	return strings.TrimSpace(string(readGCSBytes(t, host, bucket, name)))
}

// findGCSJSON lists objects under prefix, reads the first data file, and decodes
// it as a single JSON object.
func findGCSJSON(t *testing.T, host, bucket, prefix string) map[string]any {
	t.Helper()
	name := findGCSObjectName(t, host, bucket, prefix)
	body := readGCSBytes(t, host, bucket, name)
	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("parse JSON from gs://%s/%s: %v\nbody: %s", bucket, name, err, body)
	}
	return result
}

// createGCSBucket creates a bucket via the GCS REST API (idempotent; errors are
// ignored because the bucket may already exist). Used by TestMain and the
// persistence test, which have no *testing.T in scope for the setup step.
func createGCSBucket(host, bucket string) {
	body := fmt.Sprintf(`{"name":%q}`, bucket)
	_, _, _ = gcsRequest(host, http.MethodPost, "/storage/v1/b?project=proj", body)
}

// ─── persistence server helpers ──────────────────────────────────────────────

// gcpBin returns the path to the jaiscloud-gcp binary. Tests run with the
// working directory set to tests/persistent_mode/gcp/iceberg/, so the project
// root binary is four levels up. JAISCLOUD_GCP_BIN overrides.
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

// persistPort returns the port used by the managed JaisCloud GCP process in the
// persistence test.
func persistPort() int {
	if v := os.Getenv("JAISCLOUD_GCP_PERSIST_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			return p
		}
	}
	return 8098
}

// startGCPProcess starts a managed jaiscloud-gcp subprocess and returns the
// exec.Cmd. Callers are responsible for killing it when done.
//
// There is deliberately no --mode flag: the GCP binary persists via --dsn +
// --blob-dir (the AWS --mode full invocation is dead code on its own binary).
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

// waitForHealth polls /_jaiscloud/health until the server responds 200 or times out.
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

// stopProcess kills the managed server and waits for the port to be released.
func stopProcess(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill jaiscloud-gcp: %v", err)
	}
	_ = cmd.Wait()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if !portInUse(persistPort()) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("jaiscloud-gcp did not release the port after kill")
}

func portInUse(port int) bool {
	c, err := http.Get(fmt.Sprintf("http://localhost:%d/_jaiscloud/health", port))
	if err != nil {
		return false
	}
	c.Body.Close()
	return true
}
