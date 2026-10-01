//go:build gcp_persistence

// Package parity_test extends the GCP Postgres persistence-parity coverage to
// every REST provider that the existing core/dataproc/storage/iceberg suites do
// not already exercise. For each probed service it:
//
//  1. seeds a representative resource over REST (names are unique per run, so
//     re-runs are idempotent and never collide with a previous run's rows),
//  2. restarts jaiscloud-gcp against the SAME --dsn and asserts the resource
//     survived the restart,
//  3. POSTs /_jaiscloud/reset and asserts the resource is gone (resetter parity).
//
// The probe inventory below covers the remainder of internal/gcp/provider/:
//
//	covered elsewhere (not probed here):
//	  pubsub, secretmanager, kms, iam   — tests/persistent_mode/gcp/core
//	  dataproc                          — tests/persistent_mode/gcp/dataproc
//	  storage                           — tests/persistent_mode/gcp/storage
//	  iceberg                           — tests/persistent_mode/gcp/iceberg
//
//	probed here:
//	  bigquery, clouddns, cloudsql, compute, eventarc, firestore, functions,
//	  managedkafka, memorystore, metastore, scheduler, tasks, workflowexecutions, workflows
//
// No provider service is skipped: each has a simple, schema-correct
// REST create/read pair. Services whose data lives in the shared ResourceStore
// (clouddns, cloudsql, compute, memorystore) are cleared by the resources
// resetter; the rest are cleared by their own store resetter.
//
// Required env:
//
//	JAISCLOUD_DSN — PostgreSQL DSN
//
// Optional env:
//
//	JAISCLOUD_GCP_BIN          — path to the jaiscloud-gcp binary
//	JAISCLOUD_GCP_PERSIST_PORT — port for the managed server (default 8099)
package parity_test

import (
	"encoding/json"
	"errors"
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
)

const (
	project  = "parity-proj"
	location = "us-central1"
)

// probe seeds one representative resource for a service and returns two checks:
// survived (run after a restart with the same DSN; must succeed) and cleared
// (run after POST /_jaiscloud/reset; must succeed, i.e. it asserts absence).
type probe struct {
	service string
	seed    func(d *driver, suffix string) (survived, cleared func() error, err error)
}

// probes is the table of REST services under test. Each entry is independent and
// self-describing; failures are reported per service per phase.
var probes = []probe{
	{"bigquery", seedBigQuery},
	{"clouddns", seedCloudDNS},
	{"cloudsql", seedCloudSQL},
	{"compute", seedCompute},
	{"eventarc", seedEventarc},
	{"firestore", seedFirestore},
	{"functions", seedFunctions},
	{"managedkafka", seedManagedKafka},
	{"memorystore", seedMemorystore},
	{"metastore", seedMetastore},
	{"scheduler", seedScheduler},
	{"tasks", seedTasks},
	{"workflowexecutions", seedWorkflowExecutions},
	{"workflows", seedWorkflows},
}

// driver wraps the running emulator's base URL and the HTTP client used by all
// probes. Closures returned by seed capture the driver, so they keep working
// across the restart (the port is stable).
type driver struct {
	base   string
	client *http.Client
}

func (d *driver) do(method, path, body string) (int, string, error) {
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, d.base+path, rd)
	if err != nil {
		return 0, "", err
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, "", err
	}
	return resp.StatusCode, string(raw), nil
}

// expect performs a request and fails the seed when the status is not want.
func (d *driver) expect(method, path, body string, want int) error {
	code, resp, err := d.do(method, path, body)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	if code != want {
		return fmt.Errorf("%s %s: got HTTP %d (want %d): %s", method, path, code, want, truncate(resp))
	}
	return nil
}

// verifyPresent returns a check that the resource at path is readable (HTTP 200).
func (d *driver) verifyPresent(path string) func() error {
	return func() error {
		code, resp, err := d.do("GET", path, "")
		if err != nil {
			return fmt.Errorf("GET %s: %w", path, err)
		}
		if code != http.StatusOK {
			return fmt.Errorf("GET %s: got HTTP %d (want 200): %s", path, code, truncate(resp))
		}
		return nil
	}
}

// verifyGone returns a check that the resource at path is absent (HTTP 404).
func (d *driver) verifyGone(path string) func() error {
	return func() error {
		code, resp, err := d.do("GET", path, "")
		if err != nil {
			return fmt.Errorf("GET %s: %w", path, err)
		}
		if code != http.StatusNotFound {
			return fmt.Errorf("GET %s after reset: got HTTP %d (want 404): %s", path, code, truncate(resp))
		}
		return nil
	}
}

func jsonBody(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("parity: marshal probe body: %v", err))
	}
	return string(b)
}

func truncate(s string) string {
	const max = 300
	if len(s) > max {
		return s[:max] + "…"
	}
	return s
}

// ── per-service seeds ────────────────────────────────────────────────────────

// seedBigQuery creates a dataset + table, streams rows through
// tabledata.insertAll, and executes a SQL job over them. The survived check
// asserts the table and its streamed rows still answer an executed query after
// a restart; the cleared check asserts the dataset is gone after reset.
func seedBigQuery(d *driver, suffix string) (func() error, func() error, error) {
	id := "ds-" + suffix
	tbl := "t-" + suffix
	post := fmt.Sprintf("/bigquery/v2/projects/%s/datasets", project)
	if err := d.expect("POST", post, jsonBody(map[string]any{
		"datasetReference": map[string]any{"projectId": project, "datasetId": id},
	}), http.StatusOK); err != nil {
		return nil, nil, err
	}
	tables := post + "/" + id + "/tables"
	if err := d.expect("POST", tables, jsonBody(map[string]any{
		"tableReference": map[string]any{"projectId": project, "datasetId": id, "tableId": tbl},
		"schema": map[string]any{"fields": []any{
			map[string]any{"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
			map[string]any{"name": "name", "type": "STRING", "mode": "NULLABLE"},
		}},
	}), http.StatusOK); err != nil {
		return nil, nil, err
	}
	if err := d.expect("POST", tables+"/"+tbl+"/insertAll", jsonBody(map[string]any{
		"rows": []any{
			map[string]any{"insertId": "1", "json": map[string]any{"id": 1, "name": "alice"}},
			map[string]any{"insertId": "2", "json": map[string]any{"id": 2, "name": "bob"}},
		},
	}), http.StatusOK); err != nil {
		return nil, nil, err
	}

	// Executing a query is the strongest survival assertion: it requires the
	// dataset, the table, its schema and every streamed row to have survived.
	queryBody := jsonBody(map[string]any{
		"query":        fmt.Sprintf("SELECT name FROM `%s.%s.%s` ORDER BY id", project, id, tbl),
		"useLegacySql": false,
	})
	runQuery := func() error {
		code, body, err := d.do("POST", fmt.Sprintf("/bigquery/v2/projects/%s/queries", project), queryBody)
		if err != nil {
			return fmt.Errorf("bigquery query: %w", err)
		}
		if code != http.StatusOK {
			return fmt.Errorf("bigquery query: got HTTP %d (want 200): %s", code, truncate(body))
		}
		var resp struct {
			TotalRows string `json:"totalRows"`
			Rows      []struct {
				F []struct {
					V string `json:"v"`
				} `json:"f"`
			} `json:"rows"`
		}
		if err := json.Unmarshal([]byte(body), &resp); err != nil {
			return fmt.Errorf("parse query response: %w: %s", err, truncate(body))
		}
		if resp.TotalRows != "2" || len(resp.Rows) != 2 {
			return fmt.Errorf("query returned totalRows=%q rows=%d, want 2", resp.TotalRows, len(resp.Rows))
		}
		if resp.Rows[0].F[0].V != "alice" || resp.Rows[1].F[0].V != "bob" {
			return fmt.Errorf("query rows = %+v, want alice,bob", resp.Rows)
		}
		return nil
	}

	getDataset := post + "/" + id
	getTable := tables + "/" + tbl
	survived := func() error {
		if err := d.verifyPresent(getDataset)(); err != nil {
			return err
		}
		if err := d.verifyPresent(getTable)(); err != nil {
			return err
		}
		return runQuery()
	}
	return survived, d.verifyGone(getDataset), nil
}

// seedCloudDNS creates a managed zone (dns/v1 projects.managedZones.create).
func seedCloudDNS(d *driver, suffix string) (func() error, func() error, error) {
	name := "zone-" + suffix
	post := fmt.Sprintf("/dns/v1/projects/%s/managedZones", project)
	if err := d.expect("POST", post, jsonBody(map[string]any{
		"name":    name,
		"dnsName": name + ".example.com.",
	}), http.StatusOK); err != nil {
		return nil, nil, err
	}
	get := post + "/" + name
	return d.verifyPresent(get), d.verifyGone(get), nil
}

// seedCloudSQL creates an instance (sql/v1beta4 projects.instances.insert).
func seedCloudSQL(d *driver, suffix string) (func() error, func() error, error) {
	name := "sql-" + suffix
	post := fmt.Sprintf("/sql/v1beta4/projects/%s/instances", project)
	if err := d.expect("POST", post, jsonBody(map[string]any{
		"name":            name,
		"region":          location,
		"databaseVersion": "MYSQL_8_0",
		"settings":        map[string]any{"tier": "db-n1-standard-1"},
	}), http.StatusOK); err != nil {
		return nil, nil, err
	}
	get := post + "/" + name
	return d.verifyPresent(get), d.verifyGone(get), nil
}

// seedCompute creates a global network (compute/v1 projects.global.networks.insert).
func seedCompute(d *driver, suffix string) (func() error, func() error, error) {
	name := "net-" + suffix
	post := fmt.Sprintf("/compute/v1/projects/%s/global/networks", project)
	if err := d.expect("POST", post, jsonBody(map[string]any{
		"name":                  name,
		"autoCreateSubnetworks": false,
	}), http.StatusOK); err != nil {
		return nil, nil, err
	}
	get := post + "/" + name
	return d.verifyPresent(get), d.verifyGone(get), nil
}

// seedEventarc creates a trigger (v1 projects.locations.triggers.create).
func seedEventarc(d *driver, suffix string) (func() error, func() error, error) {
	id := "tr-" + suffix
	post := fmt.Sprintf("/v1/projects/%s/locations/%s/triggers?triggerId=%s", project, location, url.QueryEscape(id))
	if err := d.expect("POST", post, jsonBody(map[string]any{
		"destination": map[string]any{
			"cloudRun": map[string]any{"service": "svc", "region": location},
		},
		"eventFilters": []any{
			map[string]any{"attribute": "type", "value": "google.cloud.pubsub.topic.v1.messagePublished"},
		},
	}), http.StatusOK); err != nil {
		return nil, nil, err
	}
	get := fmt.Sprintf("/v1/projects/%s/locations/%s/triggers/%s", project, location, id)
	return d.verifyPresent(get), d.verifyGone(get), nil
}

// seedFirestore creates a document (v1 projects.databases.documents.createDocument).
func seedFirestore(d *driver, suffix string) (func() error, func() error, error) {
	coll := "coll-" + suffix
	doc := "doc-" + suffix
	post := fmt.Sprintf("/v1/projects/%s/databases/(default)/documents/%s?documentId=%s",
		project, coll, url.QueryEscape(doc))
	if err := d.expect("POST", post, jsonBody(map[string]any{
		"fields": map[string]any{"name": map[string]any{"stringValue": "hello"}},
	}), http.StatusOK); err != nil {
		return nil, nil, err
	}
	get := fmt.Sprintf("/v1/projects/%s/databases/(default)/documents/%s/%s", project, coll, doc)
	return d.verifyPresent(get), d.verifyGone(get), nil
}

// seedFunctions creates a function (v1 projects.locations.functions.create) and
// verifies that both the function and its persisted long-running operation
// survive a restart (and clear on reset).
func seedFunctions(d *driver, suffix string) (func() error, func() error, error) {
	id := "fn-" + suffix
	name := fmt.Sprintf("projects/%s/locations/%s/functions/%s", project, location, id)
	post := fmt.Sprintf("/v1/projects/%s/locations/%s/functions?functionId=%s", project, location, url.QueryEscape(id))
	code, resp, err := d.do("POST", post, jsonBody(map[string]any{
		"name":       name,
		"runtime":    "nodejs20",
		"entryPoint": "helloWorld",
	}))
	if err != nil {
		return nil, nil, err
	}
	if code != http.StatusOK {
		return nil, nil, fmt.Errorf("POST %s: got HTTP %d (want 200): %s", post, code, truncate(resp))
	}
	var op struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal([]byte(resp), &op); err != nil || op.Name == "" {
		return nil, nil, fmt.Errorf("POST %s: no operation name in response: %s", post, truncate(resp))
	}
	get := fmt.Sprintf("/v1/projects/%s/locations/%s/functions/%s", project, location, id)
	// The v1 operation is named top-level "operations/{id}" and is read back
	// over /v1/operations/{id}; a location-scoped name (other services / older
	// shape) is read over /v2/. Follow the name the API actually handed out.
	opPath := op.Name
	if !strings.HasPrefix(opPath, "/") {
		if strings.HasPrefix(opPath, "operations/") {
			opPath = "/v1/" + opPath
		} else {
			opPath = "/v2/" + opPath
		}
	}
	survived := func() error {
		if err := d.verifyPresent(get)(); err != nil {
			return err
		}
		return d.verifyPresent(opPath)()
	}
	cleared := func() error {
		if err := d.verifyGone(get)(); err != nil {
			return err
		}
		return d.verifyGone(opPath)()
	}
	return survived, cleared, nil
}

// seedManagedKafka creates a cluster (v1 projects.locations.clusters.create).
func seedScheduler(d *driver, suffix string) (func() error, func() error, error) {
	id := "sched-" + suffix
	post := fmt.Sprintf("/v1/projects/%s/locations/%s/jobs", project, location)
	if err := d.expect("POST", post, jsonBody(map[string]any{
		"name":     fmt.Sprintf("projects/%s/locations/%s/jobs/%s", project, location, id),
		"schedule": "* * * * *",
		"timeZone": "UTC",
		"httpTarget": map[string]any{
			"uri":        "http://example.test/hook",
			"httpMethod": "GET",
		},
	}), http.StatusOK); err != nil {
		return nil, nil, err
	}
	get := fmt.Sprintf("/v1/projects/%s/locations/%s/jobs/%s", project, location, id)
	return d.verifyPresent(get), d.verifyGone(get), nil
}

// seedTasks creates a Cloud Tasks queue (v2 projects.locations.queues.create)
// and a task under it, then verifies both survive a restart and are cleared by
// reset (the queue delete cascades to the task).
func seedTasks(d *driver, suffix string) (func() error, func() error, error) {
	id := "tasks-" + suffix
	queuePath := fmt.Sprintf("/v2/projects/%s/locations/%s/queues/%s", project, location, id)
	post := fmt.Sprintf("/v2/projects/%s/locations/%s/queues", project, location)
	if err := d.expect("POST", post, jsonBody(map[string]any{
		"name": fmt.Sprintf("projects/%s/locations/%s/queues/%s", project, location, id),
	}), http.StatusOK); err != nil {
		return nil, nil, err
	}
	taskID := "task-" + suffix
	taskPath := queuePath + "/tasks/" + taskID
	if err := d.expect("POST", queuePath+"/tasks", jsonBody(map[string]any{
		"name":        fmt.Sprintf("projects/%s/locations/%s/queues/%s/tasks/%s", project, location, id, taskID),
		"httpRequest": map[string]any{"url": "http://example.test/hook", "httpMethod": "GET"},
	}), http.StatusOK); err != nil {
		return nil, nil, err
	}
	survived := func() error {
		if err := d.verifyPresent(queuePath)(); err != nil {
			return err
		}
		return d.verifyPresent(taskPath)()
	}
	cleared := func() error {
		if err := d.verifyGone(queuePath)(); err != nil {
			return err
		}
		return d.verifyGone(taskPath)()
	}
	return survived, cleared, nil
}

func seedManagedKafka(d *driver, suffix string) (func() error, func() error, error) {
	id := "kafka-" + suffix
	post := fmt.Sprintf("/v1/projects/%s/locations/%s/clusters?clusterId=%s", project, location, url.QueryEscape(id))
	if err := d.expect("POST", post, jsonBody(map[string]any{
		"capacityConfig": map[string]any{"vcpuCount": 3, "memoryBytes": 3221225472},
	}), http.StatusOK); err != nil {
		return nil, nil, err
	}
	get := fmt.Sprintf("/v1/projects/%s/locations/%s/clusters/%s", project, location, id)
	return d.verifyPresent(get), d.verifyGone(get), nil
}

// seedMemorystore creates a Redis instance (v1 projects.locations.instances.create).
func seedMemorystore(d *driver, suffix string) (func() error, func() error, error) {
	id := "redis-" + suffix
	post := fmt.Sprintf("/v1/projects/%s/locations/%s/instances?instanceId=%s", project, location, url.QueryEscape(id))
	if err := d.expect("POST", post, jsonBody(map[string]any{
		"tier":         "BASIC",
		"memorySizeGb": 1,
		"redisVersion": "REDIS_7_0",
	}), http.StatusOK); err != nil {
		return nil, nil, err
	}
	get := fmt.Sprintf("/v1/projects/%s/locations/%s/instances/%s", project, location, id)
	return d.verifyPresent(get), d.verifyGone(get), nil
}

// seedMetastore creates a service (v1 projects.locations.services.create).
func seedMetastore(d *driver, suffix string) (func() error, func() error, error) {
	id := "ms-" + suffix
	post := fmt.Sprintf("/v1/projects/%s/locations/%s/services?serviceId=%s", project, location, url.QueryEscape(id))
	if err := d.expect("POST", post, jsonBody(map[string]any{
		"hiveMetastoreConfig": map[string]any{"version": "3.1.2"},
	}), http.StatusOK); err != nil {
		return nil, nil, err
	}
	get := fmt.Sprintf("/v1/projects/%s/locations/%s/services/%s", project, location, id)
	return d.verifyPresent(get), d.verifyGone(get), nil
}

// seedWorkflows creates a workflow (v1 projects.locations.workflows.create).
func seedWorkflows(d *driver, suffix string) (func() error, func() error, error) {
	id := "wf-" + suffix
	post := fmt.Sprintf("/v1/projects/%s/locations/%s/workflows?workflowId=%s", project, location, url.QueryEscape(id))
	if err := d.expect("POST", post, jsonBody(map[string]any{
		"sourceContents": "main:\n  steps:\n    - done:\n        return: \"ok\"\n",
	}), http.StatusOK); err != nil {
		return nil, nil, err
	}
	get := fmt.Sprintf("/v1/projects/%s/locations/%s/workflows/%s", project, location, id)
	return d.verifyPresent(get), d.verifyGone(get), nil
}

// seedWorkflowExecutions creates a workflow plus one execution of it. The
// execution ID is generated server-side, so the returned checks target the
// concrete execution resource name from the create response.
func seedWorkflowExecutions(d *driver, suffix string) (func() error, func() error, error) {
	id := "wf-exec-" + suffix
	workflows := fmt.Sprintf("/v1/projects/%s/locations/%s/workflows", project, location)
	if err := d.expect("POST", workflows+"?workflowId="+url.QueryEscape(id), jsonBody(map[string]any{
		"sourceContents": "main:\n  steps:\n    - done:\n        return: \"ok\"\n",
	}), http.StatusOK); err != nil {
		return nil, nil, err
	}

	execs := fmt.Sprintf("%s/%s/executions", workflows, id)
	code, resp, err := d.do("POST", execs, `{"argument":"{}"}`)
	if err != nil {
		return nil, nil, fmt.Errorf("POST %s: %w", execs, err)
	}
	if code != http.StatusOK {
		return nil, nil, fmt.Errorf("POST %s: got HTTP %d (want 200): %s", execs, code, truncate(resp))
	}
	var created struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal([]byte(resp), &created); err != nil {
		return nil, nil, fmt.Errorf("decode execution create response: %w", err)
	}
	if created.Name == "" {
		return nil, nil, errors.New("execution create response omitted name")
	}
	get := "/v1/" + created.Name
	return d.verifyPresent(get), d.verifyGone(get), nil
}

// ── process harness (mirrors tests/persistent_mode/gcp/core, self-contained) ──

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
	if v := os.Getenv("JAISCLOUD_GCP_PERSIST_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil {
			return p
		}
	}
	return 8099
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

func waitForHealth(t *testing.T, base string) {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(base + "/_jaiscloud/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("jaiscloud-gcp at %s did not become healthy within 30s", base)
}

func stopProcess(t *testing.T, cmd *exec.Cmd, port int) {
	t.Helper()
	if cmd == nil || cmd.Process == nil {
		return
	}
	if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("kill jaiscloud-gcp: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(fmt.Sprintf("http://localhost:%d/_jaiscloud/health", port))
		if err != nil {
			return
		}
		resp.Body.Close()
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("jaiscloud-gcp did not release port %d after kill", port)
}

// TestPersistenceParity seeds every probe, restarts the emulator against the
// same DSN, asserts survival, resets, and asserts the resources are gone.
func TestPersistenceParity(t *testing.T) {
	dsn := os.Getenv("JAISCLOUD_DSN")
	if dsn == "" {
		t.Skip("JAISCLOUD_DSN not set — skipping persistence parity test")
	}

	port := persistPort()
	base := fmt.Sprintf("http://localhost:%d", port)
	blobDir := t.TempDir()
	d := &driver{base: base, client: &http.Client{Timeout: 30 * time.Second}}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)

	// ── Phase 1: seed against emulator #1 ────────────────────────────────────
	proc1 := startGCPProcess(t, port, dsn, blobDir)
	stopped1 := false
	defer func() {
		if !stopped1 {
			stopProcess(t, proc1, port)
		}
	}()
	waitForHealth(t, base)

	type seeded struct {
		service  string
		survived func() error
		cleared  func() error
	}
	var seeds []seeded
	for _, p := range probes {
		survived, cleared, err := p.seed(d, suffix)
		if err != nil {
			t.Errorf("seed %s: %v", p.service, err)
			continue
		}
		seeds = append(seeds, seeded{p.service, survived, cleared})
	}

	stopProcess(t, proc1, port)
	stopped1 = true

	// ── Phase 2: restart against the same backend ────────────────────────────
	proc2 := startGCPProcess(t, port, dsn, blobDir)
	defer stopProcess(t, proc2, port)
	waitForHealth(t, base)

	for _, s := range seeds {
		s := s
		t.Run(s.service+"/survived-restart", func(t *testing.T) {
			if err := s.survived(); err != nil {
				t.Errorf("resource did not survive restart: %v", err)
			}
		})
	}

	// ── Phase 3: reset parity ────────────────────────────────────────────────
	if code, body, err := d.do("POST", "/_jaiscloud/reset", ""); err != nil {
		t.Fatalf("POST /_jaiscloud/reset: %v", err)
	} else if code != http.StatusOK {
		t.Fatalf("POST /_jaiscloud/reset: got HTTP %d: %s", code, truncate(body))
	}

	for _, s := range seeds {
		s := s
		t.Run(s.service+"/cleared-by-reset", func(t *testing.T) {
			if err := s.cleared(); err != nil {
				t.Errorf("resource not cleared by reset: %v", err)
			}
		})
	}
}
