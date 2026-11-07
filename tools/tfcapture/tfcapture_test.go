package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	providerschema "jaiscloud/tools/providerschema"
)

// TestEmptyLabelIsDroppedClientSide pins the TGM15 calibration case with a real
// captured request: the module configures labels = { team = "", env = "probe" }
// and the provider's create body carries only env (plus its own provisioned
// marker). The emulator therefore never sees `team`, and the gate must classify
// the attribute as served rather than blaming the emulator for the dropped
// value.
func TestEmptyLabelIsDroppedClientSide(t *testing.T) {
	logText, err := os.ReadFile(filepath.Join("testdata", "tgm15-empty-label", "tf-debug.log"))
	if err != nil {
		t.Fatalf("read fixture log: %v", err)
	}
	requests := ParseDebugLog(string(logText))
	if len(requests) != 1 {
		t.Fatalf("parsed %d requests, want 1", len(requests))
	}
	req := requests[0]
	if req.Method != "POST" || ResourceForPath(req.Method, req.Path) != "google_storage_bucket" {
		t.Fatalf("request = %s %s, want POST google_storage_bucket create", req.Method, req.Path)
	}
	labels, ok := req.Body["labels"].(map[string]any)
	if !ok {
		t.Fatalf("create body has no labels map: %v", req.Body)
	}
	if _, supplied := labels["team"]; supplied {
		t.Fatalf("captured labels carry team, want the provider to drop the empty value (TGM15): %v", labels)
	}
	if labels["env"] != "probe" {
		t.Fatalf("captured labels lost the non-empty value: %v", labels)
	}

	schema, err := providerschema.Load(filepath.Join("testdata", "tgm15-empty-label", "schema.json"))
	if err != nil {
		t.Fatalf("load schema fixture: %v", err)
	}
	mapping, err := loadMapping(filepath.Join("..", "..", "docs", "fidelity", "provider-attributes.csv"))
	if err != nil {
		t.Fatalf("load mapping: %v", err)
	}
	catalog, err := loadCatalog(filepath.Join("..", "..", "docs", "fidelity", "field-coverage.csv"))
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	report := Check("tgm15-empty-label", requests, schema, mapping, catalog, false, false)
	if report.Failed() {
		t.Fatalf("empty-label capture failed the gate; the provider drop is client-side, not an emulator gap:\n%s", report.Render())
	}
	if report.Sent["google_storage_bucket"] == 0 {
		t.Fatalf("no sent attributes were joined; the fixture is not exercising the join:\n%s", report.Render())
	}
}

// TestParseDebugLogReadsBodyAfterHeaders pins the parser against the real
// provider log shape: the google provider logs headers and the JSON body with
// no separating blank line.
func TestParseDebugLogReadsBodyAfterHeaders(t *testing.T) {
	const log = `2026-11-05T17:47:42.957-0800 [DEBUG] provider.terraform-provider-google_v7.46.2_x5: ---[ REQUEST ]---------------------------------------
2026-11-05T17:47:42.957-0800 [DEBUG] provider.terraform-provider-google_v7.46.2_x5: PUT /v1/projects/p/topics/t?alt=json HTTP/1.1
2026-11-05T17:47:42.957-0800 [DEBUG] provider.terraform-provider-google_v7.46.2_x5: Content-Type: application/json
2026-11-05T17:47:42.957-0800 [DEBUG] provider.terraform-provider-google_v7.46.2_x5: {
2026-11-05T17:47:42.957-0800 [DEBUG] provider.terraform-provider-google_v7.46.2_x5:  "labels": {"a": "b"}
2026-11-05T17:47:42.957-0800 [DEBUG] provider.terraform-provider-google_v7.46.2_x5: }
2026-11-05T17:47:42.957-0800 [DEBUG] provider.terraform-provider-google_v7.46.2_x5: -----------------------------------------------------
`
	reqs := ParseDebugLog(log)
	if len(reqs) != 1 {
		t.Fatalf("parsed %d requests, want 1", len(reqs))
	}
	if reqs[0].Method != "PUT" || reqs[0].Path != "/v1/projects/p/topics/t?alt=json" {
		t.Fatalf("request line = %s %s", reqs[0].Method, reqs[0].Path)
	}
	labels, _ := reqs[0].Body["labels"].(map[string]any)
	if labels["a"] != "b" {
		t.Fatalf("body = %v, want labels.a=b", reqs[0].Body)
	}
	if ResourceForPath(reqs[0].Method, reqs[0].Path) != "google_pubsub_topic" {
		t.Fatalf("path mapping failed for %s", reqs[0].Path)
	}
}

// ─── gate failure modes ─────────────────────────────────────────────────────

func testSchema(attrs ...string) *providerschema.ProviderSchema {
	specs := make([]providerschema.Attribute, 0, len(attrs))
	for _, a := range attrs {
		specs = append(specs, providerschema.Attribute{Path: a, Optional: true})
	}
	return &providerschema.ProviderSchema{Resources: map[string][]providerschema.Attribute{
		"google_storage_bucket": specs,
	}}
}

func testCatalog(t *testing.T, coverage string) map[catalogKey]string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "catalog.csv")
	body := "service,transport,resource,field,behavior,coverage\nstorage,rest,Bucket,labels,,roundtrip\n"
	if coverage != "" {
		body = "service,transport,resource,field,behavior,coverage\nstorage,rest,Bucket,labels,," + coverage + "\n"
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	catalog, err := loadCatalog(path)
	if err != nil {
		t.Fatalf("loadCatalog: %v", err)
	}
	return catalog
}

func TestSentButUnsupportedFails(t *testing.T) {
	reqs := []Request{{Method: "POST", Path: "/storage/v1/b", Body: map[string]any{"labels": map[string]any{"a": "b"}}}}
	mapping := map[mappingKey]mappingRow{
		{"google_storage_bucket", "labels"}: {providerResource: "google_storage_bucket", attribute: "labels", apiField: "labels", class: Unsupported, reason: "not modelled"},
	}
	report := Check("x", reqs, testSchema("labels"), mapping, testCatalog(t, ""), false, false)
	if !report.Failed() {
		t.Fatalf("sent-but-unsupported capture passed:\n%s", report.Render())
	}
}

func TestSentWithMissingCatalogRowFails(t *testing.T) {
	reqs := []Request{{Method: "POST", Path: "/storage/v1/b", Body: map[string]any{"labels": map[string]any{"a": "b"}}}}
	mapping := map[mappingKey]mappingRow{
		{"google_storage_bucket", "labels"}: {providerResource: "google_storage_bucket", catalogService: "storage", catalogResource: "OtherResource", attribute: "labels", apiField: "labels", class: Served},
	}
	report := Check("x", reqs, testSchema("labels"), mapping, testCatalog(t, ""), false, false)
	if !report.Failed() {
		t.Fatalf("sent field with no catalog row passed:\n%s", report.Render())
	}
}

func TestUnclassifiedAttributeFails(t *testing.T) {
	report := Check("x", []Request{{Method: "GET", Path: "/v1/projects/p/topics/t"}}, testSchema("labels", "new_field"), map[mappingKey]mappingRow{
		{"google_storage_bucket", "labels"}: {providerResource: "google_storage_bucket", attribute: "labels", class: NotExercised, reason: "x"},
	}, map[catalogKey]string{}, false, false)
	if !report.Failed() {
		t.Fatalf("unclassified attribute passed:\n%s", report.Render())
	}
}

func TestStaleMappingRowFails(t *testing.T) {
	report := Check("x", []Request{}, testSchema("labels"), map[mappingKey]mappingRow{
		{"google_storage_bucket", "removed_by_provider"}: {providerResource: "google_storage_bucket", attribute: "removed_by_provider", class: NotExercised, reason: "x"},
	}, map[catalogKey]string{}, false, false)
	if !report.Failed() {
		t.Fatalf("stale mapping row passed:\n%s", report.Render())
	}
}

// TestAllowMissingAttributesDowngradesStaleRows pins the escape hatch for a
// case whose consumer module pins an older provider: an attribute the newer
// mapping carries is absent from that case's schema and must stay a note, while
// an unclassified attribute under the same case's schema still fails.
func TestAllowMissingAttributesDowngradesStaleRows(t *testing.T) {
	mapping := map[mappingKey]mappingRow{
		{"google_storage_bucket", "labels"}:               {providerResource: "google_storage_bucket", attribute: "labels", class: NotExercised, reason: "x"},
		{"google_storage_bucket", "added_by_provider_v8"}: {providerResource: "google_storage_bucket", attribute: "added_by_provider_v8", class: NotExercised, reason: "x"},
	}
	report := Check("x", []Request{}, testSchema("labels"), mapping, map[catalogKey]string{}, false, true)
	if report.Failed() {
		t.Fatalf("an older-provider case failed on a post-dated attribute:\n%s", report.Render())
	}
	if len(report.Notes) == 0 {
		t.Fatalf("the absent mapping row was dropped silently; want a note:\n%s", report.Render())
	}

	// The same flag must not hide an attribute the case's schema *does* declare
	// but the mapping does not classify.
	report = Check("x", []Request{}, testSchema("labels", "new_field"),
		map[mappingKey]mappingRow{
			{"google_storage_bucket", "labels"}: {providerResource: "google_storage_bucket", attribute: "labels", class: NotExercised, reason: "x"},
		}, map[catalogKey]string{}, false, true)
	if !report.Failed() {
		t.Fatalf("a declared-but-unclassified attribute passed with -allow-missing-attributes:\n%s", report.Render())
	}
}

// TestScopeMappingKeepsOnlyNamedResources pins -only-resources: another
// resource's rows (and its stale-row failure) must not reach a case that never
// drives it.
func TestScopeMappingKeepsOnlyNamedResources(t *testing.T) {
	mapping := map[mappingKey]mappingRow{
		{"google_storage_bucket", "labels"}:              {providerResource: "google_storage_bucket", attribute: "labels", class: NotExercised, reason: "x"},
		{"google_workflows_workflow", "source_contents"}: {providerResource: "google_workflows_workflow", attribute: "source_contents", class: NotExercised, reason: "x"},
	}
	scoped := scopeMapping(mapping, []string{" google_workflows_workflow "})
	if len(scoped) != 1 {
		t.Fatalf("scoped mapping has %d rows, want 1", len(scoped))
	}
	if _, ok := scoped[mappingKey{"google_workflows_workflow", "source_contents"}]; !ok {
		t.Fatalf("scoped mapping dropped the named resource: %v", scoped)
	}
	report := Check("x", []Request{}, testSchema("labels"), scoped, map[catalogKey]string{}, false, false)
	if report.Failed() {
		t.Fatalf("a scoped case failed on an unscoped resource:\n%s", report.Render())
	}
}

func TestRequireResourcesFailsWhenNothingMapped(t *testing.T) {
	report := Check("x", []Request{{Method: "GET", Path: "/v1/unknown"}}, testSchema("labels"), map[mappingKey]mappingRow{
		{"google_storage_bucket", "labels"}: {providerResource: "google_storage_bucket", attribute: "labels", class: NotExercised, reason: "x"},
	}, map[catalogKey]string{}, true, false)
	if !report.Failed() {
		t.Fatalf("a capture with no mapped resources passed with -require-resources:\n%s", report.Render())
	}
}

func TestCatalogMissingColumnIsLoud(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.csv")
	if err := os.WriteFile(path, []byte("service,resource,field\nstorage,Bucket,labels\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadCatalog(path); err == nil {
		t.Fatal("loadCatalog accepted a catalog without the coverage column")
	}
}

func TestNestedArrayPathIsSent(t *testing.T) {
	body := map[string]any{"lifecycle": map[string]any{"rule": []any{
		map[string]any{"action": map[string]any{"type": "Delete"}},
	}}}
	if !hasJSONPath(body, "lifecycle.rule.action.type") {
		t.Fatal("hasJSONPath missed a path through a JSON array")
	}
}

// TestInitMappingFlushesTheWholeFile pins the csv.Writer flush: with more rows
// than the writer's buffer, the tail used to be lost because the deferred
// Flush ran after the file was closed.
func TestInitMappingFlushesTheWholeFile(t *testing.T) {
	specs := make([]providerschema.Attribute, 0, 500)
	for i := 0; i < 500; i++ {
		specs = append(specs, providerschema.Attribute{Path: fmt.Sprintf("attribute_%03d", i), Optional: true})
	}
	schema := &providerschema.ProviderSchema{Resources: map[string][]providerschema.Attribute{
		"google_test_resource": specs,
	}}
	path := filepath.Join(t.TempDir(), "mapping.csv")
	if err := writeInitialMapping(path, schema, []string{"google_test_resource"}); err != nil {
		t.Fatalf("writeInitialMapping: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read mapping: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != len(specs)+1 {
		t.Fatalf("mapping has %d lines, want header + %d rows (the writer tail was dropped)", len(lines), len(specs))
	}
	if !strings.Contains(lines[len(lines)-1], "attribute_499") {
		t.Fatalf("last mapping row = %q, want attribute_499", lines[len(lines)-1])
	}
}

// TestBigQueryPathRules pins the BigQuery resource attribution: datasets and
// tables create with a collection POST and update with a full-body PUT, while
// tables.insert/tabledata and dataset-scoped routines are not attributed to
// the dataset/table resource.
func TestBigQueryPathRules(t *testing.T) {
	cases := []struct {
		method, path, want string
	}{
		{"POST", "/bigquery/v2/projects/p/datasets?alt=json", "google_bigquery_dataset"},
		{"PUT", "/bigquery/v2/projects/p/datasets/d?alt=json", "google_bigquery_dataset"},
		{"PATCH", "/bigquery/v2/projects/p/datasets/d?updateMask=labels", "google_bigquery_dataset"},
		{"POST", "/bigquery/v2/projects/p/datasets/d/tables?alt=json", "google_bigquery_table"},
		{"PUT", "/bigquery/v2/projects/p/datasets/d/tables/t?alt=json", "google_bigquery_table"},
		{"PATCH", "/bigquery/v2/projects/p/datasets/d/tables/t?alt=json", "google_bigquery_table"},
		{"POST", "/bigquery/v2/projects/p/datasets/d/tables/t/insertAll", ""},
		{"POST", "/bigquery/v2/projects/p/datasets/d/routines", ""},
		{"POST", "/bigquery/v2/projects/p/datasets/d/tables/t/rowAccessPolicies", ""},
	}
	for _, c := range cases {
		if got := ResourceForPath(c.method, c.path); got != c.want {
			t.Errorf("ResourceForPath(%s %s) = %q, want %q", c.method, c.path, got, c.want)
		}
	}
}

// TestSecretManagerAndWorkflowsPathRules pins the Secret Manager and Cloud
// Workflows attribution: the create/update bodies are the metadata writes, a
// version is the :addVersion custom method, the IAM binding is :setIamPolicy,
// and the read-only custom methods are not attributed.
func TestSecretManagerAndWorkflowsPathRules(t *testing.T) {
	cases := []struct {
		method, path, want string
	}{
		{"POST", "/v1/projects/p/secrets?alt=json&secretId=s", "google_secret_manager_secret"},
		{"PATCH", "/v1/projects/p/secrets/s?alt=json&updateMask=labels", "google_secret_manager_secret"},
		{"POST", "/v1/projects/p/secrets/s:addVersion?alt=json", "google_secret_manager_secret_version"},
		{"POST", "/v1/projects/p/secrets/s:setIamPolicy?alt=json", "google_secret_manager_secret_iam_binding"},
		{"POST", "/v1/projects/p/secrets/s:getIamPolicy?alt=json", ""},
		{"GET", "/v1/projects/p/secrets/s/versions/latest:access?alt=json", ""},
		{"POST", "/v1/projects/p/locations/us-central1/workflows?alt=json&workflowId=w", "google_workflows_workflow"},
		{"PATCH", "/v1/projects/p/locations/us-central1/workflows/w?alt=json&updateMask=sourceContents", "google_workflows_workflow"},
		{"POST", "/v1/projects/p/locations/us-central1/workflows/w/executions", ""},
		{"GET", "/v1/projects/p/locations/us-central1/workflows/w:listRevisions", ""},
	}
	for _, c := range cases {
		if got := ResourceForPath(c.method, c.path); got != c.want {
			t.Errorf("ResourceForPath(%s %s) = %q, want %q", c.method, c.path, got, c.want)
		}
	}
}

// TestSchedulerAndTasksPathRules pins the Cloud Scheduler and Cloud Tasks
// attribution: the collection POST and the PATCH are the resource writes, the
// queue IAM member is :setIamPolicy, and the custom methods / task creates are
// not attributed.
func TestSchedulerAndTasksPathRules(t *testing.T) {
	cases := []struct {
		method, path, want string
	}{
		{"POST", "/v1/projects/p/locations/us-central1/jobs?alt=json", "google_cloud_scheduler_job"},
		{"PATCH", "/v1/projects/p/locations/us-central1/jobs/j?alt=json&updateMask=schedule", "google_cloud_scheduler_job"},
		{"POST", "/v1/projects/p/locations/us-central1/jobs/j:pause?alt=json", ""},
		{"POST", "/v1/projects/p/locations/us-central1/jobs/j:resume?alt=json", ""},
		{"POST", "/v1/projects/p/locations/us-central1/jobs/j:run?alt=json", ""},
		{"POST", "/v2/projects/p/locations/us-central1/queues?alt=json", "google_cloud_tasks_queue"},
		{"PATCH", "/v2/projects/p/locations/us-central1/queues/q?alt=json&updateMask=rateLimits", "google_cloud_tasks_queue"},
		{"POST", "/v2/projects/p/locations/us-central1/queues/q:setIamPolicy?alt=json", "google_cloud_tasks_queue_iam_member"},
		{"POST", "/v2/projects/p/locations/us-central1/queues/q:getIamPolicy?alt=json", ""},
		{"POST", "/v2/projects/p/locations/us-central1/queues/q:purge?alt=json", ""},
		{"POST", "/v2/projects/p/locations/us-central1/queues/q/tasks?alt=json", ""},
	}
	for _, c := range cases {
		if got := ResourceForPath(c.method, c.path); got != c.want {
			t.Errorf("ResourceForPath(%s %s) = %q, want %q", c.method, c.path, got, c.want)
		}
	}
}

// TestCloudRunV2PathRules pins the Cloud Run resource attribution: the create
// (collection POST) and update (PATCH one service) carry the provider's write
// bodies, while the service IAM custom methods are POSTs under the same
// service name and must not be attributed to the resource.
func TestCloudRunV2PathRules(t *testing.T) {
	cases := []struct {
		method, path, want string
	}{
		{"POST", "/v2/projects/p/locations/us-central1/services?alt=json&serviceId=svc", "google_cloud_run_v2_service"},
		{"PATCH", "/v2/projects/p/locations/us-central1/services/svc?alt=json", "google_cloud_run_v2_service"},
		{"POST", "/v2/projects/p/locations/us-central1/services/svc:setIamPolicy?alt=json", ""},
		{"POST", "/v2/projects/p/locations/us-central1/services/svc:getIamPolicy?alt=json", ""},
		{"POST", "/v2/projects/p/locations/us-central1/jobs?jobId=job", ""},
	}
	for _, c := range cases {
		if got := ResourceForPath(c.method, c.path); got != c.want {
			t.Errorf("ResourceForPath(%s %s) = %q, want %q", c.method, c.path, got, c.want)
		}
	}
}

// TestCloudSQLPathRules pins the sqladmin v1beta4 attribution: the instance,
// database and user create/update bodies are the provider writes, while the
// instance-level custom methods (a POST under the instance name) and the
// read-only GETs are not attributed.
func TestCloudSQLPathRules(t *testing.T) {
	cases := []struct {
		method, path, want string
	}{
		{"POST", "/sql/v1beta4/projects/p/instances?alt=json", "google_sql_database_instance"},
		{"PUT", "/sql/v1beta4/projects/p/instances/i?alt=json", "google_sql_database_instance"},
		{"PATCH", "/sql/v1beta4/projects/p/instances/i?alt=json", "google_sql_database_instance"},
		{"POST", "/sql/v1beta4/projects/p/instances/i/restoreBackup?alt=json", ""},
		{"POST", "/sql/v1beta4/projects/p/instances/i/databases?alt=json", "google_sql_database"},
		{"PUT", "/sql/v1beta4/projects/p/instances/i/databases/d?alt=json", "google_sql_database"},
		{"POST", "/sql/v1beta4/projects/p/instances/i/users?alt=json", "google_sql_user"},
		{"PUT", "/sql/v1beta4/projects/p/instances/i/users?alt=json", "google_sql_user"},
		{"GET", "/sql/v1beta4/projects/p/instances/i?alt=json", ""},
	}
	for _, c := range cases {
		if got := ResourceForPath(c.method, c.path); got != c.want {
			t.Errorf("ResourceForPath(%s %s) = %q, want %q", c.method, c.path, got, c.want)
		}
	}
}
