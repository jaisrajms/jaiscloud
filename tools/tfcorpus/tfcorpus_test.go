package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func testCoverage() map[string]CoverageRow {
	return map[string]CoverageRow{
		"google_storage_bucket":            {ProviderResource: "google_storage_bucket", Service: "storage", EmulatorState: "served", Classification: "served"},
		"google_storage_bucket_iam_member": {ProviderResource: "google_storage_bucket_iam_member", Service: "storage", EmulatorState: "served", Classification: "served"},
		"google_secret_manager_secret":     {ProviderResource: "google_secret_manager_secret", Service: "secretmanager", EmulatorState: "served", Classification: "served"},
	}
}

// docsTree writes a small provider-docs checkout: one served block, one
// unsupported block, one data-only snippet and one multi-resource block.
func docsTree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "website", "docs", "r", "storage_bucket.html.markdown"), `---
subcategory: "Cloud Storage"
---

# google_storage_bucket

## Example Usage

`+"```hcl"+`
resource "google_storage_bucket" "a" {
  name     = "a"
  project  = "not-the-test-project"
  location = "EU"
}
`+"```"+`

`+"```hcl"+`
resource "google_artifact_registry_repository" "b" {
  repository_id = "b"
}
`+"```"+`

`+"```hcl"+`
data "google_project" "p" {}
`+"```"+`

`+"```terraform"+`
resource "google_storage_bucket" "c" {
  name = "c"
}

resource "google_storage_bucket_iam_member" "c" {
  bucket = google_storage_bucket.c.name
  role   = "roles/storage.objectViewer"
  member = "user:a@example.com"
}
`+"```"+`
`)
	return root
}

func TestDiscoverDocs(t *testing.T) {
	root := docsTree(t)
	cases, stats, err := DiscoverDocs(root, testCoverage())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Docs != 1 || stats.ResourceBlocks != 3 || stats.InScope != 2 || stats.Excluded != 1 {
		t.Fatalf("stats = %+v", stats)
	}
	if len(cases) != 3 {
		t.Fatalf("cases = %d, want 3", len(cases))
	}
	if cases[0].Name != "storage_bucket-example-1" || cases[0].Status != StatusCandidate || cases[0].Service != "storage" {
		t.Errorf("first case = %+v", cases[0])
	}
	if cases[1].Status != StatusExcluded || !strings.Contains(cases[1].Reason, "google_artifact_registry_repository") {
		t.Errorf("second case = %+v", cases[1])
	}
	if len(cases[2].ResourceTypes) != 2 || cases[2].Status != StatusCandidate {
		t.Errorf("third case = %+v", cases[2])
	}

	body, err := DocBlock(root, cases[2].File, cases[2].Block)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(body, `google_storage_bucket_iam_member" "c"`) {
		t.Fatalf("block 3 re-extracted the wrong body:\n%s", body)
	}
}

func TestNormalizeExample(t *testing.T) {
	in := `provider "google" {
  project = "x"
}

terraform {
  required_providers {
    google = { source = "hashicorp/google" }
  }
}

resource "google_storage_bucket" "b" {
  name    = "b-%{random_suffix}"
  project = "x"
  billing_project = "y"

  lifecycle {
    prevent_destroy = true
  }

  ...
}
`
	got := normalizeExample(in)
	if strings.Contains(got, `provider "google"`) || strings.Contains(got, "required_providers") {
		t.Fatalf("provider/terraform block survived:\n%s", got)
	}
	if strings.Contains(got, "prevent_destroy") {
		t.Fatalf("prevent_destroy survived:\n%s", got)
	}
	if strings.Contains(got, "...") {
		t.Fatalf("ellipsis placeholder survived:\n%s", got)
	}
	if strings.Count(got, "var.project") != 2 {
		t.Fatalf("project attributes not normalized:\n%s", got)
	}
	if !strings.Contains(got, `name    = "b-jctest"`) {
		t.Fatalf("name was rewritten:\n%s", got)
	}
	if strings.Contains(got, "%{random_suffix}") {
		t.Fatalf("magic suffix survived:\n%s", got)
	}
}

func TestDiscoverSamples(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "storage", "basic", "main.tf"),
		`resource "google_storage_bucket" "b" { name = "b" }`)
	writeFile(t, filepath.Join(root, "composer", "env", "main.tf"),
		`resource "google_composer_environment" "e" {}`)
	cases, err := DiscoverSamples(root, testCoverage())
	if err != nil {
		t.Fatal(err)
	}
	if len(cases) != 2 {
		t.Fatalf("cases = %d, want 2", len(cases))
	}
	byName := map[string]ManifestCase{}
	for _, c := range cases {
		byName[c.Name] = c
	}
	if c := byName["storage/basic"]; c.Status != StatusExcluded || !strings.Contains(c.Reason, "deferred") {
		t.Errorf("storage/basic = %+v", c)
	}
	if c := byName["composer/env"]; c.Status != StatusExcluded || !strings.Contains(c.Reason, "unsupported-service") {
		t.Errorf("composer/env = %+v", c)
	}
}

func TestMergeCasesPreservesHandEdits(t *testing.T) {
	old := &Manifest{Cases: []ManifestCase{
		{
			Name: "storage_bucket-example-1", Mode: ModeApply, Status: StatusCandidate,
			Assert: []Assertion{{Phase: "before", URL: "http://x", Contains: []string{"a"}}},
		},
		{Name: "storage_bucket-example-9", Mode: ModePlan, Status: StatusExcluded, Reason: "manual: too heavy for this gate"},
	}}
	discovered := []ManifestCase{
		{Name: "storage_bucket-example-1", Mode: ModePlan, Status: StatusExcluded, Reason: "unsupported-service: google_x"},
		{Name: "storage_bucket-example-2", Mode: ModePlan, Status: StatusCandidate},
		{Name: "storage_bucket-example-9", Mode: ModePlan, Status: StatusCandidate},
	}
	got := MergeCases(old, discovered)
	if got[0].Mode != ModeApply || len(got[0].Assert) != 1 {
		t.Errorf("hand edits lost: %+v", got[0])
	}
	if got[0].Status != StatusExcluded || got[0].Reason != "unsupported-service: google_x" {
		t.Errorf("generated exclusion should be refreshed: %+v", got[0])
	}
	if got[1].Mode != ModePlan || got[1].Status != StatusCandidate {
		t.Errorf("new case not defaulted: %+v", got[1])
	}
	if got[2].Status != StatusExcluded || got[2].Reason != "manual: too heavy for this gate" {
		t.Errorf("manual exclusion lost: %+v", got[2])
	}
}

func TestClassifyText(t *testing.T) {
	cases := map[string]string{
		"Error 501: not implemented":                           "unimplemented",
		"Error 404: The requested URL /v1/x was not found":     "unrouted",
		"Error 400: invalid argument: bad field":               "validation",
		"Error 403: permission denied":                         "auth",
		"panic: runtime error: invalid memory address":         "provider-panic",
		"Missing required argument: name":                      "config",
		"dial tcp 127.0.0.1:8080: connect: connection refused": "transport",
		"something unexpected":                                 "other",
	}
	for in, want := range cases {
		if got := classifyText(in); got != want {
			t.Errorf("classifyText(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestClassifyValidateJSON(t *testing.T) {
	out := `{
  "format_version": "1.0",
  "valid": false,
  "error_count": 1,
  "diagnostics": [
    {"severity": "error", "summary": "Reference to undeclared resource", "detail": "A managed resource \"google_eventarc_message_bus\" \"primary\" has not been declared."}
  ]
}`
	class, summary, _ := classifyDiagnostics(out, "")
	if class != "config" {
		t.Fatalf("class = %q, summary %q", class, summary)
	}
	if !strings.Contains(summary, "Reference to undeclared resource") {
		t.Fatalf("summary = %q", summary)
	}
}

func TestEndpointsValues(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "endpoints.yaml")
	writeFile(t, path, `provider: hashicorp/google
credential_env: GOOGLE_OAUTH_ACCESS_TOKEN
default_endpoint: http://localhost:8080
services:
  storage:
    endpoints:
      - field: storage_custom_endpoint
        path: /storage/v1/
  container:
    endpoints:
      - field: container_custom_endpoint
        path: /container/
        host_prefix: container
  firestore:
    endpoints:
      - field: firestore_custom_endpoint
        path: /v1/
  firestoreadmin:
    endpoints:
      - field: firestore_custom_endpoint
        path: /v1/
`)
	ep, err := LoadEndpoints(path)
	if err != nil {
		t.Fatal(err)
	}
	values, err := ep.Values("http://localhost:8080")
	if err != nil {
		t.Fatal(err)
	}
	if values["storage_custom_endpoint"] != "http://localhost:8080/storage/v1/" {
		t.Errorf("storage = %q", values["storage_custom_endpoint"])
	}
	if values["container_custom_endpoint"] != "http://container.localhost:8080/container/" {
		t.Errorf("container = %q", values["container_custom_endpoint"])
	}
	if values["firestore_custom_endpoint"] != "http://localhost:8080/v1/" {
		t.Errorf("firestore = %q", values["firestore_custom_endpoint"])
	}
	snippet, err := ep.ProviderSnippet("http://localhost:8080", "p", "us-central1")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(snippet, `storage_custom_endpoint`) || !strings.Contains(snippet, "container.localhost") {
		t.Errorf("snippet:\n%s", snippet)
	}
}

// stubTF writes a fake terraform binary. The plan verb emits the given JSON
// diagnostics and exits planRC; every other verb exits 0.
func stubTF(t *testing.T, planJSON string, planRC int) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "tf")
	body := `#!/bin/sh
verb="$2"
if [ "$verb" = "plan" ]; then
  cat <<'EOF'
` + planJSON + `
EOF
  exit ` + itoa(planRC) + `
fi
exit 0
`
	writeFile(t, path, body)
	if err := os.Chmod(path, 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRunPlanClassification(t *testing.T) {
	root := docsTree(t)
	path := filepath.Join(t.TempDir(), "endpoints.yaml")
	writeFile(t, path, `provider: hashicorp/google
default_endpoint: http://localhost:8080
services:
  storage:
    endpoints:
      - field: storage_custom_endpoint
        path: /storage/v1/
`)
	ep, err := LoadEndpoints(path)
	if err != nil {
		t.Fatal(err)
	}
	c := ManifestCase{Name: "storage_bucket-example-1", Source: "provider-docs",
		File: "website/docs/r/storage_bucket.html.markdown", Block: 1,
		ResourceTypes: []string{"google_storage_bucket"}, Service: "storage", Mode: ModePlan}
	tf := stubTF(t, `{"@level":"error","type":"diagnostic","diagnostic":{"severity":"error","summary":"Error 501","detail":"not implemented"}}`, 1)
	r := NewRunner(tf, "http://localhost:8080", "p", "us-central1", "7.46.2", ep)
	r.ProviderDocs = root
	got := r.RunPlan(c)
	if got.Class != "unimplemented" {
		t.Fatalf("class = %q (summary %q detail %q)", got.Class, got.Summary, got.Detail)
	}

	ok := stubTF(t, `{"@level":"info","type":"version","terraform":"1.16.3"}`, 0)
	r.TF = ok
	if got := r.RunPlan(c); got.Class != "plan-ok" {
		t.Fatalf("class = %q, summary %q", got.Class, got.Summary)
	}

	panicTF := stubTF(t, `{"@level":"error","type":"diagnostic","diagnostic":{"severity":"error","summary":"Plugin did not respond","detail":"panic: runtime error: invalid memory address"}}`, 1)
	r.TF = panicTF
	if got := r.RunPlan(c); got.Class != "provider-panic" {
		t.Fatalf("class = %q", got.Class)
	}
}

func TestDiscoverDocsHelperProviders(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "website", "docs", "r", "mixed.html.markdown"), `# google_mixed

`+"```hcl"+`
resource "random_id" "rnd" {
  byte_length = 4
}

resource "google_storage_bucket" "b" {
  name = "b-${random_id.rnd.hex}"
}
`+"```"+`

`+"```hcl"+`
resource "kubernetes_secret" "s" {
  metadata {
    name = "s"
  }
}

resource "google_storage_bucket" "c" {
  name = "c"
}
`+"```"+`

`+"```hcl"+`
data "kubernetes_secret" "existing" {
  metadata {
    name = "s"
  }
}

resource "google_storage_bucket" "d" {
  name = "d"
}
`+"```"+`

`+"```hcl"+`
resource "google_storage_bucket" "e" {
  provider = google-beta
  name     = "e"
}
`+"```"+`
`)
	cases, _, err := DiscoverDocs(root, testCoverage())
	if err != nil {
		t.Fatal(err)
	}
	if len(cases) != 4 {
		t.Fatalf("cases = %d, want 4", len(cases))
	}
	if cases[0].Status != StatusCandidate {
		t.Errorf("random_id block should be a candidate: %+v", cases[0])
	}
	if cases[1].Status != StatusExcluded || !strings.Contains(cases[1].Reason, "kubernetes_secret") {
		t.Errorf("kubernetes block should be excluded as a helper provider: %+v", cases[1])
	}
	if cases[2].Status != StatusExcluded || !strings.Contains(cases[2].Reason, "kubernetes_secret") {
		t.Errorf("kubernetes data source should exclude the block: %+v", cases[2])
	}
	if cases[3].Status != StatusExcluded || !strings.Contains(cases[3].Reason, "beta-provider") {
		t.Errorf("google-beta block should be excluded: %+v", cases[3])
	}
}

func TestRunApplyContract(t *testing.T) {
	root := docsTree(t)
	path := filepath.Join(t.TempDir(), "endpoints.yaml")
	writeFile(t, path, `provider: hashicorp/google
default_endpoint: http://localhost:8080
services:
  storage:
    endpoints:
      - field: storage_custom_endpoint
        path: /storage/v1/
`)
	ep, err := LoadEndpoints(path)
	if err != nil {
		t.Fatal(err)
	}
	c := ManifestCase{Name: "storage_bucket-example-1", Source: "provider-docs",
		File: "website/docs/r/storage_bucket.html.markdown", Block: 1,
		ResourceTypes: []string{"google_storage_bucket"}, Service: "storage", Mode: ModeApply}
	r := NewRunner(stubTF(t, "", 0), "http://localhost:8080", "p", "us-central1", "7.46.2", ep)
	r.ProviderDocs = root
	if got := r.RunApply(c); got.Class != "apply-ok" {
		t.Fatalf("class = %q, summary %q detail %q", got.Class, got.Summary, got.Detail)
	}
}

func TestRunApplyAsserts(t *testing.T) {
	root := docsTree(t)
	path := filepath.Join(t.TempDir(), "endpoints.yaml")
	writeFile(t, path, `provider: hashicorp/google
default_endpoint: http://localhost:8080
services:
  storage:
    endpoints:
      - field: storage_custom_endpoint
        path: /storage/v1/
`)
	ep, err := LoadEndpoints(path)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/present":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"name":"present"}`))
		case "/gone":
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer srv.Close()

	c := ManifestCase{
		Name: "storage_bucket-example-1", Source: "provider-docs",
		File: "website/docs/r/storage_bucket.html.markdown", Block: 1,
		ResourceTypes: []string{"google_storage_bucket"}, Service: "storage", Mode: ModeApply,
		Assert: []Assertion{
			{Phase: "before", URL: "{{endpoint}}/present", Contains: []string{"present"}},
			{Phase: "after", URL: "{{endpoint}}/gone", Status: 404},
		},
	}
	r := NewRunner(stubTF(t, "", 0), srv.URL, "p", "us-central1", "7.46.2", ep)
	r.ProviderDocs = root
	if got := r.RunApply(c); got.Class != "apply-ok" {
		t.Fatalf("class = %q, summary %q detail %q", got.Class, got.Summary, got.Detail)
	}

	c.Assert = []Assertion{{Phase: "before", URL: "{{endpoint}}/missing", Contains: []string{"x"}}}
	if got := r.RunApply(c); got.Class != "assert-failed" {
		t.Fatalf("class = %q, summary %q", got.Class, got.Summary)
	}
}

func TestSelectModesAndZeroExampleServices(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "corpus.yaml")
	m := &Manifest{
		Provider: "hashicorp/google", ProviderVersion: "7.46.2",
		Cases: []ManifestCase{
			{Name: "a", Service: "storage", Mode: ModePlan, Status: StatusCandidate},
			{Name: "b", Service: "storage", Mode: ModeApply, Status: StatusCandidate},
			{Name: "x", Service: "composer", Mode: ModePlan, Status: StatusExcluded, Reason: "unsupported-service: google_composer_environment"},
		},
	}
	if err := SaveManifest(path, m); err != nil {
		t.Fatal(err)
	}
	if err := selectModes(path, "a", ""); err != nil {
		t.Fatal(err)
	}
	got, err := LoadManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Cases[0].Mode != ModeApply {
		t.Errorf("a not promoted: %+v", got.Cases[0])
	}
	coverage := map[string]CoverageRow{
		"google_storage_bucket":       {Service: "storage", Classification: "served"},
		"google_composer_environment": {Service: "composer", Classification: "served"},
	}
	zero := servicesWithoutCandidates(got, coverage)
	if len(zero) != 1 || zero[0] != "composer" {
		t.Errorf("zero = %v", zero)
	}
}

func TestReportGroupsAndDeterminism(t *testing.T) {
	m := &Manifest{
		Provider: "hashicorp/google", ProviderVersion: "7.46.2",
		Cases: []ManifestCase{
			{Name: "b", Service: "storage", Status: StatusCandidate, Mode: ModePlan},
			{Name: "a", Service: "storage", Status: StatusCandidate, Mode: ModePlan},
			{Name: "x", Service: "composer", Status: StatusExcluded, Mode: ModePlan, Reason: "unsupported-service: google_composer_environment"},
			{Name: "y", Service: "storage", Status: StatusExcluded, Mode: ModePlan, Reason: "sample scenario; plan-run deferred (TF2-x)"},
		},
	}
	plan := []CaseResult{
		{Name: "a", Service: "storage", Class: "plan-ok"},
		{Name: "b", Service: "storage", Class: "unrouted", Summary: "Error 404: nope", ResourceTypes: []string{"google_storage_bucket"}},
	}
	rep := BuildReport(m, "Terraform v1.16.3", "http://localhost:8080", "p", plan, nil, []string{"firestore"})
	if rep.Candidate != 2 || rep.Excluded != 2 {
		t.Errorf("counts: candidates=%d excluded=%d", rep.Candidate, rep.Excluded)
	}
	if len(rep.Groups) != 1 || rep.Groups[0].Class != "unrouted" || rep.Groups[0].Count != 1 {
		t.Errorf("groups = %+v", rep.Groups)
	}
	if len(rep.Services) != 1 || rep.Services[0].Planned != 2 || rep.Services[0].PlanOK != 1 {
		t.Errorf("services = %+v", rep.Services)
	}
	if len(rep.ExcludedReasons) != 2 {
		t.Errorf("excluded reasons = %+v", rep.ExcludedReasons)
	}
}
