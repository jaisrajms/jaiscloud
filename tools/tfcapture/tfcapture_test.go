package main

import (
	"os"
	"path/filepath"
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
	report := Check("tgm15-empty-label", requests, schema, mapping, catalog, true)
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
