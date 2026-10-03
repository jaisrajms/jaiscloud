// Package sdk_iceberg_test exercises the jaiscloud-gcp emulator's BigLake
// Iceberg REST Catalog (mounted at /iceberg/v1/...) directly over net/http.
// There is no Google SDK for Iceberg, so this drives the raw REST surface and
// validates wire-level parity with the Iceberg REST catalog spec (config,
// namespace lifecycle, table lifecycle, and commit round-trips).
//
// Run with the GCP binary running and GCP_EMULATOR_ENDPOINT set (defaults to
// http://localhost:8080/):
//
//	./jaiscloud-gcp start &
//	GCP_EMULATOR_ENDPOINT=http://localhost:8080/ go test ./...
package sdk_iceberg_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func base() string {
	if e := os.Getenv("GCP_EMULATOR_ENDPOINT"); e != "" {
		return strings.TrimRight(e, "/")
	}
	return "http://localhost:8080"
}

// icebergError is the Iceberg REST ErrorResponse shape.
type icebergError struct {
	Error struct {
		Message string `json:"message"`
		Type    string `json:"type"`
		Code    int    `json:"code"`
	} `json:"error"`
}

func do(t *testing.T, method, path string, body any) (int, map[string]any, *icebergError) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, base()+path, rd)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do %s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(data) == 0 {
		return resp.StatusCode, nil, nil
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("unmarshal %q: %v", data, err)
	}
	if resp.StatusCode >= 400 {
		var ie icebergError
		_ = json.Unmarshal(data, &ie)
		return resp.StatusCode, m, &ie
	}
	return resp.StatusCode, m, nil
}

func unique(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func TestSDKIceberg(t *testing.T) {
	// --- config ---
	code, m, e := do(t, http.MethodGet, "/iceberg/v1/config", nil)
	if code != 200 || e != nil {
		t.Fatalf("config: %d %+v", code, e)
	}
	if m["defaults"] == nil || m["overrides"] == nil || m["endpoints"] == nil {
		t.Fatalf("config shape: %v", m)
	}

	ns := unique("ns")

	// --- namespace lifecycle ---
	code, m, e = do(t, http.MethodPost, "/iceberg/v1/namespaces", map[string]any{
		"namespace":  []string{ns},
		"properties": map[string]string{"owner": "me"},
	})
	if code != 200 || e != nil {
		t.Fatalf("create namespace: %d %+v", code, e)
	}

	code, m, e = do(t, http.MethodGet, "/iceberg/v1/namespaces/"+ns, nil)
	if code != 200 || e != nil {
		t.Fatalf("get namespace: %d %+v", code, e)
	}
	if props := m["properties"].(map[string]any); props["owner"] != "me" {
		t.Fatalf("namespace properties: %v", props)
	}

	code, _, _ = do(t, http.MethodHead, "/iceberg/v1/namespaces/"+ns, nil)
	if code != 204 {
		t.Fatalf("namespace head: %d", code)
	}

	code, _, _ = do(t, http.MethodGet, "/iceberg/v1/namespaces", nil)
	if code != 200 {
		t.Fatalf("list namespaces: %d", code)
	}

	// update properties
	code, m, e = do(t, http.MethodPost, "/iceberg/v1/namespaces/"+ns+"/properties", map[string]any{
		"removals": []string{"owner"},
		"updates":  map[string]string{"env": "prod"},
	})
	if code != 200 || e != nil {
		t.Fatalf("update properties: %d %+v", code, e)
	}
	if updated, ok := m["updated"].([]any); !ok || len(updated) != 1 || updated[0] != "env" {
		t.Fatalf("update properties response: %v", m)
	}

	// --- table lifecycle ---
	tableName := unique("tbl")
	location := "s3://warehouse/" + ns + "/" + tableName
	createBody := map[string]any{
		"name":     tableName,
		"location": location,
		"schema": map[string]any{
			"type": "struct",
			"fields": []any{
				map[string]any{"id": 1, "name": "id", "type": "long", "required": true},
				map[string]any{"id": 2, "name": "data", "type": "string", "required": false},
			},
		},
	}
	code, m, e = do(t, http.MethodPost, "/iceberg/v1/namespaces/"+ns+"/tables", createBody)
	if code != 200 || e != nil {
		t.Fatalf("create table: %d %+v", code, e)
	}
	metaLocation, _ := m["metadata-location"].(string)
	if metaLocation == "" {
		t.Fatal("missing metadata-location")
	}
	metadata := m["metadata"].(map[string]any)
	tableUUID, _ := metadata["table-uuid"].(string)
	if tableUUID == "" {
		t.Fatal("missing table-uuid")
	}

	// load table
	code, m, e = do(t, http.MethodGet, "/iceberg/v1/namespaces/"+ns+"/tables/"+tableName, nil)
	if code != 200 || e != nil {
		t.Fatalf("load table: %d %+v", code, e)
	}
	if m["metadata-location"] != metaLocation {
		t.Fatalf("metadata-location mismatch on load")
	}

	// list tables
	code, m, e = do(t, http.MethodGet, "/iceberg/v1/namespaces/"+ns+"/tables", nil)
	if code != 200 || e != nil {
		t.Fatalf("list tables: %d %+v", code, e)
	}
	if ids := m["identifiers"].([]any); len(ids) != 1 {
		t.Fatalf("identifiers: %v", ids)
	}

	// --- commit round-trip ---
	code, m, e = do(t, http.MethodPost, "/iceberg/v1/namespaces/"+ns+"/tables/"+tableName, map[string]any{
		"requirements": []any{},
		"updates": []any{
			map[string]any{"action": "set-properties", "updates": map[string]string{"owner": "me"}},
			map[string]any{"action": "add-schema", "schema": map[string]any{
				"type": "struct", "schema-id": 1,
				"fields": []any{map[string]any{"id": 3, "name": "extra", "type": "string", "required": false}},
			}, "last-column-id": 3},
			map[string]any{"action": "set-current-schema", "schema-id": 1},
		},
	})
	if code != 200 || e != nil {
		t.Fatalf("commit: %d %+v", code, e)
	}
	committed := m["metadata"].(map[string]any)
	if committed["properties"].(map[string]any)["owner"] != "me" {
		t.Fatalf("committed properties: %v", committed["properties"])
	}
	if committed["current-schema-id"] != float64(1) {
		t.Fatalf("committed current-schema-id: %v", committed["current-schema-id"])
	}
	newMetaLocation, _ := m["metadata-location"].(string)
	if newMetaLocation == metaLocation {
		t.Fatalf("metadata-location should advance on commit")
	}

	// failing requirement -> 409 CommitFailedException
	code, _, e = do(t, http.MethodPost, "/iceberg/v1/namespaces/"+ns+"/tables/"+tableName, map[string]any{
		"requirements": []any{map[string]any{"type": "assert-table-uuid", "uuid": "wrong"}},
		"updates":      []any{map[string]any{"action": "set-properties", "updates": map[string]string{"owner": "hacker"}}},
	})
	if code != 409 || e == nil || e.Error.Type != "CommitFailedException" {
		t.Fatalf("expected 409 CommitFailedException, got %d %+v", code, e)
	}

	// --- rename ---
	code, _, e = do(t, http.MethodPost, "/iceberg/v1/tables/rename", map[string]any{
		"source":      map[string]any{"namespace": []string{ns}, "name": tableName},
		"destination": map[string]any{"namespace": []string{ns}, "name": tableName + "-renamed"},
	})
	if code != 204 || e != nil {
		t.Fatalf("rename: %d %+v", code, e)
	}
	code, _, _ = do(t, http.MethodGet, "/iceberg/v1/namespaces/"+ns+"/tables/"+tableName+"-renamed", nil)
	if code != 200 {
		t.Fatalf("renamed table should exist: %d", code)
	}

	// --- drop table + drop namespace ---
	code, _, e = do(t, http.MethodDelete, "/iceberg/v1/namespaces/"+ns+"/tables/"+tableName+"-renamed", nil)
	if code != 204 || e != nil {
		t.Fatalf("drop table: %d %+v", code, e)
	}
	code, _, e = do(t, http.MethodDelete, "/iceberg/v1/namespaces/"+ns, nil)
	if code != 204 || e != nil {
		t.Fatalf("drop namespace: %d %+v", code, e)
	}

}

// TestSDKIcebergMetricsReport exercises the reportMetrics endpoint: a valid
// report is acknowledged with 204, and an unknown table is 404 (the Iceberg REST
// spec's NoSuchTableException). The update to this expectation (previously a GET
// returning 501) matches the real REST catalog, which reports via POST.
func TestSDKIcebergMetricsReport(t *testing.T) {
	ns := unique("ns-metrics")
	code, _, e := do(t, http.MethodPost, "/iceberg/v1/namespaces", map[string]any{
		"namespace": []string{ns}, "properties": map[string]string{},
	})
	if code != 200 || e != nil {
		t.Fatalf("create namespace: %d %+v", code, e)
	}
	tableName := unique("tbl")
	code, _, e = do(t, http.MethodPost, "/iceberg/v1/namespaces/"+ns+"/tables", map[string]any{
		"name":     tableName,
		"location": "s3://warehouse/" + ns + "/" + tableName,
		"schema": map[string]any{
			"type":   "struct",
			"fields": []any{map[string]any{"id": 1, "name": "id", "type": "long", "required": true}},
		},
	})
	if code != 200 || e != nil {
		t.Fatalf("create table: %d %+v", code, e)
	}

	if code, _, e = do(t, http.MethodPost, "/iceberg/v1/namespaces/"+ns+"/tables/"+tableName+"/metrics", map[string]any{
		"report-type": "scan-report",
	}); code != 204 || e != nil {
		t.Fatalf("report metrics: %d %+v", code, e)
	}

	code, _, e = do(t, http.MethodPost, "/iceberg/v1/namespaces/"+ns+"/tables/missing-"+tableName+"/metrics", map[string]any{
		"report-type": "scan-report",
	})
	if code != 404 || e == nil || e.Error.Type != "NoSuchTableException" {
		t.Fatalf("metrics unknown table: %d %+v", code, e)
	}

	// Cleanup.
	do(t, http.MethodDelete, "/iceberg/v1/namespaces/"+ns+"/tables/"+tableName, nil)
	do(t, http.MethodDelete, "/iceberg/v1/namespaces/"+ns, nil)
}

// TestSDKIcebergSnapshots exercises the snapshot-commit lifecycle: add-snapshot,
// set-snapshot-ref (main), and remove-snapshots over the raw REST surface.
func TestSDKIcebergSnapshots(t *testing.T) {
	ns := unique("ns-snap")
	code, _, e := do(t, http.MethodPost, "/iceberg/v1/namespaces", map[string]any{
		"namespace":  []string{ns},
		"properties": map[string]string{},
	})
	if code != 200 || e != nil {
		t.Fatalf("create namespace: %d %+v", code, e)
	}

	tableName := unique("tbl")
	location := "s3://warehouse/" + ns + "/" + tableName
	code, _, e = do(t, http.MethodPost, "/iceberg/v1/namespaces/"+ns+"/tables", map[string]any{
		"name":     tableName,
		"location": location,
		"schema": map[string]any{
			"type":   "struct",
			"fields": []any{map[string]any{"id": 1, "name": "id", "type": "long", "required": true}},
		},
	})
	if code != 200 || e != nil {
		t.Fatalf("create table: %d %+v", code, e)
	}

	// add-snapshot 100 -> current-snapshot-id = 100.
	code, m, e := do(t, http.MethodPost, "/iceberg/v1/namespaces/"+ns+"/tables/"+tableName, map[string]any{
		"requirements": []any{},
		"updates": []any{
			map[string]any{"action": "add-snapshot", "snapshot": map[string]any{"snapshot-id": 100, "timestamp-ms": 123, "manifest-list": "s3://m/ml"}},
		},
	})
	if code != 200 || e != nil {
		t.Fatalf("add-snapshot: %d %+v", code, e)
	}
	if cur := m["metadata"].(map[string]any)["current-snapshot-id"]; cur != float64(100) {
		t.Fatalf("current-snapshot-id after add-snapshot = %v, want 100", cur)
	}

	// set-snapshot-ref main -> 100 (current-snapshot-id stays in lockstep).
	code, m, e = do(t, http.MethodPost, "/iceberg/v1/namespaces/"+ns+"/tables/"+tableName, map[string]any{
		"requirements": []any{},
		"updates": []any{
			map[string]any{"action": "set-snapshot-ref", "ref-name": "main", "snapshot-id": 100, "type": "branch"},
		},
	})
	if code != 200 || e != nil {
		t.Fatalf("set-snapshot-ref: %d %+v", code, e)
	}
	refs := m["metadata"].(map[string]any)["refs"].(map[string]any)
	if main, ok := refs["main"].(map[string]any); !ok || main["snapshot-id"] != float64(100) {
		t.Fatalf("refs.main = %v, want snapshot-id 100", refs["main"])
	}

	// remove-snapshots [100] -> current-snapshot-id = -1, snapshots empty.
	code, m, e = do(t, http.MethodPost, "/iceberg/v1/namespaces/"+ns+"/tables/"+tableName, map[string]any{
		"requirements": []any{},
		"updates": []any{
			map[string]any{"action": "remove-snapshots", "snapshot-ids": []any{100}},
		},
	})
	if code != 200 || e != nil {
		t.Fatalf("remove-snapshots: %d %+v", code, e)
	}
	meta := m["metadata"].(map[string]any)
	if cur := meta["current-snapshot-id"]; cur != float64(-1) {
		t.Fatalf("current-snapshot-id after remove-snapshots = %v, want -1", cur)
	}
	if snaps := meta["snapshots"].([]any); len(snaps) != 0 {
		t.Fatalf("snapshots = %v, want empty", snaps)
	}
}

// TestSDKIcebergPagination verifies pageSize/pageToken pagination over the
// namespaces listing (Iceberg's next-page-token convention).
func TestSDKIcebergPagination(t *testing.T) {
	prefix := unique("nspage")
	created := map[string]bool{}
	for i := 0; i < 5; i++ {
		name := fmt.Sprintf("%s-%d", prefix, i)
		code, _, e := do(t, http.MethodPost, "/iceberg/v1/namespaces", map[string]any{
			"namespace":  []string{name},
			"properties": map[string]string{},
		})
		if code != 200 || e != nil {
			t.Fatalf("create namespace %s: %d %+v", name, code, e)
		}
		created[name] = true
	}

	seen := map[string]bool{}
	token := ""
	pages := 0
	for {
		path := "/iceberg/v1/namespaces?pageSize=2"
		if token != "" {
			path += "&pageToken=" + token
		}
		code, m, e := do(t, http.MethodGet, path, nil)
		if code != 200 || e != nil {
			t.Fatalf("list page: %d %+v", code, e)
		}
		ids, _ := m["namespaces"].([]any)
		if len(ids) > 2 {
			t.Fatalf("page size %d > 2", len(ids))
		}
		for _, id := range ids {
			levels := id.([]any)
			parts := make([]string, 0, len(levels))
			for _, l := range levels {
				parts = append(parts, l.(string))
			}
			name := strings.Join(parts, "/")
			if seen[name] {
				t.Fatalf("namespace %s duplicated across pages", name)
			}
			seen[name] = true
		}
		tok, _ := m["next-page-token"].(string)
		if tok == "" {
			break
		}
		token = tok
		pages++
		if pages > 100 {
			t.Fatal("pagination did not terminate")
		}
	}
	for name := range created {
		if !seen[name] {
			t.Fatalf("namespace %s missing from paginated listing", name)
		}
	}
}
