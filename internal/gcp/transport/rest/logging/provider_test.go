package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"

	core "jaiscloud/internal/gcp/service/logging"
	loggingstore "jaiscloud/internal/gcp/store/logging"
	"jaiscloud/internal/model"
)

// newTestProvider returns a codec and provider sharing one memory-backed core.
func newTestProvider(t *testing.T) (*Codec, *Provider) {
	t.Helper()
	return NewCodec(), NewProvider(core.NewService(loggingstore.NewMemoryStore(), "test"), "test")
}

// call decodes a synthetic REST request through the codec and dispatches it to
// the provider, mirroring the gateway's request flow.
func call(t *testing.T, c *Codec, p *Provider, method, path string, body any) (*model.ProviderResponse, error) {
	t.Helper()
	var raw []byte
	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
	}
	req, err := http.NewRequest(method, path, bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	nr, err := c.Decode(req, raw)
	if err != nil {
		return nil, err
	}
	return p.Routes()["Logging."+nr.Action](context.Background(), nr)
}

// wireData round-trips a provider response through JSON so the test observes the
// same shapes the HTTP client would (map[string]any/scalars), not Go's typed
// values.
func wireData(t *testing.T, resp *model.ProviderResponse) map[string]any {
	t.Helper()
	raw, err := json.Marshal(resp.Data)
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	return m
}

func TestRESTWriteListDeleteRoundTrip(t *testing.T) {
	c, p := newTestProvider(t)

	writeBody := map[string]any{
		"logName": "projects/test/logs/rest-log",
		"resource": map[string]any{
			"type":   "gce_instance",
			"labels": map[string]any{"instance_id": "123", "zone": "us-central1-a"},
		},
		"labels": map[string]any{"env": "test"},
		"entries": []any{
			map[string]any{"severity": "INFO", "textPayload": "hello"},
			map[string]any{"severity": "ERROR", "jsonPayload": map[string]any{"msg": "boom"}},
		},
	}
	if _, err := call(t, c, p, http.MethodPost, "/v2/entries:write", writeBody); err != nil {
		t.Fatalf("entries:write: %v", err)
	}

	list, err := call(t, c, p, http.MethodPost, "/v2/entries:list", map[string]any{
		"resourceNames": []any{"projects/test"},
	})
	if err != nil {
		t.Fatalf("entries:list: %v", err)
	}
	entries, _ := wireData(t, list)["entries"].([]any)
	if len(entries) != 2 {
		t.Fatalf("entries = %d, want 2: %+v", len(entries), wireData(t, list))
	}
	first := entries[0].(map[string]any)
	if first["logName"] != "projects/test/logs/rest-log" {
		t.Fatalf("logName = %v", first["logName"])
	}
	if first["severity"] != "INFO" {
		t.Fatalf("severity = %v, want INFO", first["severity"])
	}
	res := first["resource"].(map[string]any)
	if res["type"] != "gce_instance" {
		t.Fatalf("resource type = %v", res["type"])
	}
	if first["labels"].(map[string]any)["env"] != "test" {
		t.Fatalf("labels = %v", first["labels"])
	}

	logs, err := call(t, c, p, http.MethodGet, "/v2/projects/test/logs", nil)
	if err != nil {
		t.Fatalf("logs.list: %v", err)
	}
	names, _ := wireData(t, logs)["logNames"].([]any)
	if len(names) != 1 || names[0] != "projects/test/logs/rest-log" {
		t.Fatalf("logNames = %v", wireData(t, logs)["logNames"])
	}

	if _, err := call(t, c, p, http.MethodDelete, "/v2/projects/test/logs/rest-log", nil); err != nil {
		t.Fatalf("logs.delete: %v", err)
	}
	after, _ := call(t, c, p, http.MethodPost, "/v2/entries:list", map[string]any{
		"resourceNames": []any{"projects/test"},
	})
	if wireData(t, after)["entries"] != nil {
		t.Fatalf("entries after delete = %+v, want none", wireData(t, after))
	}
}

func TestRESTEntrySeverityFilter(t *testing.T) {
	c, p := newTestProvider(t)
	if _, err := call(t, c, p, http.MethodPost, "/v2/entries:write", map[string]any{
		"logName": "projects/test/logs/filter-log",
		"entries": []any{
			map[string]any{"severity": "INFO", "textPayload": "info"},
			map[string]any{"severity": "ERROR", "textPayload": "error"},
		},
	}); err != nil {
		t.Fatalf("entries:write: %v", err)
	}
	list, err := call(t, c, p, http.MethodPost, "/v2/entries:list", map[string]any{
		"resourceNames": []any{"projects/test"},
		"filter":        `logName="projects/test/logs/filter-log" AND severity>=WARNING`,
	})
	if err != nil {
		t.Fatalf("entries:list: %v", err)
	}
	entries, _ := wireData(t, list)["entries"].([]any)
	if len(entries) != 1 || entries[0].(map[string]any)["textPayload"] != "error" {
		t.Fatalf("filtered entries = %+v, want one error", wireData(t, list))
	}
}

func TestRESTEncodedLogIDRoundTrip(t *testing.T) {
	c, p := newTestProvider(t)
	const escaped = "projects/test/logs/cloudaudit.googleapis.com%2Factivity"

	if _, err := call(t, c, p, http.MethodPost, "/v2/entries:write", map[string]any{
		"entries": []any{map[string]any{"logName": escaped, "textPayload": "audit"}},
	}); err != nil {
		t.Fatalf("entries:write: %v", err)
	}

	logs, err := call(t, c, p, http.MethodGet, "/v2/projects/test/logs", nil)
	if err != nil {
		t.Fatalf("logs.list: %v", err)
	}
	names, _ := wireData(t, logs)["logNames"].([]any)
	if len(names) != 1 || names[0] != escaped {
		t.Fatalf("logNames = %v, want [%s]", names, escaped)
	}

	// DELETE with the encoded log ID must resolve to the same canonical log.
	if _, err := call(t, c, p, http.MethodDelete, "/v2/"+escaped, nil); err != nil {
		t.Fatalf("logs.delete: %v", err)
	}
	after, _ := call(t, c, p, http.MethodPost, "/v2/entries:list", map[string]any{
		"resourceNames": []any{"projects/test"},
	})
	if wireData(t, after)["entries"] != nil {
		t.Fatalf("entries after encoded delete = %+v", wireData(t, after))
	}
}

func TestRESTMonitoredResourceDescriptors(t *testing.T) {
	c, p := newTestProvider(t)

	resp, err := call(t, c, p, http.MethodGet, "/v2/monitoredResourceDescriptors?pageSize=3", nil)
	if err != nil {
		t.Fatalf("monitoredResourceDescriptors.list: %v", err)
	}
	data := wireData(t, resp)
	descs, _ := data["resourceDescriptors"].([]any)
	if len(descs) != 3 {
		t.Fatalf("page size = %d, want 3", len(descs))
	}
	if data["nextPageToken"] == nil {
		t.Fatalf("expected a nextPageToken when page is full")
	}
	d := descs[0].(map[string]any)
	if d["type"] == "" || d["displayName"] == "" {
		t.Fatalf("descriptor missing type/displayName: %+v", d)
	}
	if _, ok := d["labels"].([]any); !ok {
		t.Fatalf("descriptor missing labels: %+v", d)
	}
}

func TestRESTDefaultsFromProjectAndQuery(t *testing.T) {
	c, p := newTestProvider(t)

	// Write with no logName on the entry but a request-level logName.
	if _, err := call(t, c, p, http.MethodPost, "/v2/entries:write", map[string]any{
		"logName": "projects/test/logs/defaulted",
		"entries": []any{map[string]any{"severity": "NOTICE", "textPayload": "x"}},
	}); err != nil {
		t.Fatalf("entries:write: %v", err)
	}

	// entries:list with no resourceNames falls back to the request project.
	list, err := call(t, c, p, http.MethodPost, "/v2/entries:list", map[string]any{})
	if err != nil {
		t.Fatalf("entries:list: %v", err)
	}
	if entries, _ := wireData(t, list)["entries"].([]any); len(entries) != 1 {
		t.Fatalf("default-scope entries = %+v, want 1", wireData(t, list))
	}
}

func TestRESTErrors(t *testing.T) {
	c, p := newTestProvider(t)

	// Invalid filter -> InvalidArgument.
	if _, err := call(t, c, p, http.MethodPost, "/v2/entries:list", map[string]any{
		"resourceNames": []any{"projects/test"},
		"filter":        `logName=`,
	}); err == nil {
		t.Fatal("invalid filter should error")
	} else if perr, ok := err.(*model.ProviderError); !ok || perr.HTTPStatus != 400 {
		t.Fatalf("invalid filter err = %v, want 400 ProviderError", err)
	}

	// A well-formed absent log delete is idempotent.
	if _, err := call(t, c, p, http.MethodDelete, "/v2/projects/test/logs/missing", nil); err != nil {
		t.Fatalf("well-formed delete should be idempotent: %v", err)
	}
	// A malformed log name is rejected.
	if _, err := call(t, c, p, http.MethodPost, "/v2/entries:write", map[string]any{
		"entries": []any{map[string]any{"logName": "not-a-resource-name", "textPayload": "x"}},
	}); err == nil {
		t.Fatal("malformed log name should error")
	}
}

func TestCodecRejectsUnknownPaths(t *testing.T) {
	c := NewCodec()
	for _, tc := range []struct {
		method, path string
	}{
		{http.MethodPost, "/v2/entries:copy"},
		{http.MethodGet, "/v2/projects/test/bogus"},
		{http.MethodPost, "/v1/projects/test/logs"},
	} {
		req, _ := http.NewRequest(tc.method, tc.path, nil)
		if _, err := c.Decode(req, nil); err == nil {
			t.Errorf("%s %s decode should fail", tc.method, tc.path)
		}
	}
}

// TestCodecUnimplementedVsNotFound locks the status pairing: a real but
// unimplemented method is 501 UNIMPLEMENTED, an unknown path is 404 NOT_FOUND.
func TestCodecUnimplementedVsNotFound(t *testing.T) {
	c := NewCodec()

	req, _ := http.NewRequest(http.MethodPost, "/v2/entries:tail", nil)
	_, err := c.Decode(req, nil)
	perr, ok := err.(*model.ProviderError)
	if !ok || perr.Code != "Unimplemented" || perr.HTTPStatus != 501 {
		t.Fatalf("entries:tail err = %v, want 501 Unimplemented", err)
	}

	req, _ = http.NewRequest(http.MethodGet, "/v2/projects/test/bogus", nil)
	_, err = c.Decode(req, nil)
	perr, ok = err.(*model.ProviderError)
	if !ok || perr.Code != "NotFound" || perr.HTTPStatus != 404 {
		t.Fatalf("unknown path err = %v, want 404 NotFound", err)
	}
}

// TestRESTProjectIDsMerge locks that legacy projectIds are added to
// resourceNames (the proto's stated behavior), not ignored when both are set.
func TestRESTProjectIDsMerge(t *testing.T) {
	c, p := newTestProvider(t)

	if _, err := call(t, c, p, http.MethodPost, "/v2/entries:write", map[string]any{
		"logName": "projects/test/logs/pid",
		"entries": []any{map[string]any{"severity": "INFO", "textPayload": "x"}},
	}); err != nil {
		t.Fatalf("entries:write: %v", err)
	}

	// resourceNames names an unrelated project, but projectIds still adds the
	// owning project; the merged scope set must return the entry.
	list, err := call(t, c, p, http.MethodPost, "/v2/entries:list", map[string]any{
		"resourceNames": []any{"projects/other"},
		"projectIds":    []any{"test"},
	})
	if err != nil {
		t.Fatalf("entries:list: %v", err)
	}
	entries, _ := wireData(t, list)["entries"].([]any)
	if len(entries) != 1 {
		t.Fatalf("merged projectIds entries = %+v, want 1", wireData(t, list))
	}
}
