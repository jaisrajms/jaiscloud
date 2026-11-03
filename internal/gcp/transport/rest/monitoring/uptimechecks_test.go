package monitoring

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"jaiscloud/internal/gcp/wire"
)

func TestRESTUptimeCheckRoundTrip(t *testing.T) {
	c, p := newDashboardProvider()
	base := "/v3/projects/test/uptimeCheckConfigs"

	created, err := call(t, c, p, http.MethodPost, base, map[string]any{
		"displayName": "checkout",
		"monitoredResource": map[string]any{
			"type":   "uptime_url",
			"labels": map[string]any{"host": "shop.example.com", "project_id": "test"},
		},
		"httpCheck":       map[string]any{"path": "/healthz", "useSsl": true, "requestMethod": "GET"},
		"timeout":         "10s",
		"period":          "300s",
		"selectedRegions": []any{"USA", "EUROPE", "ASIA_PACIFIC"},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	doc := dashboardData(t, created)
	name, _ := doc["name"].(string)
	if name == "" || doc["displayName"] != "checkout" {
		t.Fatalf("created = %+v", doc)
	}
	if doc["period"] != "300s" {
		t.Fatalf("period = %v, want the declared 300s", doc["period"])
	}

	got, err := call(t, c, p, http.MethodGet, "/v3/"+name, nil)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if gotDoc := dashboardData(t, got); gotDoc["name"] != name {
		t.Fatalf("get = %+v, want the created config", gotDoc)
	}

	list, err := call(t, c, p, http.MethodGet, base, nil)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	listData := wireData(t, list)
	configs, _ := listData["uptimeCheckConfigs"].([]any)
	if len(configs) != 1 {
		t.Fatalf("list = %+v, want the one config", listData)
	}

	// The Terraform update shape: PATCH with an updateMask query parameter and a
	// partial body.
	updated, err := call(t, c, p, http.MethodPatch,
		"/v3/"+name+"?updateMask=displayName,httpCheck.path",
		map[string]any{"displayName": "checkout-v2", "httpCheck": map[string]any{"path": "/ready"}})
	if err != nil {
		t.Fatalf("patch: %v", err)
	}
	upDoc := dashboardData(t, updated)
	httpCheck, _ := upDoc["httpCheck"].(map[string]any)
	if upDoc["displayName"] != "checkout-v2" || httpCheck["path"] != "/ready" {
		t.Fatalf("patched = %+v, want the masked displayName/path", upDoc)
	}
	if httpCheck["useSsl"] != true {
		t.Fatalf("httpCheck = %+v, want the unmasked useSsl preserved", httpCheck)
	}

	if _, err := call(t, c, p, http.MethodDelete, "/v3/"+name, nil); err != nil {
		t.Fatalf("delete: %v", err)
	}
	_, err = call(t, c, p, http.MethodGet, "/v3/"+name, nil)
	dashboardWireStatus(t, err, 404, "NotFound")
}

func TestRESTUptimeCheckErrorsAndRouting(t *testing.T) {
	c, p := newDashboardProvider()
	base := "/v3/projects/test/uptimeCheckConfigs"

	// Invalid bodies are 400 INVALID_ARGUMENT.
	for name, body := range map[string]map[string]any{
		"missing display name": {"monitoredResource": map[string]any{"type": "uptime_url"}, "timeout": "10s"},
		"missing resource":     {"displayName": "x", "timeout": "10s"},
		"bad period":           {"displayName": "x", "monitoredResource": map[string]any{"type": "uptime_url"}, "timeout": "10s", "period": "120s"},
		"unknown field":        {"displayName": "x", "monitoredResource": map[string]any{"type": "uptime_url"}, "timeout": "10s", "bogus": 1},
	} {
		_, err := call(t, c, p, http.MethodPost, base, body)
		if err == nil {
			t.Errorf("%s: create should fail", name)
			continue
		}
		dashboardWireStatus(t, err, 400, "InvalidArgument")
	}

	// An item name that is not the Discovery pattern is not a route.
	for _, path := range []string{
		"/v3/projects/test/uptimeCheckConfigs/a/b",
		"/v3/organizations/1/uptimeCheckConfigs",
	} {
		req, _ := http.NewRequest(http.MethodGet, path, nil)
		if _, err := c.Decode(req, nil); err == nil {
			t.Errorf("%s: should not route", path)
		} else {
			dashboardWireStatus(t, err, 404, "NotFound")
		}
	}

	// The probe-derived checker address list stays a loud 501.
	for _, path := range []string{"/v3/uptimeCheckIps", "/v3/projects/test/uptimeCheckIps"} {
		req, _ := http.NewRequest(http.MethodGet, path, nil)
		if _, err := c.Decode(req, nil); err == nil {
			t.Errorf("%s: should be unimplemented", path)
		} else {
			dashboardWireStatus(t, err, 501, "Unimplemented")
		}
	}

	// Each method routes to its own action.
	for _, tc := range []struct {
		method, path, wantAction string
	}{
		{http.MethodGet, base, "ListUptimeCheckConfigs"},
		{http.MethodPost, base, "CreateUptimeCheckConfig"},
		{http.MethodGet, base + "/id1", "GetUptimeCheckConfig"},
		{http.MethodPatch, base + "/id1", "UpdateUptimeCheckConfig"},
		{http.MethodDelete, base + "/id1", "DeleteUptimeCheckConfig"},
	} {
		req, _ := http.NewRequest(tc.method, tc.path, nil)
		nr, err := c.Decode(req, nil)
		if err != nil {
			t.Fatalf("%s %s: %v", tc.method, tc.path, err)
		}
		if nr.Action != tc.wantAction {
			t.Errorf("%s %s action = %q, want %q", tc.method, tc.path, nr.Action, tc.wantAction)
		}
		if nr.Params["project"] != "test" {
			t.Errorf("%s %s project = %v, want test", tc.method, tc.path, nr.Params["project"])
		}
	}
}

// TestRESTUptimeCheckRepeatedMaskParam pins the second documented updateMask
// encoding: repeated query parameters rather than one comma-separated value.
func TestRESTUptimeCheckRepeatedMaskParam(t *testing.T) {
	c, p := newDashboardProvider()
	base := "/v3/projects/test/uptimeCheckConfigs"
	created, err := call(t, c, p, http.MethodPost, base, map[string]any{
		"displayName":       "a",
		"monitoredResource": map[string]any{"type": "uptime_url"},
		"timeout":           "10s",
		"period":            "60s",
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	name, _ := dashboardData(t, created)["name"].(string)
	updated, err := call(t, c, p, http.MethodPatch,
		"/v3/"+name+"?updateMask=displayName&updateMask=period",
		map[string]any{"displayName": "b", "period": "900s"})
	if err != nil {
		t.Fatalf("patch: %v", err)
	}
	doc := dashboardData(t, updated)
	if doc["displayName"] != "b" || doc["period"] != "900s" {
		t.Fatalf("patched = %+v, want both repeated-mask fields applied", doc)
	}
}

// TestRESTUptimeCheckDocumentIsRawJSON pins that the response is emitted from the
// stored document rather than re-marshalled through the float64 body decode.
func TestRESTUptimeCheckDocumentIsRawJSON(t *testing.T) {
	c, p := newDashboardProvider()
	base := "/v3/projects/test/uptimeCheckConfigs"
	created, err := call(t, c, p, http.MethodPost, base, map[string]any{
		"displayName":       "ints",
		"monitoredResource": map[string]any{"type": "uptime_url"},
		"timeout":           "10s",
		"httpCheck":         map[string]any{"port": 9007199254740993},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	raw, ok := created.Data[wire.RawJSONKey].(json.RawMessage)
	if !ok {
		t.Fatalf("response is not a raw JSON document: %+v", created.Data)
	}
	if !json.Valid(raw) || !strings.Contains(string(raw), "9007199254740993") {
		t.Fatalf("document = %s, want the exact integer preserved", raw)
	}
}
