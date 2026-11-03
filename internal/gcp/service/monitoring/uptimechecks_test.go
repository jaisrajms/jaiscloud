package monitoring

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"jaiscloud/internal/store"
)

func newUptimeCheckService() *Service {
	return NewService(nil, "test", WithResources(store.NewMemoryResourceStore()))
}

// uptimeCheckBody is the shape a Terraform/google_monitoring_uptime_check_config
// apply sends: an HTTPS check on an uptime_url monitored resource.
const uptimeCheckBody = `{
	"displayName": "checkout",
	"monitoredResource": {"type": "uptime_url", "labels": {"host": "shop.example.com", "project_id": "test"}},
	"httpCheck": {"path": "/healthz", "useSsl": true, "port": 443, "requestMethod": "GET"},
	"timeout": "10s",
	"period": "300s",
	"selectedRegions": ["USA", "EUROPE", "ASIA_PACIFIC"],
	"userLabels": {"team": "checkout"}
}`

func decodeDoc(t *testing.T, doc json.RawMessage) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(doc, &m); err != nil {
		t.Fatalf("decode document: %v", err)
	}
	return m
}

func TestUptimeCheckCRUD(t *testing.T) {
	ctx := context.Background()
	s := newUptimeCheckService()

	created, err := s.CreateUptimeCheckConfig(ctx, "proj", json.RawMessage(uptimeCheckBody))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.Name != "projects/proj/uptimeCheckConfigs/"+created.ID {
		t.Fatalf("name = %q, want the path name for id %q", created.Name, created.ID)
	}
	if created.DisplayName != "checkout" {
		t.Fatalf("displayName = %q, want checkout", created.DisplayName)
	}

	// The whole declaration is echoed verbatim.
	doc := decodeDoc(t, created.Document)
	if doc["name"] != created.Name {
		t.Errorf("document name = %v, want %q", doc["name"], created.Name)
	}
	if doc["period"] != "300s" || doc["timeout"] != "10s" {
		t.Errorf("period/timeout = %v/%v, want 300s/10s", doc["period"], doc["timeout"])
	}
	httpCheck, _ := doc["httpCheck"].(map[string]any)
	if httpCheck["path"] != "/healthz" || httpCheck["useSsl"] != true {
		t.Errorf("httpCheck = %v, want the declared path/useSsl", httpCheck)
	}
	labels, _ := doc["userLabels"].(map[string]any)
	if labels["team"] != "checkout" {
		t.Errorf("userLabels = %v, want team=checkout", labels)
	}

	got, err := s.GetUptimeCheckConfig(ctx, "proj", created.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got.Document) != string(created.Document) {
		t.Fatalf("get document = %s, want %s", got.Document, created.Document)
	}

	page, next, err := s.ListUptimeCheckConfigs(ctx, "proj", "", 0, "")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(page) != 1 || next != "" || page[0].ID != created.ID {
		t.Fatalf("list = %d items (next %q), want the one created config", len(page), next)
	}
	// A different project does not see it.
	if other, _, err := s.ListUptimeCheckConfigs(ctx, "other", "", 0, ""); err != nil || len(other) != 0 {
		t.Fatalf("list other project = %d items (err %v), want none", len(other), err)
	}

	if err := s.DeleteUptimeCheckConfig(ctx, "proj", created.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := s.GetUptimeCheckConfig(ctx, "proj", created.ID); err == nil {
		t.Fatal("get after delete should be NotFound")
	} else {
		dashboardErrStatus(t, err, 404, "NotFound")
	}
}

func TestUptimeCheckCreateHonorsPathName(t *testing.T) {
	ctx := context.Background()
	s := newUptimeCheckService()

	body := `{"name":"projects/proj/uptimeCheckConfigs/chosen","displayName":"d","monitoredResource":{"type":"uptime_url"},"timeout":"5s"}`
	created, err := s.CreateUptimeCheckConfig(ctx, "proj", json.RawMessage(body))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.ID != "chosen" || created.Name != "projects/proj/uptimeCheckConfigs/chosen" {
		t.Fatalf("created = %q/%q, want the caller-chosen id", created.ID, created.Name)
	}
	// A second create with the same id is AlreadyExists.
	if _, err := s.CreateUptimeCheckConfig(ctx, "proj", json.RawMessage(body)); err == nil {
		t.Fatal("duplicate create should fail")
	} else {
		dashboardErrStatus(t, err, 409, "AlreadyExists")
	}
	// A name in another project is rejected.
	bad := `{"name":"projects/other/uptimeCheckConfigs/x","displayName":"d","monitoredResource":{"type":"uptime_url"},"timeout":"5s"}`
	if _, err := s.CreateUptimeCheckConfig(ctx, "proj", json.RawMessage(bad)); err == nil {
		t.Fatal("cross-project name should fail")
	} else {
		dashboardErrStatus(t, err, 400, "InvalidArgument")
	}
}

func TestUptimeCheckValidation(t *testing.T) {
	ctx := context.Background()
	s := newUptimeCheckService()

	base := func(mutate func(m map[string]any)) string {
		m := map[string]any{"displayName": "d", "monitoredResource": map[string]any{"type": "uptime_url"}, "timeout": "10s"}
		mutate(m)
		data, err := json.Marshal(m)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		return string(data)
	}

	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{"missing display name", base(func(m map[string]any) { delete(m, "displayName") }), "display_name is required"},
		{"empty display name", base(func(m map[string]any) { m["displayName"] = "" }), "display_name is required"},
		{"missing resource", base(func(m map[string]any) { delete(m, "monitoredResource") }), "exactly one of"},
		{"two resources", base(func(m map[string]any) {
			m["resourceGroup"] = map[string]any{"groupId": "g"}
		}), "exactly one of"},
		{"missing timeout", base(func(m map[string]any) { delete(m, "timeout") }), "timeout is required"},
		{"timeout out of range", base(func(m map[string]any) { m["timeout"] = "90s" }), "between 1 and 60 seconds"},
		{"bad period", base(func(m map[string]any) { m["period"] = "120s" }), "period must be one of"},
		{"unknown field", base(func(m map[string]any) { m["nonsense"] = true }), "unknown field"},
		{"post without content type", base(func(m map[string]any) {
			m["httpCheck"] = map[string]any{"requestMethod": "POST"}
		}), "content_type is required"},
		{"content type on GET", base(func(m map[string]any) {
			m["httpCheck"] = map[string]any{"contentType": "URL_ENCODED"}
		}), "requires request_method POST"},
	} {
		if _, err := s.CreateUptimeCheckConfig(ctx, "proj", json.RawMessage(tc.body)); err == nil {
			t.Errorf("%s: create should fail", tc.name)
		} else {
			perr, ok := err.(interface{ Error() string })
			if !ok || !strings.Contains(perr.Error(), tc.want) {
				t.Errorf("%s: err = %v, want it to mention %q", tc.name, err, tc.want)
			}
			dashboardErrStatus(t, err, 400, "InvalidArgument")
		}
	}

	// A synthetic monitor has no check_request_type; that shape is valid.
	if _, err := s.CreateUptimeCheckConfig(ctx, "proj", json.RawMessage(base(func(m map[string]any) {
		delete(m, "monitoredResource")
		m["syntheticMonitor"] = map[string]any{"cloudFunctionV2": map[string]any{"name": "projects/p/locations/l/functions/f"}}
	}))); err != nil {
		t.Fatalf("synthetic monitor create: %v", err)
	}
}

func TestUptimeCheckUpdateMask(t *testing.T) {
	ctx := context.Background()
	s := newUptimeCheckService()

	created, err := s.CreateUptimeCheckConfig(ctx, "proj", json.RawMessage(uptimeCheckBody))
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// A nested mask path touches only the named subfield: the rest of httpCheck
	// survives.
	partial := `{"displayName":"checkout-renamed","httpCheck":{"path":"/ready"}}`
	updated, err := s.UpdateUptimeCheckConfig(ctx, "proj", created.ID, json.RawMessage(partial), []string{"displayName", "httpCheck.path"})
	if err != nil {
		t.Fatalf("masked update: %v", err)
	}
	doc := decodeDoc(t, updated.Document)
	if doc["displayName"] != "checkout-renamed" {
		t.Errorf("displayName = %v, want checkout-renamed", doc["displayName"])
	}
	httpCheck, _ := doc["httpCheck"].(map[string]any)
	if httpCheck["path"] != "/ready" {
		t.Errorf("httpCheck.path = %v, want /ready", httpCheck["path"])
	}
	if httpCheck["useSsl"] != true || httpCheck["port"] != float64(443) {
		t.Errorf("httpCheck = %v, want the unmasked useSsl/port preserved", httpCheck)
	}
	if doc["period"] != "300s" {
		t.Errorf("period = %v, want the unmasked period preserved", doc["period"])
	}

	// The snake_case proto spelling of the same path resolves identically.
	updated, err = s.UpdateUptimeCheckConfig(ctx, "proj", created.ID,
		json.RawMessage(`{"displayName":"snake","httpCheck":{"path":"/live"}}`), []string{"display_name", "http_check.path"})
	if err != nil {
		t.Fatalf("snake_case mask update: %v", err)
	}
	doc = decodeDoc(t, updated.Document)
	if doc["displayName"] != "snake" || decodeDoc(t, updated.Document)["httpCheck"].(map[string]any)["path"] != "/live" {
		t.Fatalf("snake_case mask update = %s, want displayName snake and path /live", updated.Document)
	}

	// A masked field omitted from the request is cleared.
	updated, err = s.UpdateUptimeCheckConfig(ctx, "proj", created.ID, json.RawMessage(`{"displayName":"cleared"}`), []string{"displayName", "userLabels"})
	if err != nil {
		t.Fatalf("clear update: %v", err)
	}
	if _, ok := decodeDoc(t, updated.Document)["userLabels"]; ok {
		t.Fatalf("userLabels survived a masked clear: %s", updated.Document)
	}

	// An empty mask replaces the whole configuration.
	replacement := `{"displayName":"replaced","monitoredResource":{"type":"uptime_url"},"timeout":"30s"}`
	updated, err = s.UpdateUptimeCheckConfig(ctx, "proj", created.ID, json.RawMessage(replacement), nil)
	if err != nil {
		t.Fatalf("full replace: %v", err)
	}
	doc = decodeDoc(t, updated.Document)
	if doc["displayName"] != "replaced" || doc["timeout"] != "30s" {
		t.Fatalf("replaced document = %s, want the new declaration", updated.Document)
	}
	if _, ok := doc["httpCheck"]; ok {
		t.Fatalf("httpCheck survived a full replace: %s", updated.Document)
	}

	// An unknown mask path is a client error, and a path that descends into a
	// repeated field names no field.
	for _, path := range []string{"nonsense", "httpCheck.nonsense", "contentMatchers.content"} {
		if _, err := s.UpdateUptimeCheckConfig(ctx, "proj", created.ID, json.RawMessage(replacement), []string{path}); err == nil {
			t.Errorf("mask path %q should fail", path)
		} else {
			dashboardErrStatus(t, err, 400, "InvalidArgument")
		}
	}

	// A missing config is NotFound.
	if _, err := s.UpdateUptimeCheckConfig(ctx, "proj", "missing", json.RawMessage(replacement), nil); err == nil {
		t.Fatal("update of a missing config should fail")
	} else {
		dashboardErrStatus(t, err, 404, "NotFound")
	}
}

func TestUptimeCheckUpdateNameMismatch(t *testing.T) {
	ctx := context.Background()
	s := newUptimeCheckService()
	created, err := s.CreateUptimeCheckConfig(ctx, "proj", json.RawMessage(uptimeCheckBody))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	body := `{"name":"projects/proj/uptimeCheckConfigs/other","displayName":"d","monitoredResource":{"type":"uptime_url"},"timeout":"10s"}`
	if _, err := s.UpdateUptimeCheckConfig(ctx, "proj", created.ID, json.RawMessage(body), nil); err == nil {
		t.Fatal("name mismatch should fail")
	} else {
		dashboardErrStatus(t, err, 400, "InvalidArgument")
	}
}

func TestUptimeCheckListFilterAndPaging(t *testing.T) {
	ctx := context.Background()
	s := newUptimeCheckService()
	for _, body := range []string{
		`{"displayName":"alpha","monitoredResource":{"type":"uptime_url"},"timeout":"10s","userLabels":{"env":"prod"}}`,
		`{"displayName":"beta","monitoredResource":{"type":"uptime_url"},"timeout":"10s","userLabels":{"env":"dev"}}`,
	} {
		if _, err := s.CreateUptimeCheckConfig(ctx, "proj", json.RawMessage(body)); err != nil {
			t.Fatalf("create: %v", err)
		}
	}
	page, _, err := s.ListUptimeCheckConfigs(ctx, "proj", `display_name = "beta"`, 0, "")
	if err != nil {
		t.Fatalf("filtered list: %v", err)
	}
	if len(page) != 1 || page[0].DisplayName != "beta" {
		t.Fatalf("display_name filter = %d items, want beta", len(page))
	}
	page, _, err = s.ListUptimeCheckConfigs(ctx, "proj", `user_labels.env = "prod"`, 0, "")
	if err != nil {
		t.Fatalf("label list: %v", err)
	}
	if len(page) != 1 || page[0].DisplayName != "alpha" {
		t.Fatalf("user_labels filter = %d items, want alpha", len(page))
	}
	// An unsupported filter key fails closed.
	if _, _, err := s.ListUptimeCheckConfigs(ctx, "proj", `resource.type = "uptime_url"`, 0, ""); err == nil {
		t.Fatal("unsupported filter key should fail")
	} else {
		dashboardErrStatus(t, err, 400, "InvalidArgument")
	}
	// Paging: one item per page, ids ordered.
	page, next, err := s.ListUptimeCheckConfigs(ctx, "proj", "", 1, "")
	if err != nil {
		t.Fatalf("paged list: %v", err)
	}
	if len(page) != 1 || next == "" {
		t.Fatalf("page = %d items (next %q), want one item and a token", len(page), next)
	}
	page2, next2, err := s.ListUptimeCheckConfigs(ctx, "proj", "", 1, next)
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if len(page2) != 1 || next2 != "" || page2[0].ID == page[0].ID {
		t.Fatalf("second page = %v (next %q), want the other config", page2, next2)
	}
}

func TestUptimeCheckRequiresResourceStore(t *testing.T) {
	s := NewService(nil, "test")
	if _, _, err := s.ListUptimeCheckConfigs(context.Background(), "proj", "", 0, ""); err == nil {
		t.Fatal("list without the shared resource store should fail loud")
	} else {
		dashboardErrStatus(t, err, 501, "Unimplemented")
	}
}

func TestParseMaskPaths(t *testing.T) {
	for _, tc := range []struct {
		in   any
		want []string
	}{
		{"displayName,httpCheck.path", []string{"displayName", "httpCheck.path"}},
		{[]any{"displayName", "timeout,period"}, []string{"displayName", "timeout", "period"}},
		{[]string{" display_name "}, []string{"display_name"}},
		{nil, nil},
	} {
		got := ParseMaskPaths(tc.in)
		if len(got) != len(tc.want) {
			t.Fatalf("ParseMaskPaths(%v) = %v, want %v", tc.in, got, tc.want)
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Fatalf("ParseMaskPaths(%v) = %v, want %v", tc.in, got, tc.want)
			}
		}
	}
}
