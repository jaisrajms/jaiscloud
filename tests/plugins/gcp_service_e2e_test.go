//go:build plugin_e2e

package plugins

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/jaisrajms/jaiscloud/sdk"
)

// A plugin can add a wire service the emulator does not have: it declares the
// REST paths it answers and one wildcard replace, and the emulator routes those
// paths to it ahead of its own path grammar (see docs/PLUGINS.md, "Plugin
// services"). These tests drive that surface through the real jaiscloud-gcp.

// widgetStore is the in-test state of the example widgets service. The plugin
// runs concurrently, so every access is guarded.
type widgetStore struct {
	mu      sync.Mutex
	widgets map[string]map[string]any
}

// widgetsMux declares the three widgets paths and the wildcard replace that
// answers them, the same shape as sdk/examples/gcp-widgets.
func widgetsMux(name string) (*sdk.Mux, *widgetStore) {
	st := &widgetStore{widgets: map[string]map[string]any{}}
	m := sdk.NewMux(name, "0.1.0")
	m.Service("gcp", "widgets").
		Get("/v1/projects/{project}/locations/{location}/widgets/{widget}", "WidgetsGet").
		Post("/v1/projects/{project}/locations/{location}/widgets", "WidgetsCreate").
		Delete("/v1/projects/{project}/locations/{location}/widgets/{widget}", "WidgetsDelete").
		Replace(widgetsHandle(st))
	return m, st
}

// widgetsHandle answers the widgets actions from st. Captures arrive under their
// names, the query string by key and the JSON body under "body".
func widgetsHandle(st *widgetStore) sdk.Func {
	return func(_ context.Context, c *sdk.Call) (sdk.Result, error) {
		project, _ := c.Req.Params["project"].(string)
		location, _ := c.Req.Params["location"].(string)
		name := func(id string) string {
			return fmt.Sprintf("projects/%s/locations/%s/widgets/%s", project, location, id)
		}
		id, _ := c.Req.Params["widget"].(string)

		switch c.Req.Action {
		case "WidgetsCreate":
			if id == "" {
				id, _ = c.Req.Params["widgetId"].(string)
			}
			if id == "" {
				if body, ok := c.Req.Params["body"].(map[string]any); ok {
					if full, ok := body["name"].(string); ok {
						id = full[strings.LastIndex(full, "/")+1:]
					}
				}
			}
			if id == "" {
				return sdk.Result{Err: &sdk.Error{
					Code: "InvalidArgument", Message: "widgetId is required", Status: 400,
				}}, nil
			}
			st.mu.Lock()
			st.widgets[id] = map[string]any{"name": name(id)}
			st.mu.Unlock()
			return sdk.Result{Resp: &sdk.Response{Data: map[string]any{}}}, nil

		case "WidgetsGet":
			st.mu.Lock()
			w := st.widgets[id]
			st.mu.Unlock()
			if w == nil {
				return sdk.Result{Err: &sdk.Error{
					Code: "NotFound", Message: "widget " + name(id) + " not found", Status: 404,
				}}, nil
			}
			return sdk.Result{Resp: &sdk.Response{Data: w}}, nil

		case "WidgetsDelete":
			st.mu.Lock()
			w := st.widgets[id]
			delete(st.widgets, id)
			st.mu.Unlock()
			if w == nil {
				return sdk.Result{Err: &sdk.Error{
					Code: "NotFound", Message: "widget " + name(id) + " not found", Status: 404,
				}}, nil
			}
			return sdk.Result{Resp: &sdk.Response{Data: map[string]any{}}}, nil
		}

		return sdk.Result{Err: &sdk.Error{
			Code: "NotFound", Message: fmt.Sprintf("unknown action %q", c.Req.Action), Status: 404,
		}}, nil
	}
}

// gcpErrorStatus returns the google.rpc status name of a GCP error envelope.
func gcpErrorStatus(t *testing.T, body map[string]any) string {
	t.Helper()
	env, ok := body["error"].(map[string]any)
	if !ok {
		t.Fatalf("not a GCP error envelope: %v", body)
	}
	status, _ := env["status"].(string)
	return status
}

// TestGCPPluginServiceCRUD drives a plugin's own service: create, get, delete,
// and the not-found envelope after the delete. Doctor must show the declared
// service.
func TestGCPPluginServiceCRUD(t *testing.T) {
	m, _ := widgetsMux("acme-widgets-crud")
	s := gcpSidecar(t, m)
	e := startGCP(t, "--plugin", s.URL)

	const collection = "/v1/projects/p1/locations/l1/widgets"
	if code, out := e.gcpREST(t, http.MethodPost, collection+"?widgetId=w1", map[string]any{}); code != 200 {
		t.Fatalf("create widget: %d %v\n%s", code, out, e.logs())
	}
	code, out := e.gcpREST(t, http.MethodGet, collection+"/w1", nil)
	if code != 200 {
		t.Fatalf("get widget: %d %v\n%s", code, out, e.logs())
	}
	if want := "projects/p1/locations/l1/widgets/w1"; out["name"] != want {
		t.Fatalf("widget name = %v, want %s", out["name"], want)
	}
	if code, out := e.gcpREST(t, http.MethodDelete, collection+"/w1", nil); code != 200 {
		t.Fatalf("delete widget: %d %v\n%s", code, out, e.logs())
	}
	code, out = e.gcpREST(t, http.MethodGet, collection+"/w1", nil)
	if code != 404 {
		t.Fatalf("get after delete: %d %v, want 404\n%s", code, out, e.logs())
	}
	if got := gcpErrorStatus(t, out); got != "NOT_FOUND" {
		t.Fatalf("get after delete status = %q, want NOT_FOUND (body %v)", got, out)
	}

	// Doctor lists the declared service, so an operator can see what the plugin
	// added without reading its manifest.
	st, raw := e.admin(t, http.MethodGet, "/_jaiscloud/doctor", nil)
	if st != 200 {
		t.Fatalf("doctor: %d %s\n%s", st, raw, e.logs())
	}
	var doc struct {
		Plugins []struct {
			Services []struct {
				Name string `json:"name"`
			} `json:"services"`
		} `json:"plugins"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("doctor JSON: %v\n%s", err, raw)
	}
	found := false
	for _, p := range doc.Plugins {
		for _, svc := range p.Services {
			if svc.Name == "widgets" {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("doctor does not list the declared service %q: %s", "widgets", raw)
	}
}

// TestGCPPluginServiceOwnedPathWrongMethod pins the precedence rule: a plugin
// path may sit in a built-in's URL space, so a method no declared path uses is
// the plugin's 404, never the built-in's answer.
func TestGCPPluginServiceOwnedPathWrongMethod(t *testing.T) {
	m, _ := widgetsMux("acme-widgets-ownpath")
	s := gcpSidecar(t, m)
	e := startGCP(t, "--plugin", s.URL)

	code, out := e.gcpREST(t, http.MethodPut, "/v1/projects/p1/locations/l1/widgets/w1", map[string]any{})
	if code != 404 {
		t.Fatalf("PUT on an owned path = %d %v, want 404\n%s", code, out, e.logs())
	}
	if got := gcpErrorStatus(t, out); got != "NOT_FOUND" {
		t.Fatalf("PUT status = %q, want NOT_FOUND (body %v)", got, out)
	}
	if env, _ := out["error"].(map[string]any); env["code"] != float64(404) {
		t.Fatalf("PUT error code = %v, want 404 (body %v)", env["code"], out)
	}
}

// TestGCPPluginServiceSharesOperationsCollection claims only the operations whose
// ID starts with "widgets-"; the rest of the collection still reaches the
// built-in.
func TestGCPPluginServiceSharesOperationsCollection(t *testing.T) {
	const pattern = "/v1/projects/{project}/locations/{location}/operations/{operation=widgets-*}"
	m := sdk.NewMux("acme-widgets-ops", "0.1.0")
	m.Service("gcp", "widgets").
		Get(pattern, "WidgetsOperationGet").
		Replace(func(_ context.Context, c *sdk.Call) (sdk.Result, error) {
			op, _ := c.Req.Params["operation"].(string)
			return sdk.Result{Resp: &sdk.Response{Data: map[string]any{
				"name":   "projects/p1/locations/l1/operations/" + op,
				"done":   true,
				"plugin": true,
			}}}, nil
		})
	s := gcpSidecar(t, m)
	e := startGCP(t, "--plugin", s.URL)

	code, out := e.gcpREST(t, http.MethodGet, "/v1/projects/p1/locations/l1/operations/widgets-abc", nil)
	if code != 200 {
		t.Fatalf("a declared operation = %d %v, want the plugin's answer\n%s", code, out, e.logs())
	}
	if out["plugin"] != true || out["name"] != "projects/p1/locations/l1/operations/widgets-abc" {
		t.Fatalf("declared operation was not answered by the plugin: %v", out)
	}

	// Another operation ID is not one of the declared paths, so the built-in
	// owns it. Its answer is not asserted, only that the plugin did not give it.
	if _, out := e.gcpREST(t, http.MethodGet, "/v1/projects/p1/locations/l1/operations/other-abc", nil); out["plugin"] == true {
		t.Fatalf("an undeclared operation must reach the built-in, got the plugin's answer: %v", out)
	}
}

// TestGCPPluginServicePluginDown pins fail-closed: a plugin service that is down
// answers the GCP UNAVAILABLE envelope and never silently falls back.
func TestGCPPluginServicePluginDown(t *testing.T) {
	m, _ := widgetsMux("acme-widgets-down")
	s := gcpSidecar(t, m)
	e := startGCP(t, "--plugin", s.URL, "--plugin-retry", "200ms")

	s.Close()

	code, out := e.gcpREST(t, http.MethodGet, "/v1/projects/p1/locations/l1/widgets/w1", nil)
	if code != 503 {
		t.Fatalf("a declared path with the plugin down = %d %v, want 503\n%s", code, out, e.logs())
	}
	if got := gcpErrorStatus(t, out); got != "UNAVAILABLE" {
		t.Fatalf("plugin-down status = %q, want UNAVAILABLE (body %v)", got, out)
	}
}

// TestGCPPluginServiceDelegates shows a declared path calling another service's
// REST API at the emulator's own endpoint (Mux.Endpoint, carried by /init).
func TestGCPPluginServiceDelegates(t *testing.T) {
	m := sdk.NewMux("acme-widgets-delegate", "0.1.0")
	m.Service("gcp", "widgets").
		Post("/v1/projects/{project}/locations/{location}/widgets:sync", "WidgetsSync").
		Replace(func(_ context.Context, c *sdk.Call) (sdk.Result, error) {
			endpoint := m.Endpoint()
			if endpoint == "" {
				return sdk.Result{Err: &sdk.Error{
					Code: "Internal", Message: "the host did not send an endpoint", Status: 500,
				}}, nil
			}
			req, err := http.NewRequest(http.MethodPut, endpoint+"/v1/projects/p1/topics/t1", bytes.NewReader([]byte("{}")))
			if err != nil {
				return sdk.Result{}, err
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return sdk.Result{}, err
			}
			defer resp.Body.Close()
			raw, _ := io.ReadAll(resp.Body)
			data := map[string]any{}
			if len(raw) > 0 {
				if err := json.Unmarshal(raw, &data); err != nil {
					return sdk.Result{}, err
				}
			}
			// Carry the delegated status and body back to the client unchanged.
			return sdk.Result{Resp: &sdk.Response{Status: resp.StatusCode, Data: data}}, nil
		})
	s := gcpSidecar(t, m)
	e := startGCP(t, "--plugin", s.URL)

	code, out := e.gcpREST(t, http.MethodPost, "/v1/projects/p1/locations/l1/widgets:sync", map[string]any{})
	if code != 200 {
		t.Fatalf("delegating handler: %d %v\n%s", code, out, e.logs())
	}

	// The topic really exists, which proves the sidecar got the endpoint and the
	// built-in ran the delegated PUT.
	code, out = e.gcpREST(t, http.MethodGet, "/v1/projects/p1/topics/t1", nil)
	if code != 200 {
		t.Fatalf("the delegated topic was not created: %d %v\n%s", code, out, e.logs())
	}
	if out["name"] != "projects/p1/topics/t1" {
		t.Fatalf("topic name = %v, want projects/p1/topics/t1", out["name"])
	}
}
