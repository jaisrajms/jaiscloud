package sdk

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func noop(context.Context, *Call) (Result, error) { return Result{}, nil }

func TestServiceBuilderAccumulatesOneService(t *testing.T) {
	m := NewMux("t", "1")
	m.Service("gcp", "widgets").
		Get("/v1/projects/{project}/widgets/{widget}", "WidgetsGet").
		Post("/v1/projects/{project}/widgets", "WidgetsCreate").
		Replace(noop).
		Replace(noop) // harmless: replaces the wildcard function, not the route
	m.Service("gcp", "widgets").
		Delete("/v1/projects/{project}/widgets/{widget}", "WidgetsDelete")
	m.Service("gcp", "gadgets").
		Patch("/v1/projects/{project}/gadgets/{gadget}", "GadgetsPatch")

	mf := m.Manifest()
	if len(mf.Services) != 2 {
		t.Fatalf("services = %+v, want two (repeated Service calls must accumulate)", mf.Services)
	}
	widgets := mf.Services[0]
	if widgets.Cloud != "gcp" || widgets.Name != "widgets" {
		t.Fatalf("first service = %+v, want gcp/widgets in declaration order", widgets)
	}
	wantPaths := []Path{
		{Method: "GET", Pattern: "/v1/projects/{project}/widgets/{widget}", Action: "WidgetsGet"},
		{Method: "POST", Pattern: "/v1/projects/{project}/widgets", Action: "WidgetsCreate"},
		{Method: "DELETE", Pattern: "/v1/projects/{project}/widgets/{widget}", Action: "WidgetsDelete"},
	}
	if !reflect.DeepEqual(widgets.Paths, wantPaths) {
		t.Fatalf("paths = %+v, want %+v in call order", widgets.Paths, wantPaths)
	}
	if g := mf.Services[1]; g.Name != "gadgets" || len(g.Paths) != 1 || g.Paths[0].Action != "GadgetsPatch" {
		t.Fatalf("second service = %+v", g)
	}

	var wildcards []Route
	for _, r := range mf.Routes {
		if r.Service == "widgets" && r.Mode == Replace && len(r.Actions) == 1 && r.Actions[0] == "*" {
			wildcards = append(wildcards, r)
		}
	}
	if len(wildcards) != 1 {
		t.Fatalf("widgets replace routes = %+v, want exactly one wildcard", mf.Routes)
	}
	if !hasCap(mf.Requires, CapServices) {
		t.Fatalf("requires = %v, want %q", mf.Requires, CapServices)
	}
}

func TestManifestAddsCapServicesOnce(t *testing.T) {
	m := NewMux("t", "1")
	m.Require(CapInProcess)
	m.Service("gcp", "widgets").Replace(noop)
	m.Service("aws", "other")
	for i := 0; i < 2; i++ {
		mf := m.Manifest()
		if !reflect.DeepEqual(mf.Requires, []string{CapInProcess, CapServices}) {
			t.Fatalf("manifest %d: requires = %v, want [%s %s]", i, mf.Requires, CapInProcess, CapServices)
		}
	}
	m.Require(CapServices)
	if mf := m.Manifest(); !reflect.DeepEqual(mf.Requires, []string{CapInProcess, CapServices}) {
		// Already listed by the author: the builder does not add a second copy.
		t.Fatalf("explicit requires = %v, want one %s", mf.Requires, CapServices)
	}
}

func TestManifestServicesJSONRoundTrip(t *testing.T) {
	m := NewMux("t", "1")
	m.Service("gcp", "widgets").
		Get("/v2/projects/{project}/locations/{location}/widgets/{widget}", "WidgetsGet").
		Post("/v2/projects/{project}/locations/{location}/widgets", "WidgetsCreate")
	mf := m.Manifest()

	b, err := json.Marshal(mf)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"services":[`) || !strings.Contains(string(b), `"requires":["`+CapServices+`"]`) {
		t.Fatalf("manifest json = %s", b)
	}
	var back Manifest
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(back.Services, mf.Services) {
		t.Fatalf("round-tripped services = %+v, want %+v", back.Services, mf.Services)
	}
	if !reflect.DeepEqual(back.Requires, mf.Requires) {
		t.Fatalf("round-tripped requires = %v, want %v", back.Requires, mf.Requires)
	}
}

func TestManifestWithoutServicesIsUnchanged(t *testing.T) {
	m := newMux()
	mf := m.Manifest()
	if mf.Services != nil {
		t.Fatalf("services = %+v, want nil", mf.Services)
	}
	if mf.Requires != nil {
		t.Fatalf("requires = %v, want nil", mf.Requires)
	}
	b, err := json.Marshal(mf)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), `"services"`) || strings.Contains(string(b), `"requires"`) {
		t.Fatalf("manifest json = %s, want no services and no requires key", b)
	}
	if len(mf.Routes) != 2 {
		t.Fatalf("routes = %+v, want the two grouped routes", mf.Routes)
	}

	m.Require(CapInProcess)
	if got := m.Manifest().Requires; !reflect.DeepEqual(got, []string{CapInProcess}) {
		t.Fatalf("requires = %v, want [%s]", got, CapInProcess)
	}
}

// The host's base URL reaches a Mux through Init, for a handler that delegates
// to another service's REST API.
func TestMuxKeepsTheHostEndpoint(t *testing.T) {
	m := NewMux("t", "1")
	if m.Endpoint() != "" {
		t.Fatalf("endpoint before Init = %q, want empty", m.Endpoint())
	}
	if err := m.Init(context.Background(), Host{Endpoint: "http://127.0.0.1:8080"}); err != nil {
		t.Fatal(err)
	}
	if got := m.Endpoint(); got != "http://127.0.0.1:8080" {
		t.Fatalf("endpoint = %q", got)
	}
}
