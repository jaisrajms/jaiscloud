package pluginhost

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"

	"github.com/jaisrajms/jaiscloud/sdk"
)

// ---- fixtures ---------------------------------------------------------------

// fakeCodec encodes nothing: the tests only check which service's codec the
// matcher hands back, which is what the adapter would encode a response with.
type fakeCodec struct{ service string }

func (c fakeCodec) ServiceName() string { return c.service }
func (c fakeCodec) Decode(*http.Request, []byte) (*model.NormalizedRequest, error) {
	return nil, nil
}
func (c fakeCodec) Encode(*model.NormalizedRequest, *model.ProviderResponse) (int, http.Header, []byte) {
	return http.StatusOK, nil, nil
}
func (c fakeCodec) EncodeError(*model.NormalizedRequest, *model.ProviderError) (int, http.Header, []byte) {
	return http.StatusBadRequest, nil, nil
}

func widgetPaths() []sdk.Path {
	return []sdk.Path{
		{Method: "GET", Pattern: "/v1/projects/{project}/widgets", Action: "WidgetsList"},
		{Method: "POST", Pattern: "/v1/projects/{project}/widgets", Action: "WidgetsCreate"},
		{Method: "GET", Pattern: "/v1/projects/{project}/widgets/{widget}", Action: "WidgetsGet"},
		{Method: "DELETE", Pattern: "/v1/projects/{project}/widgets/{widget}", Action: "WidgetsDelete"},
		{Method: "GET", Pattern: "/v1/projects/{project}/things/{thing=widget-*}", Action: "ThingsGet"},
		{Method: "POST", Pattern: "/v1/projects/{project}/runs/{run}:cancel", Action: "RunsCancel"},
	}
}

func widgetServices() []sdk.Service {
	return []sdk.Service{{Cloud: "aws", Name: "widgets", Paths: widgetPaths()}}
}

func matchRequest(method, path, query string) *http.Request {
	return &http.Request{Method: method, URL: &url.URL{Path: path, RawQuery: query}, Header: http.Header{}}
}

func mustMatcher(t *testing.T, services ...sdk.Service) *matcher {
	t.Helper()
	m, err := newMatcher(services, fakeAdapter{})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// ---- compile and match -------------------------------------------------------

func TestMatcherCompileAndMatch(t *testing.T) {
	m := mustMatcher(t, widgetServices()...)
	cases := []struct {
		name       string
		method     string
		path       string
		body       string
		wantFound  bool
		wantAction string
		wantCode   string
		wantStatus int
	}{
		{name: "literal", method: "GET", path: "/v1/projects/acme/widgets", wantFound: true, wantAction: "WidgetsList"},
		{name: "leading double slash and trailing slash", method: "GET", path: "//v1/projects/acme/widgets/", wantFound: true, wantAction: "WidgetsList"},
		{name: "variable capture", method: "GET", path: "/v1/projects/acme/widgets/w1", wantFound: true, wantAction: "WidgetsGet"},
		{name: "prefix capture", method: "GET", path: "/v1/projects/acme/things/widget-7", wantFound: true, wantAction: "ThingsGet"},
		{name: "verb", method: "POST", path: "/v1/projects/acme/runs/r1:cancel", wantFound: true, wantAction: "RunsCancel"},
		{name: "owned path with another method", method: "PUT", path: "/v1/projects/acme/widgets", wantFound: true, wantCode: "NotFound", wantStatus: http.StatusNotFound},
		{name: "verb does not match another verb", method: "POST", path: "/v1/projects/acme/runs/r1:wait"},
		{name: "verb pattern needs the verb", method: "POST", path: "/v1/projects/acme/runs/r1"},
		{name: "unknown method on a verb path owns it", method: "DELETE", path: "/v1/projects/acme/runs/r1:cancel", wantFound: true, wantCode: "NotFound", wantStatus: http.StatusNotFound},
		{name: "prefix capture mismatch", method: "GET", path: "/v1/projects/acme/things/gadget-7"},
		{name: "unknown path", method: "GET", path: "/v1/projects/acme/gadgets"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			nr, codec, found, err := m.Match(matchRequest(tc.method, tc.path, ""), []byte(tc.body))
			if found != tc.wantFound {
				t.Fatalf("found = %v, want %v (nr=%v err=%v)", found, tc.wantFound, nr, err)
			}
			if tc.wantCode != "" {
				pe, ok := err.(*model.ProviderError)
				if !ok || pe.Code != tc.wantCode || pe.HTTPStatus != tc.wantStatus {
					t.Fatalf("err = %v, want %s/%d", err, tc.wantCode, tc.wantStatus)
				}
				if codec == nil || codec.ServiceName() != "widgets" {
					t.Fatalf("an owned path must come with the service's codec, got %v", codec)
				}
				if nr != nil {
					t.Fatalf("an owned-path error carries no request, got %+v", nr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !tc.wantFound {
				if nr != nil {
					t.Fatalf("no match returned a request: %+v", nr)
				}
				return
			}
			if nr == nil || nr.Action != tc.wantAction || nr.Service != "widgets" {
				t.Fatalf("request = %+v, want action %s of service widgets", nr, tc.wantAction)
			}
			if codec == nil || codec.ServiceName() != "widgets" {
				t.Fatalf("codec = %v, want the widgets codec", codec)
			}
			if _, has := nr.Params["name"]; has {
				t.Fatalf("name must not be synthesised: %v", nr.Params)
			}
			if _, has := nr.Params["resourceType"]; has {
				t.Fatalf("resourceType must not be synthesised: %v", nr.Params)
			}
		})
	}
}

func TestMatcherBuildsParams(t *testing.T) {
	m := mustMatcher(t, widgetServices()...)
	// The URL holds the decoded path; EscapedPath re-escapes it, so the matcher
	// must unescape the capture before handing it to the plugin.
	req := matchRequest("GET", "/v1/projects/acme/widgets/w 1", "alt=json&maxResults=5&maxResults=6")
	nr, codec, found, err := m.Match(req, nil)
	if err != nil || !found || nr == nil {
		t.Fatalf("match = %v, %v, %v", nr, found, err)
	}
	if codec == nil || codec.ServiceName() != "widgets" {
		t.Fatalf("codec = %v", codec)
	}
	want := map[string]string{
		"project": "acme", "widget": "w 1", "alt": "json", "maxResults": "5",
		"apiVersion": "v1", "path": req.URL.Path,
	}
	for k, v := range want {
		if got, _ := nr.Params[k].(string); got != v {
			t.Errorf("Params[%q] = %v, want %q (params: %v)", k, nr.Params[k], v, nr.Params)
		}
	}

	// A JSON body is decoded into "body"; an object is the usual shape.
	nr, _, _, err = m.Match(matchRequest("POST", "/v1/projects/acme/widgets", ""), []byte(`{"title":"x","n":2}`))
	if err != nil || nr == nil {
		t.Fatalf("body match = %v, %v", nr, err)
	}
	body, ok := nr.Params["body"].(map[string]any)
	if !ok || body["title"] != "x" {
		t.Fatalf("body = %v", nr.Params["body"])
	}

	// A non-empty body that is not JSON is a 400, encoded by the service codec.
	_, codec, found, err = m.Match(matchRequest("POST", "/v1/projects/acme/widgets", ""), []byte(`{"title":`))
	if !found || codec == nil {
		t.Fatalf("a malformed body must still be an owned request: found=%v codec=%v", found, codec)
	}
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.Code != "InvalidArgument" || pe.HTTPStatus != http.StatusBadRequest || pe.Message != "malformed JSON body" {
		t.Fatalf("malformed body err = %v", err)
	}

	// An empty body is not an error and sets no "body" param.
	nr, _, _, err = m.Match(matchRequest("POST", "/v1/projects/acme/widgets", ""), nil)
	if err != nil || nr == nil {
		t.Fatalf("empty body match = %v, %v", nr, err)
	}
	if _, has := nr.Params["body"]; has {
		t.Fatalf("an empty body must not set body: %v", nr.Params)
	}
}

func TestMatcherPluginPathsOrder(t *testing.T) {
	m := mustMatcher(t, widgetServices()...)
	var paths []adapter.PluginPath
	if p, ok := adapter.PluginServiceMatcher(m).(adapter.PluginServicePaths); ok {
		paths = p.PluginPaths()
	}
	if len(paths) != len(widgetPaths()) {
		t.Fatalf("paths = %v, want %d entries", paths, len(widgetPaths()))
	}
	for i, want := range widgetPaths() {
		got := paths[i]
		if got.Service != "widgets" || got.Method != want.Method || got.Pattern != want.Pattern {
			t.Fatalf("path %d = %+v, want %s %s", i, got, want.Method, want.Pattern)
		}
	}
}

// countingAdapter shows the matcher asks for one codec per service, at build time.
type countingAdapter struct {
	fakeAdapter
	calls int
}

func (a *countingAdapter) PluginCodec(service string) (adapter.Codec, error) {
	a.calls++
	return a.fakeAdapter.PluginCodec(service)
}

func TestMatcherCachesOneCodecPerService(t *testing.T) {
	a := &countingAdapter{}
	if _, err := newMatcher([]sdk.Service{
		{Cloud: "aws", Name: "widgets", Paths: widgetPaths()},
		{Cloud: "aws", Name: "gadgets"},
	}, a); err != nil {
		t.Fatal(err)
	}
	if a.calls != 2 {
		t.Fatalf("PluginCodec calls = %d, want one per service", a.calls)
	}
	if _, err := newMatcher([]sdk.Service{{Cloud: "aws", Name: "sqs"}}, fakeAdapter{}); err == nil {
		t.Fatal("a codec error at build time must be a load error")
	}
}

func TestCompilePatternRejects(t *testing.T) {
	for _, pattern := range []string{
		"v1/projects/widgets",                 // no leading /
		"/v1",                                 // one segment
		"/v1/{project}/widgets",               // one literal leading segment
		"/{project}/v1/widgets",               // variable first
		"/projects/widgets",                   // no version token
		"/v1/projects/{widget",                // stray {
		"/v1/projects/wid}gets",               // stray }
		"/v1/projects/{=*}/widgets",           // empty capture name
		"/v1/projects/{name=*}/widgets",       // empty prefix
		"/v1/{project}:cancel/widgets",        // :verb is only allowed last
		"/v1/projects/widgets/",               // empty last segment
		"/v1/projects/{name=prefix}/widgets",  // prefix capture without *
		"/v1/projects/{name=pre*fix}/widgets", // * is not the suffix
	} {
		if _, err := compilePattern(pattern); err == nil {
			t.Errorf("pattern %q must be rejected", pattern)
		}
	}
	for _, pattern := range []string{
		"/v1/projects/{project}/widgets/{widget}",
		"/v1/projects/{project}/things/{name=widget-*}",
		"/v1/projects/{project}/runs/{run}:cancel",
		"/v2beta1/widgets",
	} {
		if _, err := compilePattern(pattern); err != nil {
			t.Errorf("pattern %q must compile: %v", pattern, err)
		}
	}
}

// ---- overlap predicate -------------------------------------------------------

func compileOne(t *testing.T, method, pattern string) *compiledPath {
	t.Helper()
	cp, err := compilePattern(pattern)
	if err != nil {
		t.Fatalf("pattern %q: %v", pattern, err)
	}
	cp.method, cp.raw = method, pattern
	return cp
}

func TestPathsOverlap(t *testing.T) {
	cases := []struct {
		name string
		a, b string
		want bool
	}{
		{"same literal", "GET /v1/projects/acme/widgets", "GET /v1/projects/acme/widgets", true},
		{"literal and variable", "GET /v1/projects/acme/widgets", "GET /v1/projects/{project}/widgets", true},
		{"two variables", "GET /v1/projects/{project}/widgets", "GET /v1/projects/{other}/widgets", true},
		{"prefix and literal inside it", "GET /v1/projects/{p=acme-*}/widgets", "GET /v1/projects/acme-1/widgets", true},
		{"prefix and literal outside it", "GET /v1/projects/{p=other-*}/widgets", "GET /v1/projects/acme-1/widgets", false},
		{"prefixes where one starts the other", "GET /v1/projects/{p=a-*}/widgets", "GET /v1/projects/{q=a-b-*}/widgets", true},
		{"prefixes that diverge", "GET /v1/projects/{p=a-*}/widgets", "GET /v1/projects/{q=b-*}/widgets", false},
		{"different verbs", "POST /v1/projects/{project}/runs/{run}:cancel", "POST /v1/projects/{project}/runs/{run}:wait", false},
		{"verb and no verb", "POST /v1/projects/{project}/runs/{run}:cancel", "POST /v1/projects/{project}/runs/{run}", false},
		{"same verb", "POST /v1/projects/{project}/runs/{run}:cancel", "POST /v1/projects/{project}/runs/{other}:cancel", true},
		{"different methods", "GET /v1/projects/{project}/widgets", "POST /v1/projects/{project}/widgets", false},
		{"different segment counts", "GET /v1/projects/{project}/widgets", "GET /v1/projects/{project}/widgets/{widget}", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			am, ap, _ := strings.Cut(tc.a, " ")
			bm, bp, _ := strings.Cut(tc.b, " ")
			if got := pathsOverlap(compileOne(t, am, ap), compileOne(t, bm, bp)); got != tc.want {
				t.Fatalf("%s overlaps %s = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

// ---- adopt and apply ---------------------------------------------------------

func widgetMux() func(*sdk.Mux) {
	return func(m *sdk.Mux) {
		m.Service("aws", "widgets").
			Get("/v1/projects/{project}/widgets", "WidgetsList").
			Post("/v1/projects/{project}/widgets", "WidgetsCreate").
			Get("/v1/projects/{project}/widgets/{widget}", "WidgetsGet").
			Delete("/v1/projects/{project}/widgets/{widget}", "WidgetsDelete").
			Replace(noop)
	}
}

func TestPluginServiceLoadsAndRoutes(t *testing.T) {
	e := newEnv(t)
	var seen []string
	p := muxRoutes("w", func(m *sdk.Mux) {
		m.Service("aws", "widgets").
			Get("/v1/projects/{project}/widgets", "WidgetsList").
			Post("/v1/projects/{project}/widgets", "WidgetsCreate").
			Replace(func(_ context.Context, c *sdk.Call) (sdk.Result, error) {
				seen = append(seen, c.Req.Service+"."+c.Req.Action)
				return sdk.Result{Resp: &sdk.Response{Data: map[string]any{"ok": true}}}, nil
			})
	})
	h, err := e.load(t, serve(t, p).URL)
	if err != nil {
		t.Fatal(err)
	}
	sm := h.ServiceMatcher()
	if sm == nil {
		t.Fatal("a plugin that declares a service must install a matcher")
	}
	nr, codec, found, err := sm.Match(matchRequest("GET", "/v1/projects/acme/widgets", ""), nil)
	if err != nil || !found || nr == nil {
		t.Fatalf("match = %v, %v, %v", nr, found, err)
	}
	if codec == nil || codec.ServiceName() != "widgets" || nr.Action != "WidgetsList" {
		t.Fatalf("match = %+v, codec %v", nr, codec)
	}
	// The wildcard replace the plugin declared answers the action the matcher
	// derived, through the registry, like a built-in would.
	resp, err := e.reg.Dispatch(context.Background(), "widgets."+nr.Action, nr)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Data["ok"] != true || strings.Join(seen, ",") != "widgets.WidgetsList" {
		t.Fatalf("dispatch = %v, plugin saw %v", resp, seen)
	}
}

func TestServiceMatcherIsNilWithoutDeclaredServices(t *testing.T) {
	e := newEnv(t)
	p := muxRoutes("p", func(m *sdk.Mux) { m.Before("aws", "sqs", "SendMessage", noop) })
	h, err := e.load(t, serve(t, p).URL)
	if err != nil {
		t.Fatal(err)
	}
	if sm := h.ServiceMatcher(); sm != nil {
		t.Fatalf("no declared service must give no matcher, got %v", sm)
	}
	var nilHost *Host
	if sm := nilHost.ServiceMatcher(); sm != nil {
		t.Fatalf("a nil host must give no matcher, got %v", sm)
	}
}

func TestPluginServiceValidation(t *testing.T) {
	cases := []struct {
		name  string
		setup func(*env)
		a, b  func(*sdk.Mux)
		want  []string
	}{
		{
			name: "built-in service name",
			a: func(m *sdk.Mux) {
				m.Service("aws", "sqs").Get("/v1/projects/{project}/queues", "QueuesList").Replace(noop)
			},
			want: []string{"built in", "sqs"},
		},
		{
			name: "declared action must be answered",
			a: func(m *sdk.Mux) {
				m.Service("aws", "widgets").Get("/v1/projects/{project}/widgets", "WidgetsList")
				m.Replace("aws", "widgets", "WidgetsGet", noop)
			},
			want: []string{"WidgetsList", "not answered"},
		},
		{
			name: "own service cannot be wrapped",
			a: func(m *sdk.Mux) {
				m.Service("aws", "widgets").Get("/v1/projects/{project}/widgets", "WidgetsList")
				m.Before("aws", "widgets", "WidgetsList", noop)
				m.Replace("aws", "widgets", "*", noop)
			},
			want: []string{`mode "before" is not supported`, "widgets"},
		},
		{
			name: "bad pattern",
			a: func(m *sdk.Mux) {
				m.Service("aws", "widgets").Get("/v1/{project}/widgets", "WidgetsList").Replace(noop)
			},
			want: []string{"at least two literal", "/v1/{project}/widgets"},
		},
		{
			name: "duplicate service name",
			a:    widgetMux(),
			b: func(m *sdk.Mux) {
				m.Service("aws", "widgets").Get("/v1/projects/{project}/gadgets", "GadgetsList").Replace(noop)
			},
			want: []string{`both declare service "widgets"`, `"a"`, `"b"`},
		},
		{
			name: "overlapping patterns",
			a:    widgetMux(),
			b: func(m *sdk.Mux) {
				m.Service("aws", "gadgets").
					Get("/v1/projects/{project}/widgets/{thing}", "GadgetsGet").
					Replace(noop)
			},
			want: []string{`"a"`, `"b"`, "/v1/projects/{project}/widgets/{widget}", "/v1/projects/{project}/widgets/{thing}"},
		},
		{
			name: "route into another plugin's service",
			setup: func(e *env) {
				e.reg.RegisterAll(map[string]provider.HandlerFunc{"widgets.Ping": func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error) {
					return provider.OK(map[string]any{}), nil
				}})
			},
			a:    widgetMux(),
			b:    func(m *sdk.Mux) { m.Replace("aws", "widgets", "*", noop) },
			want: []string{`plugin "b" routes service "widgets"`, `plugin "a"`},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			if tc.setup != nil {
				tc.setup(e)
			}
			urls := []string{serve(t, muxRoutes("a", tc.a)).URL}
			if tc.b != nil {
				urls = append(urls, serve(t, muxRoutes("b", tc.b)).URL)
			}
			_, err := e.load(t, urls...)
			if err == nil {
				t.Fatalf("want a load error containing %v", tc.want)
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("err = %v, want it to contain %q", err, want)
				}
			}
		})
	}
}
