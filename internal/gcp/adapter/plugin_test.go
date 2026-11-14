package gcp

import (
	"bytes"
	"compress/gzip"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"jaiscloud/internal/adapter"
	"jaiscloud/internal/model"
)

// stubPluginMatcher is a PluginServiceMatcher that claims every request it is
// consulted for and records what it saw, so a test can prove both that the
// adapter routed to it and that it was skipped when a host rule won.
type stubPluginMatcher struct {
	paths []adapter.PluginPath

	nr    *model.NormalizedRequest
	codec adapter.Codec
	err   error

	calls  int
	method string
	body   []byte
}

func (m *stubPluginMatcher) Match(r *http.Request, body []byte) (*model.NormalizedRequest, adapter.Codec, bool, error) {
	m.calls++
	m.method = r.Method
	m.body = append([]byte(nil), body...)
	return m.nr, m.codec, true, m.err
}

func (m *stubPluginMatcher) PluginPaths() []adapter.PluginPath { return m.paths }

// newStubMatcher returns a matcher that claims a plain-origin request as a
// synthetic "widgets" service, the shape a declaring plugin produces.
func newStubMatcher() *stubPluginMatcher {
	codec := &JSONCodec{Service: "widgets"}
	return &stubPluginMatcher{
		nr:    &model.NormalizedRequest{Service: "widgets", Params: map[string]any{}},
		codec: codec,
	}
}

// TestPluginServicesHostPrecedence locks the host > plugin > path order: a
// Functions trigger host and a *.run.app host keep their data-plane codecs even
// though the matcher claims every request, while a plain-origin path the matcher
// claims returns the matcher's request and codec untouched.
func TestPluginServicesHostPrecedence(t *testing.T) {
	t.Run("functions trigger host", func(t *testing.T) {
		m := newStubMatcher()
		a := New()
		a.SetPluginServices(m)

		r := httptest.NewRequest(http.MethodPost, "/hello", strings.NewReader("payload"))
		r.Host = "us-central1-proj.cloudfunctions.net"

		nr, _, err := a.DetectAndDecode(r, []byte("payload"))
		if err != nil {
			t.Fatalf("DetectAndDecode: %v", err)
		}
		if nr.Service != "functions" || nr.Action != "InvokeTrigger" {
			t.Fatalf("got service=%q action=%q, want functions/InvokeTrigger", nr.Service, nr.Action)
		}
		if m.calls != 0 {
			t.Errorf("matcher was consulted %d times for a trigger host, want 0", m.calls)
		}
	})

	t.Run("run invocation host", func(t *testing.T) {
		m := newStubMatcher()
		a := New()
		a.SetPluginServices(m)

		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Host = "s1-abcdef012345.us-central1.run.app"

		nr, _, err := a.DetectAndDecode(r, nil)
		if err != nil {
			t.Fatalf("DetectAndDecode: %v", err)
		}
		if nr.Service != "run" {
			t.Fatalf("got service=%q, want run", nr.Service)
		}
		if m.calls != 0 {
			t.Errorf("matcher was consulted %d times for a *.run.app host, want 0", m.calls)
		}
	})

	t.Run("plain origin claimed by the plugin", func(t *testing.T) {
		m := newStubMatcher()
		a := New()
		a.SetPluginServices(m)

		r := httptest.NewRequest(http.MethodGet, "/v1/projects/p/widgets/w", nil)

		nr, codec, err := a.DetectAndDecode(r, nil)
		if err != nil {
			t.Fatalf("DetectAndDecode: %v", err)
		}
		if nr != m.nr {
			t.Errorf("got request %p, want the matcher's %p", nr, m.nr)
		}
		if codec != m.codec {
			t.Errorf("got codec %T, want the matcher's", codec)
		}
		if m.calls != 1 {
			t.Errorf("matcher calls = %d, want 1", m.calls)
		}
	})
}

// TestPluginServicesNilMatcherKeepsRouting compares the adapter against plain
// DetectService routing with no matcher installed, so the plugin seam can never
// change a built-in decision by existing.
func TestPluginServicesNilMatcherKeepsRouting(t *testing.T) {
	a := New()
	cases := []struct {
		method string
		path   string
		want   string
	}{
		{http.MethodGet, "/v1/projects/p/topics/t", "pubsub"},
		{http.MethodPost, "/storage/v1/b?project=p", "storage"},
		{http.MethodPost, "/bigquery/v2/projects/p/queries", "bigquery"},
	}
	for _, tc := range cases {
		r := httptest.NewRequest(tc.method, tc.path, nil)
		wantService, wantSource := DetectService(r)
		if wantService != tc.want || wantSource != SourcePath {
			t.Fatalf("fixture %s %s: DetectService = (%q,%v), want (%q,SourcePath)",
				tc.method, tc.path, wantService, wantSource, tc.want)
		}

		nr, codec, err := a.DetectAndDecode(httptest.NewRequest(tc.method, tc.path, nil), nil)
		if err != nil {
			t.Fatalf("%s %s: DetectAndDecode: %v", tc.method, tc.path, err)
		}
		if nr.Service != tc.want {
			t.Errorf("%s %s: decoded service %q, want %q", tc.method, tc.path, nr.Service, tc.want)
		}
		if codec.ServiceName() != tc.want {
			t.Errorf("%s %s: codec %q, want %q", tc.method, tc.path, codec.ServiceName(), tc.want)
		}
	}
}

// TestPluginServicesSeesOverrideAndGzipBody verifies the matcher is consulted
// after the same preprocessing a built-in gets: the method override is applied
// and the gzip transport body is decompressed before Match, so a plugin never
// has to re-do either.
func TestPluginServicesSeesOverrideAndGzipBody(t *testing.T) {
	const plain = `{"displayName":"w"}`
	m := newStubMatcher()
	a := New()
	a.SetPluginServices(m)

	r := httptest.NewRequest(http.MethodPost, "/v1/projects/p/widgets", nil)
	r.Header.Set("X-HTTP-Method-Override", http.MethodPatch)
	r.Header.Set("Content-Encoding", "gzip")

	if _, _, err := a.DetectAndDecode(r, gzipBytes(t, []byte(plain))); err != nil {
		t.Fatalf("DetectAndDecode: %v", err)
	}
	if m.method != http.MethodPatch {
		t.Errorf("matcher saw method %q, want PATCH", m.method)
	}
	if string(m.body) != plain {
		t.Errorf("matcher saw body %q, want %q", m.body, plain)
	}
}

func gzipBytes(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(b); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

// TestPluginServicesMatcherError verifies a claimed-but-unservable request keeps
// the plugin codec, since the gateway encodes the error with it in the cloud's
// own envelope.
func TestPluginServicesMatcherError(t *testing.T) {
	m := newStubMatcher()
	m.nr = nil
	perr := model.NewProviderError("NotFound", "widget not found", 404)
	m.err = perr

	a := New()
	a.SetPluginServices(m)

	r := httptest.NewRequest(http.MethodGet, "/v1/projects/p/widgets/w", nil)
	nr, codec, err := a.DetectAndDecode(r, nil)
	if err != perr {
		t.Fatalf("err = %v, want the matcher's %v", err, perr)
	}
	if nr != nil {
		t.Errorf("request = %v, want nil", nr)
	}
	if codec != m.codec {
		t.Errorf("codec = %T, want the matcher's", codec)
	}
}

// TestPluginCodec locks the built-in rejection set: the name check is the
// adapter's because this package owns the service vocabulary.
func TestPluginCodec(t *testing.T) {
	a := New()

	codec, err := a.PluginCodec("widgets")
	if err != nil {
		t.Fatalf("PluginCodec(widgets): %v", err)
	}
	jc, ok := codec.(*JSONCodec)
	if !ok {
		t.Fatalf("codec = %T, want *JSONCodec", codec)
	}
	if jc.Service != "widgets" {
		t.Errorf("codec service = %q, want widgets", jc.Service)
	}

	for _, name := range []string{"", "storage", "dataproc", "functions", "firestoreadmin"} {
		_, err := a.PluginCodec(name)
		if err == nil {
			t.Errorf("PluginCodec(%q) succeeded, want the built-in rejection", name)
			continue
		}
		want := `service "` + name + `" is built in; a plugin can only add services the emulator does not have`
		if err.Error() != want {
			t.Errorf("PluginCodec(%q) error = %q, want %q", name, err, want)
		}
	}
}

// TestSetPluginServicesWarnsOnOverlap verifies the startup audit: a declared
// pattern that the built-in grammar would otherwise resolve logs a warning
// naming the shadowed service, while the load itself still succeeds.
func TestSetPluginServicesWarnsOnOverlap(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	m := newStubMatcher()
	m.paths = []adapter.PluginPath{
		{Service: "widgets", Method: http.MethodGet, Pattern: "/v1/projects/{project}/topics/{topic}"},
		{Service: "widgets", Method: http.MethodGet, Pattern: "/v1/projects/{project}/widgets/{widget=widget-*}"},
	}
	New().SetPluginServices(m)

	logged := buf.String()
	if !strings.Contains(logged, "plugin service path overlaps a built-in") {
		t.Fatalf("no overlap warning logged, got %q", logged)
	}
	if !strings.Contains(logged, "builtin=pubsub") {
		t.Errorf("warning does not name pubsub, got %q", logged)
	}
	if got := strings.Count(logged, "plugin service path overlaps a built-in"); got != 1 {
		t.Errorf("overlap warnings = %d, want 1 (only the topics pattern is claimed)", got)
	}
}
