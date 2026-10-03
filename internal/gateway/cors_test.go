package gateway

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// ─── corsExtractBucket ────────────────────────────────────────────────────────

func TestCorsExtractBucket_VirtualHosted(t *testing.T) {
	req, _ := http.NewRequest(http.MethodOptions, "/", nil)
	req.Host = "mybucket.s3.us-east-1.amazonaws.com"
	got := corsExtractBucket(req)
	if got != "mybucket" {
		t.Fatalf("want mybucket, got %q", got)
	}
}

func TestCorsExtractBucket_PathStyle(t *testing.T) {
	req, _ := http.NewRequest(http.MethodOptions, "/mybucket/key.txt", nil)
	got := corsExtractBucket(req)
	if got != "mybucket" {
		t.Fatalf("want mybucket, got %q", got)
	}
}

// ─── corsMatchRule ────────────────────────────────────────────────────────────

func TestCORS_Preflight_Match(t *testing.T) {
	rules := []map[string]any{
		{
			"AllowedOrigins": []string{"https://example.com"},
			"AllowedMethods": []string{"GET", "PUT"},
			"MaxAgeSeconds":  300,
		},
	}
	rule, ok := corsMatchRule(rules, "https://example.com", "GET")
	if !ok {
		t.Fatal("want match for allowed origin+method")
	}
	if rule == nil {
		t.Fatal("matched rule must not be nil")
	}
}

func TestCORS_Preflight_NoMatch_Origin(t *testing.T) {
	rules := []map[string]any{
		{
			"AllowedOrigins": []string{"https://example.com"},
			"AllowedMethods": []string{"GET"},
		},
	}
	_, ok := corsMatchRule(rules, "https://evil.com", "GET")
	if ok {
		t.Fatal("want no match for unlisted origin")
	}
}

func TestCORS_Preflight_NoMatch_Method(t *testing.T) {
	rules := []map[string]any{
		{
			"AllowedOrigins": []string{"*"},
			"AllowedMethods": []string{"GET"},
		},
	}
	_, ok := corsMatchRule(rules, "https://any.com", "DELETE")
	if ok {
		t.Fatal("want no match for disallowed method")
	}
}

func TestCORS_Wildcard_Origin(t *testing.T) {
	rules := []map[string]any{
		{
			"AllowedOrigins": []string{"*"},
			"AllowedMethods": []string{"GET"},
		},
	}
	_, ok := corsMatchRule(rules, "https://anything.com", "GET")
	if !ok {
		t.Fatal("wildcard origin must match any origin")
	}
}

// ─── corsWritePreflightHeaders ────────────────────────────────────────────────

func TestCORS_RegularRequest_HeadersAdded(t *testing.T) {
	rules := []map[string]any{
		{
			"AllowedOrigins": []string{"https://example.com"},
			"AllowedMethods": []string{"GET"},
			"ExposeHeaders":  []string{"x-custom-header"},
		},
	}
	h := http.Header{}
	CORSAddResponseHeaders(h, rules, "https://example.com")
	if h.Get("Access-Control-Allow-Origin") != "https://example.com" {
		t.Fatalf("want ACAO header, got %q", h.Get("Access-Control-Allow-Origin"))
	}
	if h.Get("Access-Control-Expose-Headers") != "x-custom-header" {
		t.Fatalf("want expose header, got %q", h.Get("Access-Control-Expose-Headers"))
	}
}

func TestCORS_PreflightHeaders_MaxAge(t *testing.T) {
	rule := map[string]any{
		"AllowedOrigins": []string{"*"},
		"AllowedMethods": []string{"GET", "POST"},
		"MaxAgeSeconds":  600,
	}
	w := httptest.NewRecorder()
	corsWritePreflightHeaders(w, rule, "https://example.com", "")
	if w.Header().Get("Access-Control-Max-Age") != "600" {
		t.Fatalf("want Max-Age=600, got %q", w.Header().Get("Access-Control-Max-Age"))
	}
	if w.Header().Get("Access-Control-Allow-Origin") != "https://example.com" {
		t.Fatalf("want ACAO header set")
	}
}

// ─── GCS bucket CORS ──────────────────────────────────────────────────────────

func gcsRule() []map[string]any {
	return []map[string]any{
		{
			"origin":         []any{"https://example.com"},
			"method":         []any{"GET", "POST"},
			"responseHeader": []any{"Content-Type", "x-goog-meta-foo"},
			"maxAgeSeconds":  float64(3600),
		},
	}
}

func TestGCSCORSExtractBucket(t *testing.T) {
	cases := map[string]string{
		"/storage/v1/b/mybucket/o/obj.txt":          "mybucket",
		"/storage/v1/b/mybucket":                    "mybucket",
		"/upload/storage/v1/b/mybucket/o":           "mybucket",
		"/download/storage/v1/b/mybucket/o/obj.txt": "mybucket",
		"/mybucket/obj.txt":                         "mybucket",
	}
	for path, want := range cases {
		req, _ := http.NewRequest(http.MethodOptions, path, nil)
		if got := gcsCORSExtractBucket(req); got != want {
			t.Errorf("path %q: want %q, got %q", path, want, got)
		}
	}
}

func TestGCSCORSMatchRule(t *testing.T) {
	if _, ok := gcsCORSMatchRule(gcsRule(), "https://example.com", "GET"); !ok {
		t.Fatal("want match for allowed origin+method")
	}
	if _, ok := gcsCORSMatchRule(gcsRule(), "https://evil.com", "GET"); ok {
		t.Fatal("want no match for unlisted origin")
	}
	if _, ok := gcsCORSMatchRule(gcsRule(), "https://example.com", "DELETE"); ok {
		t.Fatal("want no match for disallowed method")
	}
	wild := []map[string]any{{"origin": []any{"*"}, "method": []any{"*"}}}
	if _, ok := gcsCORSMatchRule(wild, "https://any.com", "PUT"); !ok {
		t.Fatal("wildcard origin+method must match")
	}
}

func TestGCSCORSPreflightHeaders(t *testing.T) {
	w := httptest.NewRecorder()
	gcsCORSPreflightHeaders(w, gcsRule()[0], "https://example.com", "X-Requested-With")
	h := w.Header()
	if h.Get("Access-Control-Allow-Origin") != "https://example.com" {
		t.Errorf("want ACAO, got %q", h.Get("Access-Control-Allow-Origin"))
	}
	if h.Get("Access-Control-Allow-Methods") != "GET, POST" {
		t.Errorf("want methods GET, POST, got %q", h.Get("Access-Control-Allow-Methods"))
	}
	if h.Get("Access-Control-Allow-Headers") != "X-Requested-With" {
		t.Errorf("want requested headers reflected, got %q", h.Get("Access-Control-Allow-Headers"))
	}
	if h.Get("Access-Control-Max-Age") != "3600" {
		t.Errorf("want Max-Age 3600, got %q", h.Get("Access-Control-Max-Age"))
	}
}

func TestGCSCORSResponseHeaders(t *testing.T) {
	h := http.Header{}
	gcsCORSAddResponseHeaders(h, gcsRule(), "https://example.com")
	if h.Get("Access-Control-Allow-Origin") != "https://example.com" {
		t.Errorf("want ACAO, got %q", h.Get("Access-Control-Allow-Origin"))
	}
	if h.Get("Access-Control-Expose-Headers") != "Content-Type, x-goog-meta-foo" {
		t.Errorf("want expose headers, got %q", h.Get("Access-Control-Expose-Headers"))
	}

	// No match → no CORS headers added.
	h2 := http.Header{}
	gcsCORSAddResponseHeaders(h2, gcsRule(), "https://evil.com")
	if h2.Get("Access-Control-Allow-Origin") != "" {
		t.Error("want no ACAO for unlisted origin")
	}
}
