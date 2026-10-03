package functions

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"jaiscloud/internal/model"
)

func TestTriggerCodecDecode(t *testing.T) {
	c := NewTriggerCodec()
	r := httptest.NewRequest(http.MethodPost, "/my-func?x=1", strings.NewReader("payload"))
	r.Host = "europe-west1-my-project.cloudfunctions.net"

	nr, err := c.Decode(r, []byte("payload"))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if nr.Service != "functions" || nr.Action != "InvokeTrigger" {
		t.Errorf("service/action = %q/%q, want functions/InvokeTrigger", nr.Service, nr.Action)
	}
	if nr.Params["project"] != "my-project" || nr.Params["location"] != "europe-west1" || nr.Params["functionId"] != "my-func" {
		t.Errorf("params = %v", nr.Params)
	}
	if nr.Params["payload"] != "payload" {
		t.Errorf("payload = %v, want payload", nr.Params["payload"])
	}
	if nr.Params["triggerLabel"] != "europe-west1-my-project" {
		t.Errorf("triggerLabel = %v", nr.Params["triggerLabel"])
	}
	if nr.Raw != r {
		t.Errorf("Raw request not carried through")
	}
}

// TestTriggerCodecDecodeHostVariants covers host forms the codec must accept or
// defer: a sub-path still addresses the function; an upper-case host is
// normalised; a region outside the advertised catalog leaves location/project
// unset for the provider to split from the trigger label.
func TestTriggerCodecDecodeHostVariants(t *testing.T) {
	c := NewTriggerCodec()

	sub, err := c.Decode(hostReq(http.MethodPost, "/my-func/a/b", "us-central1-proj.cloudfunctions.net"), nil)
	if err != nil || sub.Params["functionId"] != "my-func" {
		t.Fatalf("sub-path decode = (%v, %v), want functionId my-func", sub, err)
	}

	upper, err := c.Decode(hostReq(http.MethodGet, "/fn", "US-CENTRAL1-PROJ.CLOUDFUNCTIONS.NET:443"), nil)
	if err != nil || upper.Params["location"] != "us-central1" || upper.Params["project"] != "proj" {
		t.Fatalf("upper-case decode = (%v, %v), want us-central1/proj", upper, err)
	}

	unlisted, err := c.Decode(hostReq(http.MethodGet, "/fn", "me-west1-my-proj.cloudfunctions.net"), nil)
	if err != nil {
		t.Fatalf("unlisted region decode: %v", err)
	}
	if _, ok := unlisted.Params["location"]; ok {
		t.Errorf("unlisted region must not set location, got %v", unlisted.Params["location"])
	}
	if unlisted.Params["triggerLabel"] != "me-west1-my-proj" {
		t.Errorf("triggerLabel = %v, want me-west1-my-proj", unlisted.Params["triggerLabel"])
	}
}

func TestTriggerCodecDecodeNotFound(t *testing.T) {
	c := NewTriggerCodec()
	cases := []struct {
		name string
		host string
		path string
	}{
		{"control-plane host", "cloudfunctions.googleapis.com", "/fn"},
		{"empty path", "us-central1-proj.cloudfunctions.net", "/"},
		{"custom verb", "us-central1-proj.cloudfunctions.net", "/fn:call"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, tc.path, nil)
			r.Host = tc.host
			if _, err := c.Decode(r, nil); err == nil {
				t.Fatalf("Decode(%s %s) = nil error, want NotFound", tc.host, tc.path)
			}
		})
	}
}

func TestTriggerCodecEncode(t *testing.T) {
	c := NewTriggerCodec()

	status, headers, body := c.Encode(nil, &model.ProviderResponse{
		HTTPStatus: http.StatusOK,
		Data:       map[string]any{"body": []byte("hello")},
	})
	if status != http.StatusOK || string(body) != "hello" {
		t.Errorf("encode = (%d, %q), want (200, hello)", status, body)
	}
	if ct := headers.Get("Content-Type"); ct != "text/plain; charset=utf-8" {
		t.Errorf("content-type = %q", ct)
	}

	// An executor failure surfaces as HTTP 500 with the error text.
	status, _, body = c.Encode(nil, &model.ProviderResponse{
		HTTPStatus: http.StatusInternalServerError,
		Data:       map[string]any{"body": []byte("boom")},
	})
	if status != http.StatusInternalServerError || string(body) != "boom" {
		t.Errorf("error encode = (%d, %q), want (500, boom)", status, body)
	}
}

func TestTriggerCodecEncodeError(t *testing.T) {
	c := NewTriggerCodec()
	status, headers, body := c.EncodeError(nil, model.NewProviderError("NotFound", "function not found", 404))
	if status != http.StatusNotFound {
		t.Errorf("status = %d, want 404", status)
	}
	if ct := headers.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("content-type = %q, want application/json", ct)
	}
	if !strings.Contains(string(body), "function not found") || !strings.Contains(string(body), "NOT_FOUND") {
		t.Errorf("error envelope = %s", body)
	}
}

// hostReq builds a request with an explicit Host header.
func hostReq(method, path, host string) *http.Request {
	r := httptest.NewRequest(method, path, nil)
	r.Host = host
	return r
}
