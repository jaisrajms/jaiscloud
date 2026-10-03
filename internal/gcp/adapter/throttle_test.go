package gcp

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"jaiscloud/internal/gateway"
	"jaiscloud/internal/gcp/throttle"
	"jaiscloud/internal/model"
)

// Compile-time proof the adapter exposes the optional gateway capability.
var _ gateway.RequestFilter = (*GCPAdapter)(nil)

func TestAdapterFilterRequestDisabledAllows(t *testing.T) {
	a := NewAdapter("proj")
	nr := &model.NormalizedRequest{Service: "storage", Action: "ObjectsGet", AccountID: "proj"}
	if pe := a.FilterRequest(nr); pe != nil {
		t.Fatalf("adapter without throttle must allow: %+v", pe)
	}
}

func TestAdapterFilterRequestInjects(t *testing.T) {
	a := NewAdapter("proj")
	a.SetThrottle(throttle.New(throttle.Config{
		Fault: true, FailFirst: 1, Status: 429, RetryDelay: time.Second,
	}))
	nr := &model.NormalizedRequest{Service: "storage", Action: "ObjectsGet", AccountID: "proj"}

	pe := a.FilterRequest(nr)
	if pe == nil {
		t.Fatal("expected an injected failure")
	}
	if pe.HTTPStatus != http.StatusTooManyRequests || pe.RetryAfter != time.Second {
		t.Fatalf("injected error: %+v", pe)
	}
	// Scoped per project+service, so the first match fails and the next passes.
	if pe := a.FilterRequest(nr); pe != nil {
		t.Fatalf("second request must pass: %+v", pe)
	}
}

// TestAdapterInjectedEnvelopes runs the real service codecs over an injected
// error and asserts the envelope each service uses: the standard Google JSON
// envelope carries "status"; the GCS envelope omits it and uses errors[].reason.
func TestAdapterInjectedEnvelopes(t *testing.T) {
	nr := &model.NormalizedRequest{Service: "storage", Action: "ObjectsGet", Params: map[string]any{}}

	t.Run("gcs-429", func(t *testing.T) {
		inj := throttle.New(throttle.Config{Fault: true, FailFirst: 1, Status: http.StatusTooManyRequests, RetryDelay: time.Second})
		pe := inj.Check("proj", "storage", "ObjectsGet")
		status, headers, body := (&GCSCodec{}).EncodeError(nr, pe)
		status, headers, body = throttle.DecorateError(pe, status, headers, body)
		if status != http.StatusTooManyRequests {
			t.Fatalf("status: %d", status)
		}
		if headers.Get("Retry-After") != "1" {
			t.Fatalf("Retry-After: %q", headers.Get("Retry-After"))
		}
		env := decodeEnvelope(t, body)
		errObj := env["error"].(map[string]any)
		if _, hasStatus := errObj["status"]; hasStatus {
			t.Fatalf("GCS envelope must omit status: %s", body)
		}
		errorsArr := errObj["errors"].([]any)
		if reason := errorsArr[0].(map[string]any)["reason"]; reason != "rateLimitExceeded" {
			t.Fatalf("GCS reason = %v, want rateLimitExceeded", reason)
		}
		assertRetryDetail(t, errObj)
	})

	t.Run("gcs-503", func(t *testing.T) {
		inj := throttle.New(throttle.Config{Fault: true, FailFirst: 1, Status: http.StatusServiceUnavailable, RetryDelay: time.Second})
		pe := inj.Check("proj", "storage", "ObjectsGet")
		status, headers, body := (&GCSCodec{}).EncodeError(nr, pe)
		_, _, body = throttle.DecorateError(pe, status, headers, body)
		errObj := decodeEnvelope(t, body)["error"].(map[string]any)
		errorsArr := errObj["errors"].([]any)
		if reason := errorsArr[0].(map[string]any)["reason"]; reason != "backendError" {
			t.Fatalf("GCS reason = %v, want backendError", reason)
		}
	})

	t.Run("standard-json", func(t *testing.T) {
		inj := throttle.New(throttle.Config{Fault: true, FailFirst: 1, Status: http.StatusTooManyRequests, RetryDelay: time.Second})
		pe := inj.Check("proj", "pubsub", "Publish")
		status, headers, body := (&JSONCodec{Service: "pubsub"}).EncodeError(nr, pe)
		_, _, body = throttle.DecorateError(pe, status, headers, body)
		errObj := decodeEnvelope(t, body)["error"].(map[string]any)
		if errObj["status"] != "RESOURCE_EXHAUSTED" {
			t.Fatalf("standard envelope status = %v, want RESOURCE_EXHAUSTED", errObj["status"])
		}
		assertRetryDetail(t, errObj)
	})
}

func decodeEnvelope(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var env map[string]any
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("error envelope is not JSON: %v (%s)", err, body)
	}
	return env
}

func assertRetryDetail(t *testing.T, errObj map[string]any) {
	t.Helper()
	details, ok := errObj["details"].([]any)
	if !ok || len(details) != 1 {
		t.Fatalf("RetryInfo detail missing: %#v", errObj["details"])
	}
	detail := details[0].(map[string]any)
	if detail["@type"] != "type.googleapis.com/google.rpc.RetryInfo" || detail["retryDelay"] != "1s" {
		t.Fatalf("bad RetryInfo detail: %#v", detail)
	}
}

func TestAdapterDecorateError(t *testing.T) {
	a := NewAdapter("")
	body := []byte(`{"error":{"code":429,"message":"quota","status":"RESOURCE_EXHAUSTED"}}`)
	pe := &model.ProviderError{Code: "ResourceExhausted", HTTPStatus: http.StatusTooManyRequests, RetryAfter: time.Second}

	status, headers, out := a.DecorateError(nil, pe, http.StatusTooManyRequests, http.Header{}, body)
	if status != http.StatusTooManyRequests {
		t.Fatalf("status: %d", status)
	}
	if got := headers.Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After: %q", got)
	}
	if !strings.Contains(string(out), "google.rpc.RetryInfo") || !strings.Contains(string(out), `"retryDelay":"1s"`) {
		t.Fatalf("decorated body: %s", out)
	}
}
