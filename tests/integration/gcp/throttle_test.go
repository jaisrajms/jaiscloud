package gcp_test

import (
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// This file is the live end-to-end gate for the opt-in throttle/quota injector.
// It is skipped unless JAISCLOUD_THROTTLE=1, so the default
// `make test-integration-gcp` run stays green. `make test-throttle-gcp` starts
// jaiscloud-gcp with JAISCLOUD_GCP_THROTTLE=fault (first matching request
// fails, scoped to storage), sets the variable, then runs TestThrottle.

// TestThrottleFaultInjection drives the real emulator over HTTP with a
// backoff-and-retry client: the first storage request is refused with the
// injected 429 envelope (Retry-After + google.rpc.RetryInfo), and the retry —
// honouring the advertised delay — reaches the service.
func TestThrottleFaultInjection(t *testing.T) {
	if os.Getenv("JAISCLOUD_THROTTLE") != "1" {
		t.Skip("set JAISCLOUD_THROTTLE=1 and start jaiscloud-gcp with JAISCLOUD_GCP_THROTTLE=fault")
	}
	resetState(t)

	// A GET for a bucket that does not exist. The injector fails the first
	// matching request, then lets it through to the real 404.
	const path = "/storage/v1/b/throttle-no-such-bucket"

	resp, body := do(t, "GET", path, nil, nil)
	require.Equal(t, http.StatusTooManyRequests, resp.StatusCode,
		"first request must be refused by the injector: %s", body)
	require.Equal(t, "1", resp.Header.Get("Retry-After"), "Retry-After header: %s", body)

	env := jsonMap(t, body)
	errObj, ok := env["error"].(map[string]any)
	require.True(t, ok, "error envelope: %s", body)
	require.EqualValues(t, http.StatusTooManyRequests, errObj["code"], "envelope code: %s", body)

	details, ok := errObj["details"].([]any)
	require.True(t, ok && len(details) == 1, "RetryInfo detail missing: %s", body)
	detail, _ := details[0].(map[string]any)
	require.Equal(t, "type.googleapis.com/google.rpc.RetryInfo", detail["@type"], "detail: %s", body)
	require.Equal(t, "1s", detail["retryDelay"], "detail: %s", body)

	// Back off by the advertised delay, then retry. The injected failure is
	// transient (fail-first), so the retry must reach the service.
	time.Sleep(time.Second)
	resp2, body2 := do(t, "GET", path, nil, nil)
	require.Equal(t, http.StatusNotFound, resp2.StatusCode,
		"retry must reach the service and return NOT_FOUND: %s", body2)
}

// TestThrottleRuntimeControl drives POST/GET /_jaiscloud/throttle to disarm then
// arm the injector mid-run (no emulator restart), proving the runtime control
// surface reaches the same injector the request path consults.
func TestThrottleRuntimeControl(t *testing.T) {
	if os.Getenv("JAISCLOUD_THROTTLE") != "1" {
		t.Skip("set JAISCLOUD_THROTTLE=1 and start jaiscloud-gcp")
	}
	resetState(t)

	const path = "/storage/v1/b/throttle-runtime-no-such-bucket"

	// Disarm: an unarmed injector lets the request reach the service (404).
	r0, _ := postThrottle(t, `{"mode":"off"}`)
	require.Equal(t, http.StatusOK, r0.StatusCode)
	resp, body := do(t, "GET", path, nil, nil)
	require.Equal(t, http.StatusNotFound, resp.StatusCode,
		"disarmed injector must let the request through: %s", body)

	// Arm fault mode at runtime; the next matching request is refused.
	resp, body = postThrottle(t, `{"mode":"fault","failFirst":1,"services":["storage"],"status":429,"retryDelay":"1s"}`)
	require.Equal(t, http.StatusOK, resp.StatusCode, "arm: %s", body)
	st := jsonMap(t, body)
	require.EqualValues(t, true, st["enabled"], "status after arm: %s", body)
	require.Equal(t, "fault", st["mode"], "status after arm: %s", body)

	resp, body = do(t, "GET", path, nil, nil)
	require.Equal(t, http.StatusTooManyRequests, resp.StatusCode,
		"runtime-armed injector must refuse the first request: %s", body)

	// The fault is spent (fail-first), so the next request reaches the service.
	resp, body = do(t, "GET", path, nil, nil)
	require.Equal(t, http.StatusNotFound, resp.StatusCode,
		"the fault is transient: %s", body)

	// GET reports the current state.
	resp, body = do(t, "GET", "/_jaiscloud/throttle", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.EqualValues(t, true, jsonMap(t, body)["enabled"], "GET status: %s", body)

	// A malformed control document is rejected and changes nothing.
	resp, body = postThrottle(t, `{"mode":"turbo"}`)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, "bad mode must 400: %s", body)
}

func postThrottle(t *testing.T, body string) (*http.Response, []byte) {
	t.Helper()
	return do(t, "POST", "/_jaiscloud/throttle", []byte(body), map[string]string{"Content-Type": "application/json"})
}
