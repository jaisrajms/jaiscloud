package throttle

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/model"
)

func TestFromEnvOffByDefault(t *testing.T) {
	t.Setenv(envMode, "")
	cfg := FromEnv()
	if cfg.Enabled() {
		t.Fatalf("unset mode must be disabled: %+v", cfg)
	}
}

func TestFromEnvModes(t *testing.T) {
	cases := []struct {
		mode  string
		rate  bool
		fault bool
	}{
		{"off", false, false},
		{"rate", true, false},
		{"fault", false, true},
		{"both", true, true},
		{"nonsense", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.mode, func(t *testing.T) {
			t.Setenv(envMode, tc.mode)
			cfg := FromEnv()
			if cfg.Rate != tc.rate || cfg.Fault != tc.fault {
				t.Fatalf("mode %q: got rate=%v fault=%v, want rate=%v fault=%v", tc.mode, cfg.Rate, cfg.Fault, tc.rate, tc.fault)
			}
		})
	}
}

func TestFromEnvTuning(t *testing.T) {
	t.Setenv(envMode, "both")
	t.Setenv(envRPS, "2.5")
	t.Setenv(envBurst, "4")
	t.Setenv(envServices, "storage, pubsub")
	t.Setenv(envFailFirst, "3")
	t.Setenv(envFailEvery, "5")
	t.Setenv(envFailCount, "9")
	t.Setenv(envStatus, "503")
	t.Setenv(envRetryDelay, "250ms")

	cfg := FromEnv()
	if cfg.RPS != 2.5 || cfg.Burst != 4 {
		t.Fatalf("rps/burst: %+v", cfg)
	}
	if len(cfg.Services) != 2 || cfg.Services[0] != "storage" || cfg.Services[1] != "pubsub" {
		t.Fatalf("services: %+v", cfg.Services)
	}
	if cfg.FailFirst != 3 || cfg.FailEvery != 5 || cfg.FailCount != 9 {
		t.Fatalf("fault tuning: %+v", cfg)
	}
	if cfg.Status != http.StatusServiceUnavailable || cfg.RetryDelay != 250*time.Millisecond {
		t.Fatalf("status/delay: %+v", cfg)
	}
}

func TestFaultFirstThenEvery(t *testing.T) {
	inj := New(Config{Fault: true, FailFirst: 1, FailEvery: 2, Status: http.StatusTooManyRequests})
	// Fails on request 1, then every 2nd after it: 1,3,5...
	want := []bool{true, false, true, false, true, false}
	for n, fail := range want {
		got := inj.Check("proj", "storage", "ObjectsGet") != nil
		if got != fail {
			t.Fatalf("request %d: got fail=%v want %v", n+1, got, fail)
		}
	}
}

func TestFaultCountConsecutiveThenRecover(t *testing.T) {
	inj := New(Config{Fault: true, FailCount: 2, Status: http.StatusTooManyRequests})
	want := []bool{true, true, false, false}
	for n, fail := range want {
		got := inj.Check("proj", "pubsub", "Publish") != nil
		if got != fail {
			t.Fatalf("request %d: got fail=%v want %v", n+1, got, fail)
		}
	}
}

func TestShouldFailAllThree(t *testing.T) {
	// FailCount is ignored once FailFirst/FailEvery are set.
	c := Config{FailFirst: 2, FailEvery: 3, FailCount: 100}
	want := []bool{true, true, false, false, true, false, false, true}
	for n, w := range want {
		if got := c.shouldFail(n + 1); got != w {
			t.Fatalf("n=%d: got %v want %v", n+1, got, w)
		}
	}
}

func TestFromEnvRPSClamp(t *testing.T) {
	t.Setenv(envMode, "rate")
	t.Setenv(envRPS, "0")
	if cfg := FromEnv(); cfg.RPS != defaultRPS {
		t.Fatalf("non-positive RPS must be clamped to the default, got %v", cfg.RPS)
	}
}

func TestAllowListCanScopeToMethod(t *testing.T) {
	c := Config{Services: []string{"storage/objectsget"}}
	if !c.allows("storage", "ObjectsGet") {
		t.Fatal("method-scoped token must match its action")
	}
	if c.allows("storage", "ObjectsList") {
		t.Fatal("method-scoped token must not match a different action")
	}
}

func TestInjectedErrorShape(t *testing.T) {
	inj := New(Config{Fault: true, FailFirst: 1, Status: http.StatusServiceUnavailable, RetryDelay: 1500 * time.Millisecond})
	pe := inj.Check("proj", "storage", "ObjectsGet")
	if pe == nil {
		t.Fatal("expected a failure")
	}
	if pe.Code != "Unavailable" || pe.HTTPStatus != http.StatusServiceUnavailable {
		t.Fatalf("code/status: %+v", pe)
	}
	if pe.RetryAfter != 1500*time.Millisecond {
		t.Fatalf("retryAfter: %v", pe.RetryAfter)
	}
}

func TestServicesAllowList(t *testing.T) {
	inj := New(Config{Fault: true, FailCount: 100, Services: []string{"storage"}})
	if inj.Check("proj", "pubsub", "Publish") != nil {
		t.Fatal("pubsub must not be throttled when only storage is listed")
	}
	if inj.Check("proj", "storage", "ObjectsGet") == nil {
		t.Fatal("storage must be throttled")
	}
}

func TestRateBurst(t *testing.T) {
	inj := New(Config{Rate: true, RPS: 0.0001, Burst: 1})
	if inj.Check("proj", "storage", "ObjectsGet") != nil {
		t.Fatal("first request must pass")
	}
	if inj.Check("proj", "storage", "ObjectsGet") == nil {
		t.Fatal("second request must be refused once the burst is spent")
	}
}

// TestRateRefillsDespiteFrozenClock verifies the bucket refills on the real
// clock, not the business clock: with the global clock frozen, requests must
// still eventually pass instead of being throttled forever.
func TestRateRefillsDespiteFrozenClock(t *testing.T) {
	clock.SetGlobalClock(clock.FixedClock{T: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })

	inj := New(Config{Rate: true, RPS: 1000, Burst: 1})
	if inj.Check("proj", "storage", "ObjectsGet") != nil {
		t.Fatal("first request must pass")
	}
	if inj.Check("proj", "storage", "ObjectsGet") == nil {
		t.Fatal("second request must be refused")
	}
	time.Sleep(20 * time.Millisecond)
	if inj.Check("proj", "storage", "ObjectsGet") != nil {
		t.Fatal("bucket must refill on the real clock even while the business clock is frozen")
	}
}

func TestResetClearsRateBuckets(t *testing.T) {
	inj := New(Config{Rate: true, RPS: 0.0001, Burst: 1})
	if inj.Check("proj", "storage", "ObjectsGet") != nil {
		t.Fatal("first request must pass")
	}
	if inj.Check("proj", "storage", "ObjectsGet") == nil {
		t.Fatal("second request must be refused")
	}
	inj.Reset(context.Background())
	if inj.Check("proj", "storage", "ObjectsGet") != nil {
		t.Fatal("reset must restore the bucket")
	}
}

func TestCheckMethodSkipsHealth(t *testing.T) {
	inj := New(Config{Fault: true, FailCount: 100})
	if inj.CheckMethod("/grpc.health.v1.Health/Check") != nil {
		t.Fatal("health must never be throttled")
	}
	if inj.CheckMethod("/google.storage.v2.Storage/GetObject") == nil {
		t.Fatal("storage must be throttled")
	}
}

func TestCheckMethodAllowList(t *testing.T) {
	inj := New(Config{Fault: true, FailCount: 100, Services: []string{"storage"}})
	if inj.CheckMethod("/google.pubsub.v1.Publisher/Publish") != nil {
		t.Fatal("pubsub must not match a storage-only allow-list")
	}
	if inj.CheckMethod("/google.storage.v2.Storage/GetObject") == nil {
		t.Fatal("storage must match")
	}
}

func TestDisabledInjectorAllowsEverything(t *testing.T) {
	inj := New(Config{})
	if inj.Check("proj", "storage", "ObjectsGet") != nil {
		t.Fatal("disabled injector must allow")
	}
	if !(Config{}).allows("anything", "Anything") {
		t.Fatal("zero Config allows all")
	}
}

func TestDecorateError(t *testing.T) {
	body := []byte(`{"error":{"code":429,"message":"quota","status":"RESOURCE_EXHAUSTED"}}`)
	pe := &model.ProviderError{
		Code:       "ResourceExhausted",
		HTTPStatus: http.StatusTooManyRequests,
		RetryAfter: 1500 * time.Millisecond,
	}

	status, headers, out := DecorateError(pe, http.StatusTooManyRequests, http.Header{}, body)
	if status != http.StatusTooManyRequests {
		t.Fatalf("status: %d", status)
	}
	if got := headers.Get("Retry-After"); got != "2" {
		t.Fatalf("Retry-After: %q", got)
	}
	var env map[string]any
	if err := json.Unmarshal(out, &env); err != nil {
		t.Fatalf("body: %v", err)
	}
	errObj := env["error"].(map[string]any)
	details, ok := errObj["details"].([]any)
	if !ok || len(details) != 1 {
		t.Fatalf("details: %#v", errObj["details"])
	}
	d := details[0].(map[string]any)
	if d["@type"] != retryInfoType {
		t.Fatalf("@type: %v", d["@type"])
	}
	if d["retryDelay"] != "1.500s" {
		t.Fatalf("retryDelay: %v", d["retryDelay"])
	}
}

func TestDecorateErrorNoOps(t *testing.T) {
	// nil error, zero delay, and a non-JSON body all pass through unchanged.
	if _, h, b := DecorateError(nil, 200, nil, []byte("x")); string(b) != "x" || h != nil {
		t.Fatal("nil error must be a no-op")
	}
	pe := &model.ProviderError{Code: "ResourceExhausted", HTTPStatus: http.StatusTooManyRequests}
	if _, _, b := DecorateError(pe, 429, http.Header{}, []byte("plain")); string(b) != "plain" {
		t.Fatal("zero delay must be a no-op")
	}
	pe.RetryAfter = time.Second
	_, h, b := DecorateError(pe, 429, http.Header{}, []byte("not-json"))
	if string(b) != "not-json" {
		t.Fatalf("non-JSON body must be preserved: %q", b)
	}
	if h.Get("Retry-After") != "1" {
		t.Fatalf("Retry-After must still be set: %q", h.Get("Retry-After"))
	}
}

func TestFormatProtoDuration(t *testing.T) {
	cases := map[time.Duration]string{
		time.Second:             "1s",
		1500 * time.Millisecond: "1.500s",
		250 * time.Millisecond:  "0.250s",
		2 * time.Second:         "2s",
		1500 * time.Microsecond: "0.001500s",
	}
	for d, want := range cases {
		if got := formatProtoDuration(d); got != want {
			t.Fatalf("formatProtoDuration(%v): got %q want %q", d, got, want)
		}
	}
}
