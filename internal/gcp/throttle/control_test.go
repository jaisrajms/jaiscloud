package throttle

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

func TestSetThrottleConfigArmsAndDisarms(t *testing.T) {
	inj := New(Config{})

	status, err := inj.SetThrottleConfig([]byte(`{"mode":"fault","failFirst":1,"services":["storage"],"status":429,"retryDelay":"2s"}`))
	if err != nil {
		t.Fatalf("arm: %v", err)
	}
	var st ControlStatus
	if err := json.Unmarshal(status, &st); err != nil {
		t.Fatalf("status: %v", err)
	}
	if !st.Enabled || st.Mode != "fault" || st.FailFirst != 1 || st.Status != http.StatusTooManyRequests || st.RetryDelay != "2s" {
		t.Fatalf("status after arm: %+v", st)
	}
	if inj.Check("proj", "storage", "ObjectsGet") == nil {
		t.Fatal("armed injector must refuse the first matching request")
	}
	if inj.Check("proj", "pubsub", "Publish") != nil {
		t.Fatal("service allow-list must still apply")
	}

	// Applying a control document fully replaces the configuration and clears
	// the fault counter, so re-arming the same policy fails again from one.
	if _, err := inj.SetThrottleConfig([]byte(`{"mode":"fault","failFirst":1}`)); err != nil {
		t.Fatalf("re-arm: %v", err)
	}
	if inj.Check("proj", "storage", "ObjectsGet") == nil {
		t.Fatal("re-arm must reset the counter")
	}

	// Disarm.
	status, err = inj.SetThrottleConfig([]byte(`{"mode":"off"}`))
	if err != nil {
		t.Fatalf("disarm: %v", err)
	}
	if err := json.Unmarshal(status, &st); err != nil {
		t.Fatalf("status: %v", err)
	}
	if st.Enabled || st.Mode != "off" {
		t.Fatalf("status after disarm: %+v", st)
	}
	if inj.Check("proj", "storage", "ObjectsGet") != nil {
		t.Fatal("disarmed injector must allow")
	}
}

func TestSetThrottleConfigDefaults(t *testing.T) {
	inj := New(Config{})
	if _, err := inj.SetThrottleConfig([]byte(`{"mode":"rate","rps":2.5}`)); err != nil {
		t.Fatalf("arm rate: %v", err)
	}
	cfg := inj.Config()
	if cfg.RPS != 2.5 {
		t.Fatalf("rps: %v", cfg.RPS)
	}
	if cfg.Burst != 3 { // ceil(2.5)
		t.Fatalf("burst default: %d", cfg.Burst)
	}
	if cfg.Status != http.StatusTooManyRequests || cfg.RetryDelay != time.Second {
		t.Fatalf("status/retry defaults: %+v", cfg)
	}
}

func TestSetThrottleConfigRejectsInvalid(t *testing.T) {
	cases := map[string]string{
		"unknown mode":          `{"mode":"turbo"}`,
		"bad status":            `{"mode":"fault","failFirst":1,"status":500}`,
		"bad retryDelay":        `{"mode":"fault","failFirst":1,"retryDelay":"soon"}`,
		"negative rps":          `{"mode":"rate","rps":-1}`,
		"negative burst":        `{"mode":"rate","burst":-1}`,
		"negative failFirst":    `{"mode":"fault","failFirst":-3}`,
		"fault without counter": `{"mode":"fault"}`,
		"both without counter":  `{"mode":"both"}`,
		"unknown field":         `{"mode":"fault","failFirst":1,"wat":true}`,
		"malformed json":        `{"mode":`,
		"trailing value":        `{"mode":"off"}{"mode":"fault"}`,
		"trailing junk":         `{"mode":"off"} junk`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			inj := New(Config{})
			if _, err := inj.SetThrottleConfig([]byte(body)); err == nil {
				t.Fatalf("expected rejection for %s", body)
			}
			if inj.Enabled() {
				t.Fatal("a rejected request must not arm the injector")
			}
		})
	}
}

// TestSetThrottleConfigRejectionPreservesState proves a rejected request is
// atomic: it must not disturb an already-armed policy or its counters.
func TestSetThrottleConfigRejectionPreservesState(t *testing.T) {
	inj := New(Config{})
	if _, err := inj.SetThrottleConfig([]byte(`{"mode":"fault","failFirst":1}`)); err != nil {
		t.Fatalf("arm: %v", err)
	}
	if inj.Check("proj", "storage", "ObjectsGet") == nil {
		t.Fatal("armed injector must fail the first request")
	}
	if _, err := inj.SetThrottleConfig([]byte(`{"mode":"fault"}`)); err == nil {
		t.Fatal("fault mode without a counter must be rejected")
	}
	// The spent fail-first counter must not have been reset by the rejection.
	if inj.Check("proj", "storage", "ObjectsGet") != nil {
		t.Fatal("a rejected request must leave the armed policy and counters untouched")
	}
}

func TestThrottleStatusReportsDefaults(t *testing.T) {
	inj := New(Config{})
	var st ControlStatus
	if err := json.Unmarshal(inj.ThrottleStatus(), &st); err != nil {
		t.Fatalf("status: %v", err)
	}
	if st.Enabled || st.Mode != "off" {
		t.Fatalf("default status: %+v", st)
	}
}

func TestRuntimeArmAffectsGRPCPath(t *testing.T) {
	inj := New(Config{})
	if inj.CheckMethod("/google.storage.v2.Storage/GetObject") != nil {
		t.Fatal("disabled injector must allow gRPC")
	}
	if _, err := inj.SetThrottleConfig([]byte(`{"mode":"fault","failFirst":1}`)); err != nil {
		t.Fatalf("arm: %v", err)
	}
	if inj.CheckMethod("/google.storage.v2.Storage/GetObject") == nil {
		t.Fatal("runtime arm must throttle the gRPC path")
	}
}
