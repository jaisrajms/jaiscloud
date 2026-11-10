package scheduler

import (
	"context"
	"testing"
	"time"

	schedstore "jaiscloud/internal/gcp/store/scheduler"
)

// wantRetryDefaults is the RetryConfig the Discovery/proto field descriptions
// document for every unset field: retry_count 0, min_backoff_duration 5s,
// max_backoff_duration 1h, max_doublings 5, max_retry_duration 0 (unlimited).
func wantRetryDefaults() schedstore.RetryConfig {
	return schedstore.RetryConfig{
		RetryCount:         0,
		MinBackoffDuration: 5 * time.Second,
		MaxBackoffDuration: time.Hour,
		MaxDoublings:       5,
		MaxRetryDuration:   0,
	}
}

func assertRetryConfig(t *testing.T, label string, got *schedstore.RetryConfig, want schedstore.RetryConfig) {
	t.Helper()
	if got == nil {
		t.Fatalf("%s: retryConfig = nil, want %+v", label, want)
	}
	if *got != want {
		t.Fatalf("%s: retryConfig = %+v, want %+v", label, *got, want)
	}
}

// TestEffectiveRetryConfig pins the documented defaults for every shape a read
// can see. RetryConfig is proto3 without field presence, so an unset field and
// a zero one are indistinguishable: both resolve to the documented default.
func TestEffectiveRetryConfig(t *testing.T) {
	defaults := wantRetryDefaults()

	// A job created without a retryConfig (the provider dropped the all-zero
	// `{ retry_count = 0 }` block from the create body).
	assertRetryConfig(t, "no retryConfig", EffectiveRetryConfig(schedstore.Job{}), defaults)

	// An all-zero config, which is what the provider's dropped block leaves
	// behind if it reaches the store at all.
	assertRetryConfig(t, "all-zero retryConfig", EffectiveRetryConfig(schedstore.Job{RetryConfig: &schedstore.RetryConfig{}}), defaults)

	// A partially specified config: the unset fields take their documented
	// default, the set ones stay.
	assertRetryConfig(t, "partial retryConfig", EffectiveRetryConfig(schedstore.Job{
		RetryConfig: &schedstore.RetryConfig{RetryCount: 2, MinBackoffDuration: 30 * time.Second},
	}), schedstore.RetryConfig{
		RetryCount:         2,
		MinBackoffDuration: 30 * time.Second,
		MaxBackoffDuration: time.Hour,
		MaxDoublings:       5,
	})

	// An explicitly stored non-default config is echoed verbatim.
	stored := schedstore.RetryConfig{
		RetryCount:         3,
		MaxDoublings:       3,
		MinBackoffDuration: 2 * time.Second,
		MaxBackoffDuration: 20 * time.Second,
		MaxRetryDuration:   2 * time.Minute,
	}
	assertRetryConfig(t, "stored retryConfig", EffectiveRetryConfig(schedstore.Job{RetryConfig: &stored}), stored)

	// retry_count 0 is itself the documented default, so it survives alongside
	// a non-default backoff.
	assertRetryConfig(t, "retryCount 0 with a custom backoff", EffectiveRetryConfig(schedstore.Job{
		RetryConfig: &schedstore.RetryConfig{MinBackoffDuration: time.Second},
	}), schedstore.RetryConfig{
		RetryCount:         0,
		MinBackoffDuration: time.Second,
		MaxBackoffDuration: time.Hour,
		MaxDoublings:       5,
	})
}

// TestJobRetryConfigReadBack covers the create → read path: a job created with
// no retry config (or the provider's all-zero block) reads back the documented
// RetryConfig, and a job created with a config echoes it verbatim.
func TestJobRetryConfigReadBack(t *testing.T) {
	ctx := context.Background()
	core, _, _ := newCore(t)

	httpTarget := func() *schedstore.HttpTarget {
		return &schedstore.HttpTarget{URI: "http://example.test/hook", HTTPMethod: "POST"}
	}
	for _, tc := range []struct {
		name string
		cfg  *schedstore.RetryConfig
		want schedstore.RetryConfig
	}{
		{"no-retry", nil, wantRetryDefaults()},
		{"zero-retry", &schedstore.RetryConfig{}, wantRetryDefaults()},
		{"custom-retry", &schedstore.RetryConfig{
			RetryCount:         3,
			MaxDoublings:       3,
			MinBackoffDuration: 2 * time.Second,
			MaxBackoffDuration: 20 * time.Second,
		}, schedstore.RetryConfig{
			RetryCount:         3,
			MaxDoublings:       3,
			MinBackoffDuration: 2 * time.Second,
			MaxBackoffDuration: 20 * time.Second,
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			created, err := core.CreateJob(ctx, "p", "l", schedstore.Job{
				Name:        tc.name,
				Schedule:    "0 0 1 1 *",
				TimeZone:    "UTC",
				Target:      schedstore.TargetHTTP,
				HTTP:        httpTarget(),
				RetryConfig: tc.cfg,
			})
			if err != nil {
				t.Fatalf("CreateJob: %v", err)
			}
			got, err := core.GetJob(ctx, "p", "l", created.Name)
			if err != nil {
				t.Fatalf("GetJob: %v", err)
			}
			// The job is persisted with the message the API answers, and the read
			// helper renders the same values.
			assertRetryConfig(t, tc.name+" (stored)", got.RetryConfig, tc.want)
			assertRetryConfig(t, tc.name, EffectiveRetryConfig(got), tc.want)
		})
	}
}
