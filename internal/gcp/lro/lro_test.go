package lro

import (
	"testing"
	"time"

	"jaiscloud/internal/clock"
)

// freezeClock pins the global clock to at and restores real time on cleanup.
func freezeClock(t *testing.T, at time.Time) {
	t.Helper()
	clock.SetGlobalClock(clock.FixedClock{T: at})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })
}

func TestAsync(t *testing.T) {
	if (Mode{}).Async() {
		t.Errorf("zero Mode must be synchronous")
	}
	if !(Mode{Enabled: true}).Async() {
		t.Errorf("Enabled Mode must be async")
	}
}

// TestPendingSync verifies the synchronous default never reports an operation
// in flight, even for a delay that would otherwise keep it pending.
func TestPendingSync(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	if (Mode{Delay: time.Hour}).Pending(t0) {
		t.Errorf("sync mode must never be pending")
	}
}

// TestPendingZeroDelay settles on the first read: the operation is not pending
// at its own create instant.
func TestPendingZeroDelay(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	m := Mode{Enabled: true, Delay: 0}
	if m.Pending(t0) {
		t.Errorf("zero-delay async mode must settle immediately")
	}
	if m.Pending(t0.Add(-time.Second)) {
		t.Errorf("create in the future settles immediately")
	}
}

// TestPendingPositiveDelay verifies an operation stays pending until the delay
// elapses on the (frozen) clock.
func TestPendingPositiveDelay(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	m := Mode{Enabled: true, Delay: 30 * time.Second}

	if !m.Pending(t0) {
		t.Fatalf("operation at +0 must be pending")
	}
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(29 * time.Second)})
	if !m.Pending(t0) {
		t.Fatalf("operation at +29s must still be pending")
	}
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(30 * time.Second)})
	if m.Pending(t0) {
		t.Fatalf("operation at +30s must be settled (Before is exclusive)")
	}
}

func TestFromEnvDefaultSync(t *testing.T) {
	t.Setenv(envMode, "")
	t.Setenv(envDelay, "")
	m := FromEnv()
	if m.Async() || m.Delay != 0 {
		t.Fatalf("unset mode = %+v, want synchronous zero value", m)
	}
}

func TestFromEnvExplicitSync(t *testing.T) {
	t.Setenv(envMode, "sync")
	t.Setenv(envDelay, "5s")
	m := FromEnv()
	if m.Async() || m.Delay != 0 {
		t.Fatalf("sync mode = %+v, want synchronous zero value", m)
	}
}

func TestFromEnvUnknownModeWarnsToSync(t *testing.T) {
	t.Setenv(envMode, "sometimes")
	m := FromEnv()
	if m.Async() {
		t.Fatalf("unknown mode = %+v, want synchronous", m)
	}
}

func TestFromEnvAsyncDefaultDelay(t *testing.T) {
	t.Setenv(envMode, "async")
	t.Setenv(envDelay, "")
	m := FromEnv()
	if !m.Async() {
		t.Fatalf("async mode not enabled: %+v", m)
	}
	if m.Delay != defaultDelay {
		t.Fatalf("default delay = %v, want %v", m.Delay, defaultDelay)
	}
}

func TestFromEnvAsyncExplicitDelay(t *testing.T) {
	t.Setenv(envMode, "async")
	t.Setenv(envDelay, "2s")
	m := FromEnv()
	if !m.Async() || m.Delay != 2*time.Second {
		t.Fatalf("async mode = %+v, want enabled with 2s delay", m)
	}
}

func TestFromEnvAsyncBadDelayFallsBack(t *testing.T) {
	t.Setenv(envMode, "async")
	t.Setenv(envDelay, "not-a-duration")
	m := FromEnv()
	if !m.Async() {
		t.Fatalf("async mode not enabled: %+v", m)
	}
	if m.Delay != defaultDelay {
		t.Fatalf("bad delay fallback = %v, want %v", m.Delay, defaultDelay)
	}
}
