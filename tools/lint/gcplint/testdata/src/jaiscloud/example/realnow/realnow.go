package realnow

import (
	"context"
	"time"

	"jaiscloud/internal/clock"
)

// ---- bad: business clock driving a wall-clock wait ------------------------

func pollLoopBad(deadline time.Time) {
	for clock.Now().Before(deadline) { // want `\[clock-realnow\]`
		time.Sleep(time.Millisecond)
	}
}

func derivedDeadlineBad(d time.Duration) {
	deadline := clock.Now().Add(d) // want `\[clock-realnow\]`
	for {
		if !clock.Now().Before(deadline) { // want `\[clock-realnow\]`
			return
		}
		time.Sleep(time.Millisecond)
	}
}

func withDeadlineBad(ctx context.Context, d time.Duration) {
	_, cancel := context.WithDeadline(ctx, clock.Now().Add(d)) // want `\[clock-realnow\]`
	cancel()
}

func sleepElapsedBad(start time.Time) {
	time.Sleep(clock.Now().Sub(start)) // want `\[clock-realnow\]`
}

// ---- good: business clock for business time, RealNow for waits ------------

func businessTimestampGood() time.Time {
	return clock.Now()
}

func businessDeadlineGood(d time.Duration) time.Time {
	// A stored expiry is a business timestamp: no wall-clock wait.
	return clock.Now().Add(d)
}

func realPollLoopGood(deadline time.Time) {
	for clock.RealNow().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
}

func realDerivedDeadlineGood(d time.Duration) {
	deadline := clock.RealNow().Add(d)
	for {
		if !clock.RealNow().Before(deadline) {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

// A business-clock comparison that is not inside a loop is not a wait.
func businessCompareGood(expiry time.Time) bool {
	return clock.Now().Before(expiry)
}

type item struct{ Expiry time.Time }

// A business-clock comparison against a stored value inside a loop is not a
// wall-clock wait: nothing in the comparison is clock-derived.
func businessLoopGood(items []item) {
	for i := 0; i < len(items); i++ {
		if clock.Now().After(items[i].Expiry) {
			continue
		}
	}
}
