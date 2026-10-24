package testdepth

import (
	"fmt"
	"sync"
)

// Race runs n writers of the same operation that start together and returns each
// writer's error, indexed by writer. Callers run it from a test binary built with
// -race (AssertRaceEnabled), so the concurrent read/write of the emulator's state
// is checked as well as the outcome.
//
// The writers are released by closing a gate rather than started in a loop, so a
// race on the emulator's OCC path is actually contended instead of serialized by
// goroutine startup.
func Race(n int, fn func(i int) error) []error {
	errs := make([]error, n)
	gate := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-gate
			errs[i] = fn(i)
		}(i)
	}
	close(gate)
	wg.Wait()
	return errs
}

// CheckOneWinner returns one message per contention-contract violation: exactly
// one writer may win, and every loser must carry the documented conflict. Pass a
// nil isConflict to assert only "exactly one winner" (for a service whose losing
// writers observe a non-error outcome, e.g. a clamped update).
func CheckOneWinner(errs []error, isConflict func(error) bool) []string {
	if len(errs) == 0 {
		return []string{"no writers were run"}
	}
	var out []string
	var winners []int
	var losers []int
	for i, err := range errs {
		if err == nil {
			winners = append(winners, i)
			continue
		}
		losers = append(losers, i)
		if isConflict != nil && !isConflict(err) {
			out = append(out, fmt.Sprintf("writer %d lost with %v, which is not the documented conflict", i, err))
		}
	}
	switch {
	case len(winners) == 0:
		out = append(out, fmt.Sprintf("no writer won: all %d returned an error (first: %v)", len(errs), errs[losers[0]]))
	case len(winners) > 1:
		out = append(out, fmt.Sprintf("%d writers won (%v); exactly one must win", len(winners), winners))
	}
	return out
}

// AssertOneWinner fails t unless exactly one writer won and every loser carried
// the documented conflict. It requires the race detector (AssertRaceEnabled): a
// contention assertion in a binary without -race proves nothing about the
// emulator's interleaving, so a forgotten `-race` is a failure, not a silent
// downgrade.
func AssertOneWinner(t TB, errs []error, isConflict func(error) bool) {
	t.Helper()
	AssertRaceEnabled(t)
	for _, violation := range CheckOneWinner(errs, isConflict) {
		t.Errorf("contention: %s", violation)
	}
}

// AssertRaceEnabled fails t when the test binary was not built with the race
// detector. A contention assertion that silently runs without -race is not a
// contention assertion, so this is a failure rather than a skip.
func AssertRaceEnabled(t TB) {
	t.Helper()
	if !RaceEnabled() {
		t.Fatal("contention assertions require the race detector: run this package with -race")
	}
}
