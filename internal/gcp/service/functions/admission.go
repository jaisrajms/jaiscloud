package functions

import (
	"errors"
	"sync"
	"sync/atomic"

	"jaiscloud/internal/gcp/gcperr"
	"jaiscloud/internal/model"
)

// This file is the Cloud Functions invocation admission plane (FP1). Cloud
// Functions 2nd gen is backed by Cloud Run: a function scales to at most
// maxInstanceCount instances, each of which serves up to
// maxInstanceRequestConcurrency requests concurrently, so the function's
// concurrent-request capacity is the product of the two. Requests beyond a
// function's capacity (or beyond the project-wide account cap) are rejected
// with RESOURCE_EXHAUSTED. This is the GCP analogue of the AWS Lambda provider's
// reserved/account concurrency enforcement (internal/aws/provider/lambda).
//
// Configured counts are surfaced on the v2 serviceConfig (render.go); no quota
// or account-settings API is added (FD11 records that as unmodellable), so the
// gate is internal and only observable through the 429 it returns.

// concurrencyGate tracks in-flight invocations per function and per project and
// rejects an invocation that would exceed a configured limit. It is
// per-process and ephemeral: the counts are neither persisted nor snapshotted
// (a restart simply begins with nothing in flight) and are cleared on Service
// reset so a reset cannot inherit a count from an aborted invocation.
type concurrencyGate struct {
	mu       sync.Mutex
	funcs    map[string]*invocationCounter
	projects map[string]*invocationCounter
}

// invocationCounter is one in-flight counter. The atomic int64 keeps the hot
// path lock-free once the counter has been created.
type invocationCounter struct{ n atomic.Int64 }

func newConcurrencyGate() *concurrencyGate {
	return &concurrencyGate{
		funcs:    map[string]*invocationCounter{},
		projects: map[string]*invocationCounter{},
	}
}

// acquire reserves an in-flight slot for one invocation. fnLimit is the
// function's concurrent-request capacity and projLimit the project-wide account
// cap; either value <= 0 means "unlimited" and is not tracked. The returned
// release function must be called exactly once when the invocation finishes; it
// is never nil. An invocation that would exceed either limit is not admitted and
// acquire returns the GCP-native RESOURCE_EXHAUSTED / HTTP 429 error.
//
// When both limits are disabled (the default, and every mock-mode unit test
// that never configures maxInstanceCount) acquire takes a fast path that
// allocates nothing, so existing behavior is untouched.
func (g *concurrencyGate) acquire(project, key string, fnLimit, projLimit int64) (func(), error) {
	if g == nil || (fnLimit <= 0 && projLimit <= 0) {
		return func() {}, nil
	}
	var fn *invocationCounter
	if fnLimit > 0 {
		fn = g.counter(g.funcs, key)
		if fn.n.Add(1) > fnLimit {
			fn.n.Add(-1)
			return nil, concurrencyExceeded("function", key)
		}
	}
	var proj *invocationCounter
	if projLimit > 0 {
		proj = g.counter(g.projects, project)
		if proj.n.Add(1) > projLimit {
			proj.n.Add(-1)
			if fn != nil {
				fn.n.Add(-1)
			}
			return nil, concurrencyExceeded("project", project)
		}
	}
	return func() {
		if fn != nil {
			fn.n.Add(-1)
		}
		if proj != nil {
			proj.n.Add(-1)
		}
	}, nil
}

// counter returns the counter for key, creating it on first use. Counters are
// retained for the process lifetime (only reset clears them); the map is bounded
// by the number of distinct function ids and projects ever invoked, which is
// negligible and avoids the in-flight-counting hazard of dropping a counter
// that a concurrent invocation still holds.
func (g *concurrencyGate) counter(m map[string]*invocationCounter, key string) *invocationCounter {
	g.mu.Lock()
	defer g.mu.Unlock()
	c, ok := m[key]
	if !ok {
		c = &invocationCounter{}
		m[key] = c
	}
	return c
}

// reset clears every in-flight count. A release closure created before the
// reset still decrements its now-discarded counter, which is harmless.
func (g *concurrencyGate) reset() {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.funcs = map[string]*invocationCounter{}
	g.projects = map[string]*invocationCounter{}
}

// concurrencyExceeded is the GCP-native admission failure (RESOURCE_EXHAUSTED /
// HTTP 429), the analogue of Lambda's TooManyRequestsException. It names the
// exhausted scope (function or project) to make the cause observable.
func concurrencyExceeded(scope, name string) error {
	return model.NewProviderError("ResourceExhausted",
		"The request was aborted because there were no available instances for "+scope+" "+name+".", 429)
}

// isThrottle reports whether err is invocation admission back-pressure — the
// RESOURCE_EXHAUSTED / HTTP 429 the gate returns when a function (or the
// project) has no free instance. It lets the delivery engine distinguish a
// temporary throttle from a genuine invocation error (which is reported
// in-band) so a throttled event is re-polled without consuming a retry /
// dead-letter attempt (FH2). The check is typed and routed through
// gcperr.Resolve, so it never string-matches the error and agrees with both
// transports.
//
// Only RESOURCE_EXHAUSTED is treated as back-pressure: the admission gate is
// the emulator's sole throttle source and emits 429. A 503 UNAVAILABLE is
// deliberately excluded — an executor failure is surfaced in-band (invokeErr)
// with a nil error, so a 503 here would be an unrelated transport/infra fault,
// not admission back-pressure.
func isThrottle(err error) bool {
	var perr *model.ProviderError
	if !errors.As(err, &perr) {
		return false
	}
	status, _ := gcperr.Resolve(perr)
	return status == gcperr.ResourceExhausted
}
