package tasks

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc/codes"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/httptarget"
	tasksstore "jaiscloud/internal/gcp/store/tasks"
)

// Dispatcher delivers one attempt of a task and reports its outcome. A zero
// Status{Code: 0} means the attempt succeeded (the target answered 2xx).
type Dispatcher interface {
	Deliver(ctx context.Context, t tasksstore.Task) tasksstore.Status
}

// Runner is the default Dispatcher: it performs the HTTP request for an
// httpRequest target and records an Unimplemented failure for an
// appEngineHttpRequest (the emulator has no App Engine router, matching the
// Cloud Scheduler appEngineHttpTarget precedent).
type Runner struct {
	client *http.Client
}

// NewRunner returns a Runner.
func NewRunner() *Runner {
	return &Runner{client: &http.Client{}}
}

// Deliver implements Dispatcher.
func (r *Runner) Deliver(ctx context.Context, t tasksstore.Task) tasksstore.Status {
	switch t.Target {
	case tasksstore.TargetHTTP:
		return r.deliverHTTP(ctx, t)
	default:
		return tasksstore.Status{
			Code:    int32(codes.Unimplemented),
			Message: "appEngineHttpRequest is not delivered by the emulator (no App Engine router)",
		}
	}
}

func (r *Runner) deliverHTTP(ctx context.Context, t tasksstore.Task) tasksstore.Status {
	h := t.HTTP
	if h == nil {
		return tasksstore.Status{Code: int32(codes.InvalidArgument), Message: "httpRequest is missing"}
	}
	headers := make(map[string]string, len(h.Headers)+6)
	for k, v := range h.Headers {
		headers[k] = v
	}
	// Headers real Cloud Tasks attaches to every delivery. TaskRetryCount is
	// the number of prior attempts (0 on the first); TaskExecutionCount is the
	// number of prior handler responses other than 5XX; TaskETA is the expected
	// dispatch time in epoch seconds.
	headers["User-Agent"] = "Google-Cloud-Tasks"
	headers["X-CloudTasks-QueueName"] = t.Queue
	headers["X-CloudTasks-TaskName"] = t.Name
	headers["X-CloudTasks-TaskRetryCount"] = strconv.Itoa(int(t.DispatchCount))
	headers["X-CloudTasks-TaskExecutionCount"] = strconv.Itoa(int(t.ExecutionCount))
	if !t.ScheduleTime.IsZero() {
		headers["X-CloudTasks-TaskETA"] = strconv.FormatInt(t.ScheduleTime.Unix(), 10)
	}
	if h.OAuthToken != nil || h.OidcToken != nil {
		// Real Cloud Tasks mints a Google token; the emulator attaches a
		// synthetic emulator-local bearer token so a target that only checks the
		// header is present still works. Documented limitation.
		headers["Authorization"] = "Bearer emulator-cloud-tasks-token"
	}
	res := httptarget.Deliver(ctx, r.client, httptarget.Request{
		Method:   h.HTTPMethod,
		URL:      h.URL,
		Headers:  headers,
		Body:     h.Body,
		Deadline: t.DispatchDeadline,
		// Cloud Tasks documents that it does not set Content-Type.
		SkipContentTypeDefault: true,
	})
	return tasksstore.Status{Code: res.Code, Message: res.Message}
}

// Engine is the Cloud Tasks dispatch engine. It polls the store on a wall-time
// ticker, selects due tasks on RUNNING queues, and dispatches them gated by the
// queue's rate limits (token bucket + concurrent-dispatch cap). TickNow runs the
// same pass synchronously (POST /_jaiscloud/tasks-tick) so tests can fire tasks
// deterministically against the emulator clock. Business time is clock.Now();
// token refill uses clock.RealNow() so a frozen clock cannot stall the bucket.
type Engine struct {
	store tasksstore.Store
	disp  Dispatcher

	// tickMu serializes tick passes so the background ticker cannot overlap an
	// admin TickNow (which waits for in-flight dispatches).
	tickMu sync.Mutex
	wg     sync.WaitGroup

	mu     sync.Mutex
	queues map[string]*queueState
}

// queueState holds the per-queue rate-limit state. It is rebuilt when the
// queue's rate limits change.
type queueState struct {
	mu       sync.Mutex
	bucket   *tokenBucket
	inflight map[string]bool
	maxConc  int
	rate     float64
	burst    int
}

// NewEngine returns an Engine over the store and dispatcher. A nil dispatcher
// disables delivery (RunTask and tick are no-ops), which unit tests use.
func NewEngine(store tasksstore.Store, disp Dispatcher) *Engine {
	return &Engine{store: store, disp: disp, queues: make(map[string]*queueState)}
}

// Run implements the worker loop: it dispatches due tasks once per second until
// the context is cancelled.
func (e *Engine) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.tick(ctx, false)
		}
	}
}

// TickNow synchronously runs one dispatch pass and waits for every attempt it
// starts. Used by POST /_jaiscloud/tasks-tick and by tests (the engine's own
// ticker uses a wall-time ticker that a frozen clock does not wake).
func (e *Engine) TickNow(ctx context.Context) {
	e.tick(ctx, true)
}

// ResetState clears the in-memory rate-limit and in-flight state (called on
// store reset).
func (e *Engine) ResetState() {
	e.mu.Lock()
	e.queues = make(map[string]*queueState)
	e.mu.Unlock()
}

func (e *Engine) tick(ctx context.Context, wait bool) {
	if e.disp == nil {
		return
	}
	e.tickMu.Lock()
	defer e.tickMu.Unlock()

	queues, err := e.store.ListAllQueues(ctx)
	if err != nil {
		return
	}
	now := clock.Now()
	for _, q := range queues {
		if q.State != tasksstore.StateRunning {
			continue
		}
		tasks, err := e.store.ListTasks(ctx, q.ProjectID, q.Location, q.Name)
		if err != nil {
			continue
		}
		st := e.stateFor(q)
		for _, t := range tasks {
			if t.ScheduleTime.IsZero() || t.ScheduleTime.After(now) {
				continue
			}
			if !st.tryBegin(t.Name) {
				// Already in flight (or the concurrency cap is reached).
				continue
			}
			if !st.bucket.allow(clock.RealNow()) {
				// Dry bucket: no further task on this queue can dispatch this
				// pass, so release the claim and stop scanning it.
				st.end(t.Name)
				break
			}
			e.wg.Add(1)
			go func(t tasksstore.Task, q tasksstore.Queue, st *queueState) {
				defer e.wg.Done()
				defer st.end(t.Name)
				e.attempt(ctx, t, q)
			}(t, q, st)
		}
	}
	if wait {
		e.wg.Wait()
	}
}

func (e *Engine) stateFor(q tasksstore.Queue) *queueState {
	rate, burst, maxConc := queueLimits(q)
	key := q.ProjectID + "/" + q.Location + "/" + q.Name
	e.mu.Lock()
	defer e.mu.Unlock()
	st := e.queues[key]
	if st == nil || st.rate != rate || st.burst != burst || st.maxConc != maxConc {
		st = &queueState{
			bucket:   newTokenBucket(rate, float64(burst), clock.RealNow()),
			inflight: make(map[string]bool),
			maxConc:  maxConc,
			rate:     rate,
			burst:    burst,
		}
		e.queues[key] = st
	}
	return st
}

// queueLimits resolves a queue's effective rate limits, falling back to the
// Cloud Tasks defaults for any unset (zero) value so a queue can never wedge.
func queueLimits(q tasksstore.Queue) (rate float64, burst int, maxConc int) {
	rate, burst, maxConc = 500, 100, 1000
	if q.RateLimits != nil {
		if q.RateLimits.MaxDispatchesPerSecond > 0 {
			rate = q.RateLimits.MaxDispatchesPerSecond
		}
		if q.RateLimits.MaxBurstSize > 0 {
			burst = int(q.RateLimits.MaxBurstSize)
		}
		if q.RateLimits.MaxConcurrentDispatches > 0 {
			maxConc = int(q.RateLimits.MaxConcurrentDispatches)
		}
	}
	return rate, burst, maxConc
}

// attempt delivers one task and records the outcome: success deletes the task
// (real Cloud Tasks deletes a task once its target answers successfully);
// failure increments the counters and either reschedules with exponential
// backoff or drops the task when its retry limits are exhausted.
func (e *Engine) attempt(ctx context.Context, t tasksstore.Task, q tasksstore.Queue) {
	now := clock.Now()
	status := e.disp.Deliver(ctx, t)
	if status.Code == 0 {
		_ = e.store.DeleteTask(ctx, t.ProjectID, t.Location, t.Queue, t.Name)
		return
	}
	dispatches := t.DispatchCount + 1
	next, drop := failureSchedule(q, t, now, dispatches)
	if drop {
		_ = e.store.DeleteTask(ctx, t.ProjectID, t.Location, t.Queue, t.Name)
		return
	}
	attempt := newAttempt(t.ScheduleTime, now, status)
	_, _ = e.store.UpdateTaskAtomic(ctx, t.ProjectID, t.Location, t.Queue, t.Name, func(cur tasksstore.Task) (tasksstore.Task, error) {
		recordAttempt(&cur, attempt, status)
		cur.DispatchCount++
		cur.ScheduleTime = next
		return cur, nil
	})
}

// runNow forces one synchronous attempt for RunTask. It bypasses pause and rate
// limits, matching the Cloud Tasks contract: on success the task is deleted and
// the dispatched snapshot is returned; on failure scheduleTime is reset to now
// plus the queue's retry backoff and the updated task is returned.
func (e *Engine) runNow(ctx context.Context, t tasksstore.Task) (tasksstore.Task, error) {
	if e.disp == nil {
		return t, nil
	}
	// Resolve the queue before the atomic update: the memory store takes a
	// single lock, so a read nested inside UpdateTaskAtomic would deadlock.
	q, err := e.store.GetQueue(ctx, t.ProjectID, t.Location, t.Queue)
	if err != nil {
		return tasksstore.Task{}, mapStoreErr(err)
	}
	now := clock.Now()
	status := e.disp.Deliver(ctx, t)
	attempt := newAttempt(t.ScheduleTime, now, status)
	updated, err := e.store.UpdateTaskAtomic(ctx, t.ProjectID, t.Location, t.Queue, t.Name, func(cur tasksstore.Task) (tasksstore.Task, error) {
		if status.Code == 0 {
			if cur.FirstAttempt == nil {
				cur.FirstAttempt = &tasksstore.Attempt{DispatchTime: attempt.DispatchTime}
			}
			cur.LastAttempt = attempt
			cur.DispatchCount++
			cur.ResponseCount++
			return cur, nil
		}
		recordAttempt(&cur, attempt, status)
		cur.DispatchCount++
		cur.ScheduleTime = now.Add(retryBackoff(q.RetryConfig, t.DispatchCount+1))
		return cur, nil
	})
	if err != nil {
		return tasksstore.Task{}, mapStoreErr(err)
	}
	if status.Code == 0 {
		_ = e.store.DeleteTask(ctx, t.ProjectID, t.Location, t.Queue, t.Name)
	}
	return updated, nil
}

// newAttempt builds the Attempt record for a delivered attempt.
func newAttempt(scheduled, dispatch time.Time, status tasksstore.Status) *tasksstore.Attempt {
	return &tasksstore.Attempt{
		ScheduleTime:   scheduled,
		DispatchTime:   dispatch,
		ResponseTime:   clock.Now(),
		ResponseStatus: &status,
	}
}

// recordAttempt stores a failed attempt on the task. FirstAttempt retains only
// its dispatchTime (Cloud Tasks does not keep the rest of the first attempt);
// LastAttempt keeps the full record. responseCount counts every attempt that
// received a response; executionCount counts responses other than 5XX (the
// X-CloudTasks-TaskExecutionCount semantics). A transport-level failure updates
// neither.
func recordAttempt(cur *tasksstore.Task, attempt *tasksstore.Attempt, status tasksstore.Status) {
	if cur.FirstAttempt == nil {
		cur.FirstAttempt = &tasksstore.Attempt{DispatchTime: attempt.DispatchTime}
	}
	cur.LastAttempt = attempt
	if receivedResponse(status) {
		cur.ResponseCount++
		if status.Code < 500 {
			cur.ExecutionCount++
		}
	}
}

// receivedResponse reports whether the attempt got an HTTP response from the
// target (as opposed to a transport-level failure).
func receivedResponse(status tasksstore.Status) bool {
	return status.Code >= 100 && status.Code <= 599
}

// failureSchedule decides the next attempt time after a failed dispatch. Cloud
// Tasks stops retrying only when BOTH maxAttempts and maxRetryDuration are
// satisfied (or the task succeeds); a maxAttempts of -1 or a maxRetryDuration
// of 0 is unlimited and never blocks. It returns drop=true to have the caller
// delete the task.
func failureSchedule(q tasksstore.Queue, t tasksstore.Task, now time.Time, dispatches int32) (time.Time, bool) {
	maxAttempts := int32(100)
	maxRetryDuration := time.Duration(0)
	if q.RetryConfig != nil {
		if q.RetryConfig.MaxAttempts != 0 {
			maxAttempts = q.RetryConfig.MaxAttempts
		}
		maxRetryDuration = q.RetryConfig.MaxRetryDuration
	}
	attemptsReached := maxAttempts >= 0 && dispatches >= maxAttempts
	durationReached := true
	if maxRetryDuration > 0 {
		first := firstAttemptTime(t, now)
		durationReached = !first.IsZero() && now.Sub(first) >= maxRetryDuration
	}
	if attemptsReached && durationReached {
		return time.Time{}, true
	}
	return now.Add(retryBackoff(q.RetryConfig, dispatches)), false
}

// firstAttemptTime is when the task was first attempted: the recorded first
// attempt, or now when this failure IS the first attempt (FirstAttempt is only
// persisted once the attempt completes).
func firstAttemptTime(t tasksstore.Task, now time.Time) time.Time {
	if t.FirstAttempt != nil && !t.FirstAttempt.DispatchTime.IsZero() {
		return t.FirstAttempt.DispatchTime
	}
	return now
}

// retryBackoff computes the retry interval before attempt number `dispatches`
// (the failed attempt count, starting at 1). Per the Cloud Tasks RetryConfig
// contract the interval starts at minBackoff, doubles at most maxDoublings
// times, then increases linearly by minBackoff·2^maxDoublings, and is finally
// capped at maxBackoff — e.g. 10s,20s,40s,80s,160s,240s,300s,300s with
// min=10s, max=300s, maxDoublings=3. Values fall back to the Cloud Tasks
// defaults.
func retryBackoff(rc *tasksstore.RetryConfig, dispatches int32) time.Duration {
	minB := 100 * time.Millisecond
	maxB := time.Hour
	maxD := int32(16)
	if rc != nil {
		if rc.MinBackoff > 0 {
			minB = rc.MinBackoff
		}
		if rc.MaxBackoff > 0 {
			maxB = rc.MaxBackoff
		}
		if rc.MaxDoublings > 0 {
			maxD = rc.MaxDoublings
		}
	}
	if minB <= 0 {
		return 0
	}
	n := dispatches - 1
	if n < 0 {
		n = 0
	}
	backoff := minB
	for i := int32(0); i < n && i < maxD; i++ {
		if backoff >= maxB {
			break
		}
		backoff *= 2
	}
	if n > maxD {
		step := minB
		for i := int32(0); i < maxD && step < maxB; i++ {
			step *= 2
		}
		for i := maxD; i < n; i++ {
			if backoff >= maxB {
				break
			}
			backoff += step
		}
	}
	if backoff > maxB {
		backoff = maxB
	}
	return backoff
}

func (s *queueState) tryBegin(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inflight[name] {
		return false
	}
	if s.maxConc > 0 && len(s.inflight) >= s.maxConc {
		return false
	}
	s.inflight[name] = true
	return true
}

func (s *queueState) end(name string) {
	s.mu.Lock()
	delete(s.inflight, name)
	s.mu.Unlock()
}

// tokenBucket is a simple refillable token bucket. One token allows one
// dispatch; tokens refill at `rate` per second up to `capacity`.
type tokenBucket struct {
	mu       sync.Mutex
	tokens   float64
	last     time.Time
	rate     float64
	capacity float64
}

func newTokenBucket(rate, capacity float64, now time.Time) *tokenBucket {
	if capacity < 1 {
		capacity = 1
	}
	return &tokenBucket{tokens: capacity, last: now, rate: rate, capacity: capacity}
}

func (b *tokenBucket) allow(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.rate > 0 {
		if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
			b.tokens += elapsed * b.rate
			if b.tokens > b.capacity {
				b.tokens = b.capacity
			}
			b.last = now
		}
	}
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}
