package scheduler

import (
	"context"
	"net/http"
	"sync"
	"time"

	"google.golang.org/grpc/codes"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/httptarget"
	schedstore "jaiscloud/internal/gcp/store/scheduler"
)

// Dispatcher delivers one attempt of a job and reports its outcome. scheduledFor
// is the run's scheduled time — the cron slot this attempt serves. Real Cloud
// Scheduler exposes it to an HTTP target as X-CloudScheduler-ScheduleTime and
// holds it constant across the run's retries so the target can dedupe them. A
// zero Status{Code: 0} means the attempt succeeded.
type Dispatcher interface {
	Deliver(ctx context.Context, j schedstore.Job, scheduledFor time.Time) schedstore.Status
}

// Publisher publishes a Pub/Sub message for a pubsubTarget. It is injected by
// the binary so this package does not depend on the Pub/Sub store.
type Publisher interface {
	Publish(ctx context.Context, topic string, data []byte, attributes map[string]string) error
}

// Runner is the default Dispatcher: it performs the HTTP request for an
// httpTarget, publishes through the injected Publisher for a pubsubTarget, and
// records an Unimplemented failure for an appEngineHttpTarget (the emulator has
// no App Engine router).
type Runner struct {
	client    *http.Client
	publisher Publisher
}

// NewRunner returns a Runner. publisher may be nil when no Pub/Sub target is
// ever delivered.
func NewRunner(publisher Publisher) *Runner {
	return &Runner{client: &http.Client{}, publisher: publisher}
}

// Deliver implements Dispatcher.
func (r *Runner) Deliver(ctx context.Context, j schedstore.Job, scheduledFor time.Time) schedstore.Status {
	switch j.Target {
	case schedstore.TargetHTTP:
		return r.deliverHTTP(ctx, j, scheduledFor)
	case schedstore.TargetPubSub:
		return r.deliverPubSub(ctx, j)
	default:
		return schedstore.Status{
			Code:    int32(codes.Unimplemented),
			Message: "appEngineHttpTarget is not delivered by the emulator (no App Engine router)",
		}
	}
}

func (r *Runner) deliverHTTP(ctx context.Context, j schedstore.Job, scheduledFor time.Time) schedstore.Status {
	t := j.HTTP
	if t == nil {
		return schedstore.Status{Code: int32(codes.InvalidArgument), Message: "httpTarget is missing"}
	}
	headers := make(map[string]string, len(t.Headers)+3)
	for k, v := range t.Headers {
		headers[k] = v
	}
	// Headers real Cloud Scheduler attaches to every delivery. It computes and
	// replaces these, so they are set after the user's headers and win.
	headers["User-Agent"] = "Google-Cloud-Scheduler"
	headers["X-CloudScheduler"] = "true"
	headers["X-CloudScheduler-JobName"] = JobName(j.ProjectID, j.Location, j.Name)
	if !scheduledFor.IsZero() {
		// RFC3339; constant across a run's retries (the engine passes the same
		// run slot on every attempt) so the target can dedupe.
		headers["X-CloudScheduler-ScheduleTime"] = scheduledFor.UTC().Format(time.RFC3339)
	}
	if t.OAuthToken != nil || t.OidcToken != nil {
		// Real Cloud Scheduler mints a Google token; the emulator attaches a
		// synthetic emulator-local bearer token so a target that only checks the
		// header is present still works. Documented limitation.
		headers["Authorization"] = "Bearer emulator-cloud-scheduler-token"
	}
	res := httptarget.Deliver(ctx, r.client, httptarget.Request{
		Method:   t.HTTPMethod,
		URL:      t.URI,
		Headers:  headers,
		Body:     t.Body,
		Deadline: j.AttemptDeadline,
	})
	return schedstore.Status{Code: res.Code, Message: res.Message}
}

func (r *Runner) deliverPubSub(ctx context.Context, j schedstore.Job) schedstore.Status {
	if r.publisher == nil {
		return schedstore.Status{Code: int32(codes.FailedPrecondition), Message: "no Pub/Sub publisher configured"}
	}
	t := j.PubSub
	if t == nil || t.TopicName == "" {
		return schedstore.Status{Code: int32(codes.InvalidArgument), Message: "pubsubTarget is missing topicName"}
	}
	if err := r.publisher.Publish(ctx, t.TopicName, t.Data, t.Attributes); err != nil {
		return schedstore.Status{Code: int32(codes.Internal), Message: err.Error()}
	}
	return schedstore.Status{}
}

// Engine is the Cloud Scheduler cron engine. It polls the store on a wall-time
// ticker and fires every ENABLED job whose ScheduleTime is due; TickNow runs the
// same evaluation synchronously for deterministic tests (POST
// /_jaiscloud/scheduler-tick). Business time comes from clock.Now().
type Engine struct {
	store schedstore.Store
	disp  Dispatcher

	mu    sync.Mutex
	retry map[string]retryState
}

type retryState struct {
	attempts int
	// scheduledAt is the cron slot the run is serving. It is captured on the
	// run's first failed attempt and reused for every retry so the
	// X-CloudScheduler-ScheduleTime header stays constant across the run.
	scheduledAt time.Time
}

// NewEngine returns an Engine over the store and dispatcher.
func NewEngine(store schedstore.Store, disp Dispatcher) *Engine {
	return &Engine{store: store, disp: disp, retry: make(map[string]retryState)}
}

// SetDispatcher replaces the dispatcher (used by tests).
func (e *Engine) SetDispatcher(d Dispatcher) { e.disp = d }

// Run implements workers.Worker: it evaluates due jobs once per second until the
// context is cancelled.
func (e *Engine) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.TickNow(ctx)
		}
	}
}

// TickNow synchronously evaluates every job against clock.Now() and fires the
// due ones. Used by POST /_jaiscloud/scheduler-tick.
func (e *Engine) TickNow(ctx context.Context) {
	if e.disp == nil {
		return
	}
	jobs, err := e.store.ListAllJobs(ctx)
	if err != nil {
		return
	}
	now := clock.Now()
	for _, j := range jobs {
		if j.State != schedstore.StateEnabled || j.ScheduleTime.IsZero() || j.ScheduleTime.After(now) {
			continue
		}
		e.fire(ctx, j, now)
	}
}

// ResetRetries clears the in-memory retry counters (called on store reset).
func (e *Engine) ResetRetries() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.retry = make(map[string]retryState)
}

func retryKey(j schedstore.Job) string {
	return j.ProjectID + "/" + j.Location + "/" + j.Name
}

// fire delivers one attempt and persists the updated output-only fields.
func (e *Engine) fire(ctx context.Context, j schedstore.Job, now time.Time) {
	status := e.disp.Deliver(ctx, j, e.scheduledFor(j))
	next := e.nextScheduleTime(j, now, status)
	_, _ = e.store.UpdateJobAtomic(ctx, j.ProjectID, j.Location, j.Name, func(cur schedstore.Job) (schedstore.Job, error) {
		cur.LastAttemptTime = now
		if status.Code == 0 {
			cur.Status = nil
		} else {
			s := status
			cur.Status = &s
		}
		cur.ScheduleTime = next
		return cur, nil
	})
}

// scheduledFor returns the cron slot the current attempt serves. While a run is
// being retried it is the slot recorded on the first failure (so the retry
// carries the same X-CloudScheduler-ScheduleTime); otherwise it is the job's
// due schedule time.
func (e *Engine) scheduledFor(j schedstore.Job) time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()
	if st, ok := e.retry[retryKey(j)]; ok && !st.scheduledAt.IsZero() {
		return st.scheduledAt
	}
	return j.ScheduleTime
}

// nextScheduleTime picks the next fire time after an attempt: a retry with
// exponential backoff while attempts remain, otherwise the next cron time.
func (e *Engine) nextScheduleTime(j schedstore.Job, now time.Time, status schedstore.Status) time.Time {
	key := retryKey(j)
	if status.Code == 0 {
		e.mu.Lock()
		delete(e.retry, key)
		e.mu.Unlock()
		return nextFireFor(j, now)
	}
	minBackoff := 5 * time.Second
	maxBackoff := time.Hour
	retryCount := int32(0)
	if j.RetryConfig != nil {
		if j.RetryConfig.RetryCount > 0 {
			retryCount = j.RetryConfig.RetryCount
		}
		if j.RetryConfig.MinBackoffDuration > 0 {
			minBackoff = j.RetryConfig.MinBackoffDuration
		}
		if j.RetryConfig.MaxBackoffDuration > 0 {
			maxBackoff = j.RetryConfig.MaxBackoffDuration
		}
	}
	e.mu.Lock()
	state := e.retry[key]
	// Record the run's cron slot on its first failed attempt. j.ScheduleTime is
	// the due slot here; later retries see a backoff-rescheduled j.ScheduleTime,
	// so only the first failure captures the slot.
	if state.scheduledAt.IsZero() {
		state.scheduledAt = j.ScheduleTime
	}
	if int32(state.attempts) < retryCount {
		state.attempts++
		e.retry[key] = state
		e.mu.Unlock()
		backoff := minBackoff
		for i := 1; i < state.attempts; i++ {
			backoff *= 2
			if backoff >= maxBackoff {
				backoff = maxBackoff
				break
			}
		}
		return now.Add(backoff)
	}
	delete(e.retry, key)
	e.mu.Unlock()
	return nextFireFor(j, now)
}
