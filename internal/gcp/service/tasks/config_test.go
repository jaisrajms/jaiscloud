package tasks

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	tasksstore "jaiscloud/internal/gcp/store/tasks"
	"jaiscloud/internal/model"
)

// failingServer returns a target that always answers 500 so a dispatched task
// keeps retrying instead of being deleted.
func failingServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestEngineTaskRetryConfigOverridesQueue is CT7's core semantic: a task-level
// retryConfig is a FULL override of the queue's, so both the backoff schedule
// and the attempt limit come from the task. The queue here would drop on the
// first failure with a one-hour backoff; the task allows three attempts starting
// at five seconds.
func TestEngineTaskRetryConfigOverridesQueue(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	withFixedClock(t, at)
	ctx := context.Background()
	core, engine, _ := newEngineCore(t)
	srv := failingServer(t)

	mustQueue(t, core, tasksstore.Queue{Name: "q1", RetryConfig: &tasksstore.RetryConfig{
		MaxAttempts: 1, MinBackoff: time.Hour, MaxBackoff: 2 * time.Hour, MaxDoublings: 1,
	}})
	tk := mustTask(t, core, "q1", tasksstore.Task{
		Name: "t1", Target: tasksstore.TargetHTTP,
		HTTP: &tasksstore.HttpRequest{URL: srv.URL},
		RetryConfig: &tasksstore.RetryConfig{
			MaxAttempts: 3, MinBackoff: 5 * time.Second, MaxBackoff: time.Minute, MaxDoublings: 2,
		},
	})

	// Attempt 1: the queue's maxAttempts=1 would drop the task; the task's
	// config keeps it, and the reschedule uses the task's minBackoff, not the
	// queue's hour.
	engine.TickNow(ctx)
	got, err := core.GetTask(ctx, "p", "l", "q1", tk.Name)
	if err != nil {
		t.Fatalf("task dropped after the first failure; the queue's maxAttempts governed: %v", err)
	}
	if got.DispatchCount != 1 || !got.ScheduleTime.Equal(at.Add(5*time.Second)) {
		t.Fatalf("after attempt 1: dispatchCount=%d scheduleTime=%v, want 1/%v (task minBackoff)",
			got.DispatchCount, got.ScheduleTime, at.Add(5*time.Second))
	}

	clock.SetGlobalClock(clock.FixedClock{T: at.Add(5 * time.Second)})
	engine.TickNow(ctx)
	got, err = core.GetTask(ctx, "p", "l", "q1", tk.Name)
	if err != nil || got.DispatchCount != 2 {
		t.Fatalf("after attempt 2: %+v, %v", got, err)
	}

	// Attempt 3 reaches the task's maxAttempts (and maxRetryDuration is 0, i.e.
	// unlimited), so the task is deleted even though the queue allows more.
	clock.SetGlobalClock(clock.FixedClock{T: got.ScheduleTime})
	engine.TickNow(ctx)
	if _, err := core.GetTask(ctx, "p", "l", "q1", tk.Name); err == nil {
		t.Fatalf("task survived its task-level maxAttempts=3")
	}
}

// TestEngineTaskRetryConfigMaxRetryDuration proves maxRetryDuration is read from
// the task override too: the queue would retry forever and the task stops 60s
// after its first attempt.
func TestEngineTaskRetryConfigMaxRetryDuration(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	withFixedClock(t, at)
	ctx := context.Background()
	core, engine, _ := newEngineCore(t)
	srv := failingServer(t)

	mustQueue(t, core, tasksstore.Queue{Name: "q1", RetryConfig: &tasksstore.RetryConfig{
		MaxAttempts: 100, MinBackoff: time.Hour, MaxBackoff: time.Hour, MaxDoublings: 1,
	}})
	tk := mustTask(t, core, "q1", tasksstore.Task{
		Name: "t1", Target: tasksstore.TargetHTTP,
		HTTP: &tasksstore.HttpRequest{URL: srv.URL},
		RetryConfig: &tasksstore.RetryConfig{
			MaxAttempts: 3, MaxRetryDuration: time.Minute,
			MinBackoff: 10 * time.Second, MaxBackoff: time.Minute, MaxDoublings: 2,
		},
	})

	// Three failures inside the minute: attempts are exhausted but the duration
	// is not, so Cloud Tasks keeps retrying ("both" must be satisfied).
	now := at
	for i := 0; i < 3; i++ {
		clock.SetGlobalClock(clock.FixedClock{T: now})
		engine.TickNow(ctx)
		got, err := core.GetTask(ctx, "p", "l", "q1", tk.Name)
		if err != nil {
			t.Fatalf("task dropped after %d failure(s) before maxRetryDuration: %v", i+1, err)
		}
		now = got.ScheduleTime
	}

	// Past the minute measured from the first attempt: the next due retry now
	// satisfies both conditions and the task is dropped, although the queue's
	// own config (100 attempts, no duration limit) never would.
	clock.SetGlobalClock(clock.FixedClock{T: now})
	engine.TickNow(ctx)
	if _, err := core.GetTask(ctx, "p", "l", "q1", tk.Name); err == nil {
		t.Fatalf("task survived the task-level maxRetryDuration=60s")
	}
}

// TestEngineTaskRetryConfigZeroMeansUnspecified documents the wire convention
// the queue side already uses: a zero maxAttempts in a RetryConfig is
// "unspecified", so it resolves to the Cloud Tasks default (100 attempts), NOT
// to the queue's value — even though the task config overrides the queue.
func TestEngineTaskRetryConfigZeroMeansUnspecified(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	withFixedClock(t, at)
	ctx := context.Background()
	core, engine, _ := newEngineCore(t)
	srv := failingServer(t)

	// The queue drops after 5 attempts and backs off by 30s; a task config of
	// all zeros must instead use the defaults (100 attempts, 100ms backoff).
	mustQueue(t, core, tasksstore.Queue{Name: "q1", RetryConfig: &tasksstore.RetryConfig{
		MaxAttempts: 5, MinBackoff: 30 * time.Second, MaxBackoff: time.Hour, MaxDoublings: 4,
	}})
	tk := mustTask(t, core, "q1", tasksstore.Task{
		Name: "t1", Target: tasksstore.TargetHTTP,
		HTTP:        &tasksstore.HttpRequest{URL: srv.URL},
		RetryConfig: &tasksstore.RetryConfig{},
	})

	engine.TickNow(ctx)
	got, err := core.GetTask(ctx, "p", "l", "q1", tk.Name)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	// Default minBackoff is 100ms, not the queue's 30s.
	if want := at.Add(100 * time.Millisecond); !got.ScheduleTime.Equal(want) {
		t.Fatalf("reschedule = %v, want %v (default minBackoff, not the queue's)", got.ScheduleTime, want)
	}

	// Five failures would exhaust the queue's maxAttempts; the zeroed task
	// config means "default 100", so the task survives.
	now := got.ScheduleTime
	for i := 0; i < 4; i++ {
		clock.SetGlobalClock(clock.FixedClock{T: now})
		engine.TickNow(ctx)
		got, err = core.GetTask(ctx, "p", "l", "q1", tk.Name)
		if err != nil {
			t.Fatalf("task dropped at attempt %d: the queue's maxAttempts=5 governed a zero task config", i+2)
		}
		now = got.ScheduleTime
	}
}

// TestEngineTaskExpiresAfterMaxRetention is CT6: retention is 31 days from
// scheduleTime, enforced by the dispatch tick (never by a read), and before the
// due check so an expired-but-overdue task is not delivered.
func TestEngineTaskExpiresAfterMaxRetention(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	withFixedClock(t, at)
	ctx := context.Background()
	core, engine, _ := newEngineCore(t)
	mustQueue(t, core, tasksstore.Queue{Name: "q1", RetryConfig: &tasksstore.RetryConfig{
		MaxAttempts: 100, MinBackoff: time.Second, MaxBackoff: time.Hour,
	}})

	// A failing HTTP target keeps a non-expired, due task alive through the tick
	// (the failed attempt schedules a retry), so the assertion is about
	// retention only.
	failing := failingServer(t)
	defer failing.Close()

	retained := mustTask(t, core, "q1", tasksstore.Task{
		Name: "retained", Target: tasksstore.TargetHTTP,
		HTTP:         &tasksstore.HttpRequest{URL: failing.URL},
		ScheduleTime: at.Add(-30 * 24 * time.Hour),
	})
	expired := mustTask(t, core, "q1", tasksstore.Task{
		Name: "expired", Target: tasksstore.TargetHTTP,
		HTTP:         &tasksstore.HttpRequest{URL: failing.URL},
		ScheduleTime: at.Add(-maxTaskRetention - time.Second),
	})

	// Reads stay pure: an expired task is still readable before a tick runs.
	if _, err := core.GetTask(ctx, "p", "l", "q1", expired.Name); err != nil {
		t.Fatalf("GetTask deleted an expired task (reads must not mutate): %v", err)
	}
	if list, err := core.ListTasks(ctx, "p", "l", "q1"); err != nil || len(list) != 2 {
		t.Fatalf("ListTasks = %d, %v; want both tasks before the tick", len(list), err)
	}

	engine.TickNow(ctx)

	if _, err := core.GetTask(ctx, "p", "l", "q1", expired.Name); err == nil {
		t.Fatalf("expired task survived the dispatch tick")
	}
	if _, err := core.GetTask(ctx, "p", "l", "q1", retained.Name); err != nil {
		t.Fatalf("30-day-old task was expired too early: %v", err)
	}
}

// TestBufferTask covers tasks:buffer's two path forms plus the documented
// limitation: the emulator models no queue-level httpTarget, so a BufferTask
// request that carries only a body has no URL to deliver to and is rejected the
// way real Cloud Tasks rejects a queue without an HTTP target.
func TestBufferTask(t *testing.T) {
	ctx := context.Background()
	core, _, _ := newEngineCore(t)
	mustQueue(t, core, tasksstore.Queue{Name: "q1"})

	full := tasksstore.Task{
		Target: tasksstore.TargetHTTP,
		HTTP:   &tasksstore.HttpRequest{URL: "http://example.test/buffered", HTTPMethod: "POST"},
	}
	// Custom task id form (.../tasks/{taskId}:buffer).
	created, err := core.BufferTask(ctx, "p", "l", "q1", "buffered-1", full, nil)
	if err != nil {
		t.Fatalf("BufferTask(custom id): %v", err)
	}
	if created.Name != "buffered-1" || created.HTTP == nil || created.HTTP.URL != "http://example.test/buffered" {
		t.Fatalf("buffered task = %+v", created)
	}
	// Auto-id form (.../tasks:buffer).
	created, err = core.BufferTask(ctx, "p", "l", "q1", "", full, nil)
	if err != nil {
		t.Fatalf("BufferTask(generated id): %v", err)
	}
	if created.Name == "" {
		t.Fatalf("BufferTask generated no task id")
	}
	// Body-only request: no queue httpTarget exists to resolve, so this is the
	// error real GCP returns for a queue without an HTTP target.
	_, err = core.BufferTask(ctx, "p", "l", "q1", "", tasksstore.Task{}, &HttpBody{ContentType: "text/plain", Data: []byte("hi")})
	perr, ok := err.(*model.ProviderError)
	if !ok || perr.Code != "InvalidArgument" || perr.HTTPStatus != 400 {
		t.Fatalf("BufferTask(body only) err = %v, want InvalidArgument/400", err)
	}
}
