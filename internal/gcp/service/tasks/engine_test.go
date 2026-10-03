package tasks

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"jaiscloud/internal/clock"
	tasksstore "jaiscloud/internal/gcp/store/tasks"
	"jaiscloud/internal/store"
)

func withFixedClock(t *testing.T, at time.Time) {
	t.Helper()
	clock.SetGlobalClock(clock.FixedClock{T: at})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })
}

func newEngineCore(t *testing.T) (*Service, *Engine, *tasksstore.MemoryStore) {
	t.Helper()
	mem := tasksstore.NewMemoryStore()
	engine := NewEngine(mem, NewRunner())
	core := NewService(mem, store.NewMemoryResourceStore())
	core.SetEngine(engine)
	return core, engine, mem
}

func mustQueue(t *testing.T, core *Service, q tasksstore.Queue) {
	t.Helper()
	if _, err := core.CreateQueue(context.Background(), "p", "l", q); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
}

func mustTask(t *testing.T, core *Service, queue string, tk tasksstore.Task) tasksstore.Task {
	t.Helper()
	created, err := core.CreateTask(context.Background(), "p", "l", queue, tk)
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	return created
}

func TestEngineDispatchesHTTPAndDeletes(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	withFixedClock(t, at)
	ctx := context.Background()
	core, engine, _ := newEngineCore(t)

	var (
		mu      sync.Mutex
		hits    int
		headers http.Header
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		headers = r.Header.Clone()
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	mustQueue(t, core, tasksstore.Queue{Name: "q1"})
	tk := mustTask(t, core, "q1", tasksstore.Task{Name: "t1", Target: tasksstore.TargetHTTP, HTTP: &tasksstore.HttpRequest{URL: server.URL, HTTPMethod: "POST", Body: []byte("x")}})

	engine.TickNow(ctx)

	mu.Lock()
	defer mu.Unlock()
	if hits != 1 {
		t.Fatalf("hits = %d, want 1", hits)
	}
	if got := headers.Get("User-Agent"); got != "Google-Cloud-Tasks" {
		t.Fatalf("User-Agent = %q", got)
	}
	if got := headers.Get("X-CloudTasks-QueueName"); got != "q1" {
		t.Fatalf("X-CloudTasks-QueueName = %q", got)
	}
	if got := headers.Get("X-CloudTasks-TaskName"); got != "t1" {
		t.Fatalf("X-CloudTasks-TaskName = %q", got)
	}
	if got := headers.Get("X-CloudTasks-TaskRetryCount"); got != "0" {
		t.Fatalf("X-CloudTasks-TaskRetryCount = %q", got)
	}
	if got := headers.Get("X-CloudTasks-TaskExecutionCount"); got != "0" {
		t.Fatalf("X-CloudTasks-TaskExecutionCount = %q", got)
	}
	if got := headers.Get("X-CloudTasks-TaskETA"); got != strconv.FormatInt(at.Unix(), 10) {
		t.Fatalf("X-CloudTasks-TaskETA = %q, want epoch seconds", got)
	}
	if _, err := core.GetTask(ctx, "p", "l", "q1", tk.Name); err == nil {
		t.Fatalf("successful task was not deleted")
	}
}

func TestEngineRetryBackoffAndDrop(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	withFixedClock(t, at)
	ctx := context.Background()
	core, engine, _ := newEngineCore(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	mustQueue(t, core, tasksstore.Queue{Name: "q1", RetryConfig: &tasksstore.RetryConfig{
		MaxAttempts: 2, MinBackoff: time.Second, MaxBackoff: time.Hour, MaxDoublings: 16,
	}})
	tk := mustTask(t, core, "q1", tasksstore.Task{Name: "t1", Target: tasksstore.TargetHTTP, HTTP: &tasksstore.HttpRequest{URL: server.URL}})

	engine.TickNow(ctx)
	got, err := core.GetTask(ctx, "p", "l", "q1", tk.Name)
	if err != nil {
		t.Fatalf("GetTask after first failure: %v", err)
	}
	if got.DispatchCount != 1 || got.ResponseCount != 1 || got.LastAttempt == nil || got.LastAttempt.ResponseStatus.Code != http.StatusInternalServerError {
		t.Fatalf("after first failure: %+v", got)
	}
	if got.FirstAttempt == nil || got.FirstAttempt.ScheduleTime != (time.Time{}) {
		t.Fatalf("firstAttempt should retain only dispatchTime: %+v", got.FirstAttempt)
	}
	if got.ExecutionCount != 0 {
		t.Fatalf("executionCount after a 5XX = %d, want 0", got.ExecutionCount)
	}
	if !got.ScheduleTime.Equal(at.Add(time.Second)) {
		t.Fatalf("retry scheduleTime = %v, want %v", got.ScheduleTime, at.Add(time.Second))
	}

	clock.SetGlobalClock(clock.FixedClock{T: at.Add(time.Second)})
	engine.TickNow(ctx)
	// maxAttempts=2 and this was the second attempt: the task is dropped.
	if _, err := core.GetTask(ctx, "p", "l", "q1", tk.Name); err == nil {
		t.Fatalf("exhausted task was not dropped")
	}
}

func TestEnginePauseGating(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	withFixedClock(t, at)
	ctx := context.Background()
	core, engine, _ := newEngineCore(t)

	var hits int32
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	mustQueue(t, core, tasksstore.Queue{Name: "q1"})
	tk := mustTask(t, core, "q1", tasksstore.Task{Name: "t1", Target: tasksstore.TargetHTTP, HTTP: &tasksstore.HttpRequest{URL: server.URL}})
	if _, err := core.PauseQueue(ctx, "p", "l", "q1"); err != nil {
		t.Fatalf("PauseQueue: %v", err)
	}

	engine.TickNow(ctx)
	if hits != 0 {
		t.Fatalf("paused queue dispatched %d tasks", hits)
	}
	if _, err := core.GetTask(ctx, "p", "l", "q1", tk.Name); err != nil {
		t.Fatalf("paused task vanished: %v", err)
	}

	if _, err := core.ResumeQueue(ctx, "p", "l", "q1"); err != nil {
		t.Fatalf("ResumeQueue: %v", err)
	}
	engine.TickNow(ctx)
	if hits != 1 {
		t.Fatalf("resumed queue hits = %d, want 1", hits)
	}
}

func TestEngineRateLimit(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	withFixedClock(t, at)
	ctx := context.Background()
	core, engine, _ := newEngineCore(t)

	var mu sync.Mutex
	hits := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// One-token burst, effectively no refill within the test.
	mustQueue(t, core, tasksstore.Queue{Name: "q1", RateLimits: &tasksstore.RateLimits{
		MaxDispatchesPerSecond: 0.0001, MaxBurstSize: 1, MaxConcurrentDispatches: 1000,
	}})
	mustTask(t, core, "q1", tasksstore.Task{Name: "t1", Target: tasksstore.TargetHTTP, HTTP: &tasksstore.HttpRequest{URL: server.URL}})
	mustTask(t, core, "q1", tasksstore.Task{Name: "t2", Target: tasksstore.TargetHTTP, HTTP: &tasksstore.HttpRequest{URL: server.URL}})

	engine.TickNow(ctx)

	mu.Lock()
	defer mu.Unlock()
	if hits != 1 {
		t.Fatalf("hits = %d, want 1 (burst)", hits)
	}
	remaining, _ := core.ListTasks(ctx, "p", "l", "q1")
	if len(remaining) != 1 {
		t.Fatalf("remaining tasks = %d, want 1", len(remaining))
	}
}

// gateDispatcher blocks each delivery until release is closed and records which
// tasks started.
type gateDispatcher struct {
	started chan string
	release chan struct{}

	mu   sync.Mutex
	seen []string
}

func (d *gateDispatcher) Deliver(_ context.Context, t tasksstore.Task) tasksstore.Status {
	d.mu.Lock()
	d.seen = append(d.seen, t.Name)
	d.mu.Unlock()
	d.started <- t.Name
	<-d.release
	return tasksstore.Status{}
}

func (d *gateDispatcher) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.seen)
}

func TestEngineConcurrencyCap(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	withFixedClock(t, at)
	ctx := context.Background()

	mem := tasksstore.NewMemoryStore()
	disp := &gateDispatcher{started: make(chan string, 2), release: make(chan struct{})}
	engine := NewEngine(mem, disp)
	core := NewService(mem, store.NewMemoryResourceStore())
	core.SetEngine(engine)

	mustQueue(t, core, tasksstore.Queue{Name: "q1", RateLimits: &tasksstore.RateLimits{
		MaxDispatchesPerSecond: 500, MaxBurstSize: 100, MaxConcurrentDispatches: 1,
	}})
	mustTask(t, core, "q1", tasksstore.Task{Name: "t1", Target: tasksstore.TargetHTTP, HTTP: &tasksstore.HttpRequest{URL: "http://example.test/1"}})
	mustTask(t, core, "q1", tasksstore.Task{Name: "t2", Target: tasksstore.TargetHTTP, HTTP: &tasksstore.HttpRequest{URL: "http://example.test/2"}})

	// tick without waiting so the single in-flight attempt stays blocked.
	engine.tick(ctx, false)
	select {
	case <-disp.started:
	case <-time.After(2 * time.Second):
		t.Fatalf("no dispatch started")
	}
	if got := disp.count(); got != 1 {
		t.Fatalf("concurrent dispatches = %d, want 1", got)
	}
	close(disp.release)
	engine.wg.Wait()
}

func TestEngineRunTaskDeletesOnSuccess(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	withFixedClock(t, at)
	ctx := context.Background()
	core, _, _ := newEngineCore(t)

	var hits int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	mustQueue(t, core, tasksstore.Queue{Name: "q1"})
	tk := mustTask(t, core, "q1", tasksstore.Task{Name: "t1", Target: tasksstore.TargetHTTP, HTTP: &tasksstore.HttpRequest{URL: server.URL}})

	ran, err := core.RunTask(ctx, "p", "l", "q1", tk.Name)
	if err != nil {
		t.Fatalf("RunTask: %v", err)
	}
	if hits != 1 || ran.DispatchCount != 1 || ran.ResponseCount != 1 {
		t.Fatalf("ran=%+v hits=%d", ran, hits)
	}
	if _, err := core.GetTask(ctx, "p", "l", "q1", tk.Name); err == nil {
		t.Fatalf("RunTask success did not delete the task")
	}
}

func TestEngineRunTaskFailureReschedules(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	withFixedClock(t, at)
	ctx := context.Background()
	core, _, _ := newEngineCore(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer server.Close()

	mustQueue(t, core, tasksstore.Queue{Name: "q1", RetryConfig: &tasksstore.RetryConfig{
		MaxAttempts: 5, MinBackoff: 2 * time.Second, MaxBackoff: time.Hour, MaxDoublings: 4,
	}})
	tk := mustTask(t, core, "q1", tasksstore.Task{Name: "t1", Target: tasksstore.TargetHTTP, HTTP: &tasksstore.HttpRequest{URL: server.URL}})

	ran, err := core.RunTask(ctx, "p", "l", "q1", tk.Name)
	if err != nil {
		t.Fatalf("RunTask: %v", err)
	}
	if ran.DispatchCount != 1 || ran.LastAttempt == nil || ran.LastAttempt.ResponseStatus.Code != http.StatusBadGateway {
		t.Fatalf("ran = %+v", ran)
	}
	if !ran.ScheduleTime.Equal(at.Add(2 * time.Second)) {
		t.Fatalf("reschedule = %v, want %v", ran.ScheduleTime, at.Add(2*time.Second))
	}
	if _, err := core.GetTask(ctx, "p", "l", "q1", tk.Name); err != nil {
		t.Fatalf("failed task vanished: %v", err)
	}
}

func TestRetryBackoffShape(t *testing.T) {
	rc := &tasksstore.RetryConfig{MinBackoff: 10 * time.Second, MaxBackoff: 300 * time.Second, MaxDoublings: 3}
	want := []time.Duration{
		10 * time.Second, 20 * time.Second, 40 * time.Second, 80 * time.Second,
		160 * time.Second, 240 * time.Second, 300 * time.Second, 300 * time.Second,
	}
	for i, w := range want {
		if got := retryBackoff(rc, int32(i+1)); got != w {
			t.Fatalf("retryBackoff(attempt %d) = %v, want %v", i+1, got, w)
		}
	}
}

func TestEngineMaxRetryDurationMeasuredFromFirstAttempt(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	withFixedClock(t, at)
	ctx := context.Background()
	core, engine, _ := newEngineCore(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	// createTime is far in the past relative to the (future) schedule; the
	// duration limit must be measured from the first attempt, not creation, so
	// the first failure still schedules a retry even though the task was
	// created more than maxRetryDuration ago.
	mustQueue(t, core, tasksstore.Queue{Name: "q1", RetryConfig: &tasksstore.RetryConfig{
		MaxAttempts: 1, MaxRetryDuration: time.Hour, MinBackoff: 5 * time.Second, MaxBackoff: time.Hour, MaxDoublings: 4,
	}})
	due := at.Add(2 * time.Hour)
	tk := mustTask(t, core, "q1", tasksstore.Task{Name: "t1", Target: tasksstore.TargetHTTP, ScheduleTime: due, HTTP: &tasksstore.HttpRequest{URL: server.URL}})

	clock.SetGlobalClock(clock.FixedClock{T: due})
	engine.TickNow(ctx)
	got, err := core.GetTask(ctx, "p", "l", "q1", tk.Name)
	if err != nil {
		t.Fatalf("task dropped after first failure (maxRetryDuration measured from creation): %v", err)
	}
	if got.DispatchCount != 1 || !got.ScheduleTime.Equal(due.Add(5*time.Second)) {
		t.Fatalf("after first failure: %+v", got)
	}
}

func TestEngineAppEngineRecordsFailure(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	withFixedClock(t, at)
	ctx := context.Background()
	core, engine, _ := newEngineCore(t)

	mustQueue(t, core, tasksstore.Queue{Name: "q1"})
	tk := mustTask(t, core, "q1", tasksstore.Task{Name: "t1", Target: tasksstore.TargetAppEngine, AppEngine: &tasksstore.AppEngineHttpRequest{RelativeURI: "/do", HTTPMethod: "POST"}})

	engine.TickNow(ctx)
	got, err := core.GetTask(ctx, "p", "l", "q1", tk.Name)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if got.LastAttempt == nil || got.LastAttempt.ResponseStatus == nil || got.LastAttempt.ResponseStatus.Code != int32(codes.Unimplemented) {
		t.Fatalf("appEngine attempt = %+v", got.LastAttempt)
	}
}
