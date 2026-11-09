package scheduler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"jaiscloud/internal/clock"
	schedstore "jaiscloud/internal/gcp/store/scheduler"
	"jaiscloud/internal/model"
)

type fakePublisher struct {
	calls atomic.Int32
	topic string
	data  []byte
	attrs map[string]string
}

func (f *fakePublisher) Publish(_ context.Context, topic string, data []byte, attributes map[string]string) error {
	f.calls.Add(1)
	f.topic = topic
	f.data = data
	f.attrs = attributes
	return nil
}

func fixedClock(t *testing.T, at time.Time) {
	t.Helper()
	clock.SetGlobalClock(clock.FixedClock{T: at})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })
}

func newCore(t *testing.T) (*Service, *Engine, *schedstore.MemoryStore) {
	t.Helper()
	mem := schedstore.NewMemoryStore()
	engine := NewEngine(mem, NewRunner(nil))
	core := NewService(mem)
	core.SetEngine(engine)
	return core, engine, mem
}

func TestCreateGetListDelete(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fixedClock(t, at)
	core, _, _ := newCore(t)

	created, err := core.CreateJob(context.Background(), "proj", "us-central1", schedstore.Job{
		Schedule: "*/5 * * * *",
		TimeZone: "UTC",
		Target:   schedstore.TargetHTTP,
		HTTP:     &schedstore.HttpTarget{URI: "http://example.test/hook"},
	})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if created.State != schedstore.StateEnabled {
		t.Fatalf("state = %q, want ENABLED", created.State)
	}
	if created.ScheduleTime.IsZero() || !created.ScheduleTime.Equal(at.Add(5*time.Minute)) {
		t.Fatalf("scheduleTime = %v, want %v", created.ScheduleTime, at.Add(5*time.Minute))
	}

	got, err := core.GetJob(context.Background(), "proj", "us-central1", created.Name)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Name != created.Name {
		t.Fatalf("GetJob name = %q, want %q", got.Name, created.Name)
	}

	list, err := core.ListJobs(context.Background(), "proj", "us-central1")
	if err != nil || len(list) != 1 {
		t.Fatalf("ListJobs = %d, err %v", len(list), err)
	}

	if err := core.DeleteJob(context.Background(), "proj", "us-central1", created.Name); err != nil {
		t.Fatalf("DeleteJob: %v", err)
	}
	if _, err := core.GetJob(context.Background(), "proj", "us-central1", created.Name); !IsNotFound(err) {
		t.Fatalf("GetJob after delete err = %v, want NotFound", err)
	}
}

func TestHTTPTargetServerHeaders(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fixedClock(t, at)
	core, _, _ := newCore(t)
	ctx := context.Background()

	cases := []struct {
		name string
		t    *schedstore.HttpTarget
		want map[string]string
	}{
		{
			name: "header-less target gets the delivery User-Agent",
			t:    &schedstore.HttpTarget{URI: "http://example.test/hook", HTTPMethod: "GET"},
			want: map[string]string{"User-Agent": "Google-Cloud-Scheduler"},
		},
		{
			name: "a body adds the default Content-Type",
			t:    &schedstore.HttpTarget{URI: "http://example.test/hook", HTTPMethod: "POST", Body: []byte(`{"a":1}`)},
			want: map[string]string{"User-Agent": "Google-Cloud-Scheduler", "Content-Type": "application/octet-stream"},
		},
		{
			name: "caller headers are preserved and suppress the defaults",
			t: &schedstore.HttpTarget{
				URI:        "http://example.test/hook",
				HTTPMethod: "POST",
				Body:       []byte(`{"a":1}`),
				Headers:    map[string]string{"User-Agent": "custom-agent", "Content-Type": "application/json"},
			},
			want: map[string]string{"User-Agent": "custom-agent", "Content-Type": "application/json"},
		},
		{
			name: "a lowercase caller header still suppresses the default",
			t: &schedstore.HttpTarget{
				URI:        "http://example.test/hook",
				HTTPMethod: "POST",
				Body:       []byte(`{"a":1}`),
				Headers:    map[string]string{"content-type": "text/plain"},
			},
			want: map[string]string{"User-Agent": "Google-Cloud-Scheduler", "content-type": "text/plain"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			created, err := core.CreateJob(ctx, "proj", "us-central1", schedstore.Job{
				Schedule: "*/5 * * * *",
				TimeZone: "UTC",
				Target:   schedstore.TargetHTTP,
				HTTP:     tc.t,
			})
			if err != nil {
				t.Fatalf("CreateJob: %v", err)
			}
			if got := created.HTTP.Headers; len(got) != len(tc.want) {
				t.Fatalf("headers = %v, want %v", got, tc.want)
			}
			for k, v := range tc.want {
				if created.HTTP.Headers[k] != v {
					t.Fatalf("headers[%q] = %q, want %q (all: %v)", k, created.HTTP.Headers[k], v, created.HTTP.Headers)
				}
			}
			// The defaults are stored, so a read answers them too.
			got, err := core.GetJob(ctx, "proj", "us-central1", created.Name)
			if err != nil {
				t.Fatalf("GetJob: %v", err)
			}
			if len(got.HTTP.Headers) != len(tc.want) {
				t.Fatalf("stored headers = %v, want %v", got.HTTP.Headers, tc.want)
			}
		})
	}
}

func TestHTTPTargetHeadersAfterTargetReplacement(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fixedClock(t, at)
	core, _, _ := newCore(t)
	ctx := context.Background()

	created, err := core.CreateJob(ctx, "proj", "us-central1", schedstore.Job{
		Schedule: "*/5 * * * *",
		TimeZone: "UTC",
		Target:   schedstore.TargetHTTP,
		HTTP:     &schedstore.HttpTarget{URI: "http://example.test/hook", HTTPMethod: "GET"},
	})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	// Replacing the target through a masked update must synthesize the defaults
	// again; an update that does not touch httpTarget keeps the existing map.
	updated, err := core.UpdateJob(ctx, "proj", "us-central1", created.Name, schedstore.Job{
		Target: schedstore.TargetHTTP,
		HTTP:   &schedstore.HttpTarget{URI: "http://example.test/other", HTTPMethod: "POST", Body: []byte("hi")},
	}, []string{"httpTarget"})
	if err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	if updated.HTTP.Headers["User-Agent"] != "Google-Cloud-Scheduler" ||
		updated.HTTP.Headers["Content-Type"] != "application/octet-stream" {
		t.Fatalf("headers after replacement = %v", updated.HTTP.Headers)
	}
}

func TestCreateValidation(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fixedClock(t, at)
	core, _, _ := newCore(t)
	ctx := context.Background()

	cases := []struct {
		name string
		job  schedstore.Job
	}{
		{"missing schedule", schedstore.Job{Target: schedstore.TargetHTTP, HTTP: &schedstore.HttpTarget{URI: "http://x"}}},
		{"bad schedule", schedstore.Job{Schedule: "not a cron", Target: schedstore.TargetHTTP, HTTP: &schedstore.HttpTarget{URI: "http://x"}}},
		{"no target", schedstore.Job{Schedule: "* * * * *"}},
		{"bad uri", schedstore.Job{Schedule: "* * * * *", Target: schedstore.TargetHTTP, HTTP: &schedstore.HttpTarget{URI: "ftp://x"}}},
		{"body on GET", schedstore.Job{Schedule: "* * * * *", Target: schedstore.TargetHTTP, HTTP: &schedstore.HttpTarget{URI: "http://x", HTTPMethod: "GET", Body: []byte("x")}}},
		{"retryCount too high", schedstore.Job{Schedule: "* * * * *", Target: schedstore.TargetHTTP, HTTP: &schedstore.HttpTarget{URI: "http://x"}, RetryConfig: &schedstore.RetryConfig{RetryCount: 6}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := core.CreateJob(ctx, "proj", "us-central1", tc.job); err == nil {
				t.Fatalf("expected validation error")
			} else if perr, ok := err.(*model.ProviderError); !ok || perr.Code != "InvalidArgument" {
				t.Fatalf("err = %v, want InvalidArgument", err)
			}
		})
	}
}

func TestPauseResumeRun(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fixedClock(t, at)
	core, engine, mem := newCore(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	ctx := context.Background()

	created, err := core.CreateJob(ctx, "proj", "us-central1", schedstore.Job{
		Schedule: "* * * * *",
		TimeZone: "UTC",
		Target:   schedstore.TargetHTTP,
		HTTP:     &schedstore.HttpTarget{URI: server.URL},
	})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	paused, err := core.PauseJob(ctx, "proj", "us-central1", created.Name)
	if err != nil || paused.State != schedstore.StatePaused {
		t.Fatalf("PauseJob state = %q err %v", paused.State, err)
	}
	// A paused job is never fired, even when due.
	clock.SetGlobalClock(clock.FixedClock{T: at.Add(2 * time.Minute)})
	engine.TickNow(ctx)
	if j, _ := core.GetJob(ctx, "proj", "us-central1", created.Name); !j.LastAttemptTime.IsZero() {
		t.Fatalf("paused job fired")
	}

	resumed, err := core.ResumeJob(ctx, "proj", "us-central1", created.Name)
	if err != nil || resumed.State != schedstore.StateEnabled {
		t.Fatalf("ResumeJob state = %q err %v", resumed.State, err)
	}
	// RunJob forces a synchronous attempt and records the time.
	ran, err := core.RunJob(ctx, "proj", "us-central1", created.Name)
	if err != nil {
		t.Fatalf("RunJob: %v", err)
	}
	if ran.LastAttemptTime.IsZero() {
		t.Fatalf("RunJob did not record lastAttemptTime")
	}
	_ = mem
}

// TestPauseResumeIdempotent pins the measured Cloud Scheduler behaviour the
// Terraform provider depends on: a repeated :pause or :resume is answered with
// 200 and the unchanged job, not a FAILED_PRECONDITION. The provider issues
// :resume after every create that leaves `paused` false and :pause on every
// update that sets it, so the emulator must accept both on a job that already
// sits in the target state (TF2-12).
func TestPauseResumeIdempotent(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fixedClock(t, at)
	core, _, mem := newCore(t)
	ctx := context.Background()

	created, err := core.CreateJob(ctx, "proj", "us-central1", schedstore.Job{
		Schedule: "* * * * *",
		TimeZone: "UTC",
		Target:   schedstore.TargetHTTP,
		HTTP:     &schedstore.HttpTarget{URI: "http://example.com/hook"},
	})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	// Advancing the clock makes a no-op distinguishable from a write: a real
	// transition would stamp userUpdateTime with the new now.
	clock.SetGlobalClock(clock.FixedClock{T: at.Add(time.Minute)})

	same, err := core.ResumeJob(ctx, "proj", "us-central1", created.Name)
	if err != nil {
		t.Fatalf("resume of an ENABLED job: %v", err)
	}
	if same.State != schedstore.StateEnabled {
		t.Fatalf("state = %q, want ENABLED", same.State)
	}
	if !same.UserUpdateTime.Equal(created.UserUpdateTime) || !same.ScheduleTime.Equal(created.ScheduleTime) {
		t.Fatalf("resume of an ENABLED job changed it: userUpdateTime %v -> %v, scheduleTime %v -> %v",
			created.UserUpdateTime, same.UserUpdateTime, created.ScheduleTime, same.ScheduleTime)
	}

	paused, err := core.PauseJob(ctx, "proj", "us-central1", created.Name)
	if err != nil || paused.State != schedstore.StatePaused {
		t.Fatalf("PauseJob state = %q err %v", paused.State, err)
	}

	clock.SetGlobalClock(clock.FixedClock{T: at.Add(2 * time.Minute)})
	same, err = core.PauseJob(ctx, "proj", "us-central1", created.Name)
	if err != nil {
		t.Fatalf("pause of a PAUSED job: %v", err)
	}
	if same.State != schedstore.StatePaused || !same.UserUpdateTime.Equal(paused.UserUpdateTime) {
		t.Fatalf("pause of a PAUSED job changed it: state = %q, userUpdateTime %v -> %v",
			same.State, paused.UserUpdateTime, same.UserUpdateTime)
	}

	// The real transitions still work and a resume brings the next run back.
	resumed, err := core.ResumeJob(ctx, "proj", "us-central1", created.Name)
	if err != nil || resumed.State != schedstore.StateEnabled {
		t.Fatalf("ResumeJob state = %q err %v", resumed.State, err)
	}
	if !resumed.ScheduleTime.After(at.Add(2 * time.Minute)) {
		t.Fatalf("resume did not recompute scheduleTime: %v", resumed.ScheduleTime)
	}

	// A state the emulator never hands out (DISABLED/UPDATE_FAILED) keeps the
	// documented precondition: there is no measured real-GCP answer for whether
	// pause/resume tolerates those, so they stay FAILED_PRECONDITION.
	if err := mem.CreateJob(ctx, "proj", "us-central1", schedstore.Job{
		Name:     "disabled-job",
		State:    schedstore.StateDisabled,
		Schedule: "* * * * *",
		TimeZone: "UTC",
		Target:   schedstore.TargetHTTP,
		HTTP:     &schedstore.HttpTarget{URI: "http://example.com/hook"},
	}); err != nil {
		t.Fatalf("seed disabled job: %v", err)
	}
	for name, call := range map[string]func() (schedstore.Job, error){
		"pause":  func() (schedstore.Job, error) { return core.PauseJob(ctx, "proj", "us-central1", "disabled-job") },
		"resume": func() (schedstore.Job, error) { return core.ResumeJob(ctx, "proj", "us-central1", "disabled-job") },
	} {
		_, err := call()
		perr, ok := err.(*model.ProviderError)
		if !ok || perr.Code != "FailedPrecondition" {
			t.Fatalf("%s of a DISABLED job: err = %v, want FailedPrecondition", name, err)
		}
	}
}

func TestEngineFiresHTTPAndAdvances(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fixedClock(t, at)
	ctx := context.Background()
	mem := schedstore.NewMemoryStore()
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if got := r.Header.Get("X-CloudScheduler"); got != "true" {
			t.Errorf("X-CloudScheduler = %q", got)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	engine := NewEngine(mem, NewRunner(nil))
	core := NewService(mem)
	core.SetEngine(engine)

	created, err := core.CreateJob(ctx, "proj", "us-central1", schedstore.Job{
		Schedule: "* * * * *",
		TimeZone: "UTC",
		Target:   schedstore.TargetHTTP,
		HTTP:     &schedstore.HttpTarget{URI: server.URL, HTTPMethod: "POST", Body: []byte("hi")},
	})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if hits.Load() != 0 {
		t.Fatalf("unexpected early fire")
	}
	// Advance to the scheduled minute and tick.
	clock.SetGlobalClock(clock.FixedClock{T: at.Add(time.Minute)})
	engine.TickNow(ctx)
	if hits.Load() != 1 {
		t.Fatalf("hits = %d, want 1", hits.Load())
	}
	got, _ := core.GetJob(ctx, "proj", "us-central1", created.Name)
	if got.LastAttemptTime.IsZero() || got.Status != nil {
		t.Fatalf("after success: lastAttempt=%v status=%v", got.LastAttemptTime, got.Status)
	}
	if !got.ScheduleTime.Equal(at.Add(2 * time.Minute)) {
		t.Fatalf("next scheduleTime = %v, want %v", got.ScheduleTime, at.Add(2*time.Minute))
	}
}

func TestEngineRetryBackoff(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fixedClock(t, at)
	ctx := context.Background()
	mem := schedstore.NewMemoryStore()
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	engine := NewEngine(mem, NewRunner(nil))
	core := NewService(mem)
	core.SetEngine(engine)

	created, err := core.CreateJob(ctx, "proj", "us-central1", schedstore.Job{
		Schedule:    "0 * * * *",
		TimeZone:    "UTC",
		Target:      schedstore.TargetHTTP,
		HTTP:        &schedstore.HttpTarget{URI: server.URL},
		RetryConfig: &schedstore.RetryConfig{RetryCount: 1, MinBackoffDuration: 30 * time.Second},
	})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	// ScheduleTime is the top of the next hour.
	due := created.ScheduleTime
	clock.SetGlobalClock(clock.FixedClock{T: due})
	engine.TickNow(ctx)
	if hits.Load() != 1 {
		t.Fatalf("hits after first attempt = %d, want 1", hits.Load())
	}
	got, _ := core.GetJob(ctx, "proj", "us-central1", created.Name)
	if got.Status == nil || got.Status.Code != http.StatusInternalServerError {
		t.Fatalf("status = %+v, want 500", got.Status)
	}
	if !got.ScheduleTime.Equal(due.Add(30 * time.Second)) {
		t.Fatalf("retry scheduleTime = %v, want %v", got.ScheduleTime, due.Add(30*time.Second))
	}
	// The retry fires at +30s; after retryCount is exhausted it falls back to cron.
	clock.SetGlobalClock(clock.FixedClock{T: due.Add(30 * time.Second)})
	engine.TickNow(ctx)
	if hits.Load() != 2 {
		t.Fatalf("hits after retry = %d, want 2", hits.Load())
	}
	got, _ = core.GetJob(ctx, "proj", "us-central1", created.Name)
	if !got.ScheduleTime.Equal(due.Add(time.Hour)) {
		t.Fatalf("post-retry scheduleTime = %v, want %v", got.ScheduleTime, due.Add(time.Hour))
	}
}

func TestHTTPTargetScheduleTimeHeader(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fixedClock(t, at)
	ctx := context.Background()
	mem := schedstore.NewMemoryStore()

	var mu sync.Mutex
	var seen []http.Header
	var failFirst atomic.Bool
	failFirst.Store(true)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Header.Clone())
		mu.Unlock()
		if failFirst.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	engine := NewEngine(mem, NewRunner(nil))
	core := NewService(mem)
	core.SetEngine(engine)

	created, err := core.CreateJob(ctx, "proj", "us-central1", schedstore.Job{
		Schedule: "* * * * *",
		TimeZone: "UTC",
		Target:   schedstore.TargetHTTP,
		HTTP: &schedstore.HttpTarget{
			URI:        server.URL,
			HTTPMethod: "POST",
			// Real Cloud Scheduler replaces a user-supplied value.
			Headers: map[string]string{"X-CloudScheduler-ScheduleTime": "bogus"},
		},
		RetryConfig: &schedstore.RetryConfig{RetryCount: 1, MinBackoffDuration: 30 * time.Second},
	})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	slot := created.ScheduleTime

	// First attempt fails and schedules a retry.
	clock.SetGlobalClock(clock.FixedClock{T: slot})
	engine.TickNow(ctx)
	got, _ := core.GetJob(ctx, "proj", "us-central1", created.Name)
	if got.Status == nil || got.Status.Code != http.StatusInternalServerError {
		t.Fatalf("first attempt status = %+v, want 500", got.Status)
	}
	// Retry succeeds.
	failFirst.Store(false)
	clock.SetGlobalClock(clock.FixedClock{T: slot.Add(30 * time.Second)})
	engine.TickNow(ctx)

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 {
		t.Fatalf("deliveries = %d, want 2", len(seen))
	}
	// The header is the run's cron slot, RFC3339, and the same on the retry.
	wantSlot := slot.UTC().Format(time.RFC3339)
	wantJob := JobName("proj", "us-central1", created.Name)
	for i, h := range seen {
		if h.Get("X-CloudScheduler-ScheduleTime") != wantSlot {
			t.Errorf("attempt %d X-CloudScheduler-ScheduleTime = %q, want %q", i, h.Get("X-CloudScheduler-ScheduleTime"), wantSlot)
		}
		if h.Get("X-CloudScheduler") != "true" {
			t.Errorf("attempt %d X-CloudScheduler = %q, want true", i, h.Get("X-CloudScheduler"))
		}
		if h.Get("X-CloudScheduler-JobName") != wantJob {
			t.Errorf("attempt %d X-CloudScheduler-JobName = %q, want %q", i, h.Get("X-CloudScheduler-JobName"), wantJob)
		}
		if h.Get("User-Agent") != "Google-Cloud-Scheduler" {
			t.Errorf("attempt %d User-Agent = %q, want Google-Cloud-Scheduler", i, h.Get("User-Agent"))
		}
	}
}

func TestEngineFiresPubSub(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fixedClock(t, at)
	ctx := context.Background()
	mem := schedstore.NewMemoryStore()
	pub := &fakePublisher{}
	engine := NewEngine(mem, NewRunner(pub))
	core := NewService(mem)
	core.SetEngine(engine)

	created, err := core.CreateJob(ctx, "proj", "us-central1", schedstore.Job{
		Schedule: "* * * * *",
		TimeZone: "UTC",
		Target:   schedstore.TargetPubSub,
		PubSub:   &schedstore.PubsubTarget{TopicName: "projects/proj/topics/t", Data: []byte("payload"), Attributes: map[string]string{"k": "v"}},
	})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	clock.SetGlobalClock(clock.FixedClock{T: created.ScheduleTime})
	engine.TickNow(ctx)
	if pub.calls.Load() != 1 {
		t.Fatalf("publish calls = %d, want 1", pub.calls.Load())
	}
	if pub.topic != "projects/proj/topics/t" || string(pub.data) != "payload" || pub.attrs["k"] != "v" {
		t.Fatalf("published %q %q %v", pub.topic, pub.data, pub.attrs)
	}
}

// countingSchedDispatcher records deliveries and answers with a fixed status.
type countingSchedDispatcher struct {
	mu     sync.Mutex
	calls  int
	status schedstore.Status
}

func (d *countingSchedDispatcher) Deliver(context.Context, schedstore.Job, time.Time) schedstore.Status {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls++
	return d.status
}

func (d *countingSchedDispatcher) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

func TestEngineAppEngineRecordsUnimplemented(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fixedClock(t, at)
	ctx := context.Background()
	mem := schedstore.NewMemoryStore()
	disp := &countingSchedDispatcher{status: schedstore.Status{
		Code:    int32(codes.Unimplemented),
		Message: "appEngineHttpTarget is not delivered by the emulator (no App Engine router)",
	}}
	engine := NewEngine(mem, disp)
	core := NewService(mem)
	core.SetEngine(engine)

	created, err := core.CreateJob(ctx, "proj", "us-central1", schedstore.Job{
		Schedule:  "* * * * *",
		TimeZone:  "UTC",
		Target:    schedstore.TargetAppEngine,
		AppEngine: &schedstore.AppEngineTarget{RelativeURI: "/hook"},
		// Retries would be available, but a permanent failure must not use them.
		RetryConfig: &schedstore.RetryConfig{RetryCount: 3, MinBackoffDuration: 5 * time.Second, MaxBackoffDuration: time.Hour},
	})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	slot := created.ScheduleTime
	clock.SetGlobalClock(clock.FixedClock{T: slot})
	engine.TickNow(ctx)

	got, err := core.GetJob(ctx, "proj", "us-central1", created.Name)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if got.Status == nil || codes.Code(got.Status.Code) != codes.Unimplemented {
		t.Fatalf("appEngine status = %+v, want a recorded Unimplemented failure", got.Status)
	}
	if disp.count() != 1 {
		t.Fatalf("deliveries = %d, want exactly 1 (no retry)", disp.count())
	}
	// The next fire is the next cron slot, not a retry backoff.
	if want := nextFireFor(created, slot); !got.ScheduleTime.Equal(want) {
		t.Fatalf("next scheduleTime = %v, want the next cron slot %v", got.ScheduleTime, want)
	}

	// The backoff window (5s) has passed but the next cron slot has not: the
	// job must not be attempted again.
	clock.SetGlobalClock(clock.FixedClock{T: slot.Add(5 * time.Second)})
	engine.TickNow(ctx)
	if disp.count() != 1 {
		t.Fatalf("deliveries after the retry window = %d, want 1 (retries not consumed)", disp.count())
	}
}

func TestUpdateMasksFields(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fixedClock(t, at)
	core, _, _ := newCore(t)
	ctx := context.Background()
	created, err := core.CreateJob(ctx, "proj", "us-central1", schedstore.Job{
		Schedule:    "* * * * *",
		TimeZone:    "UTC",
		Description: "old",
		Target:      schedstore.TargetHTTP,
		HTTP:        &schedstore.HttpTarget{URI: "http://example.test/hook"},
	})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	updated, err := core.UpdateJob(ctx, "proj", "us-central1", created.Name,
		schedstore.Job{Description: "new"}, []string{"description"})
	if err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}
	if updated.Description != "new" || updated.Schedule != "* * * * *" || updated.HTTP == nil {
		t.Fatalf("masked update clobbered fields: %+v", updated)
	}
}
