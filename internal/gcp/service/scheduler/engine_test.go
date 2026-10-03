package scheduler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

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
	if _, err := core.PauseJob(ctx, "proj", "us-central1", created.Name); err == nil {
		t.Fatalf("double pause should fail")
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

func TestEngineAppEngineRecordsUnimplemented(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fixedClock(t, at)
	ctx := context.Background()
	mem := schedstore.NewMemoryStore()
	engine := NewEngine(mem, NewRunner(nil))
	core := NewService(mem)
	core.SetEngine(engine)

	created, err := core.CreateJob(ctx, "proj", "us-central1", schedstore.Job{
		Schedule:  "* * * * *",
		TimeZone:  "UTC",
		Target:    schedstore.TargetAppEngine,
		AppEngine: &schedstore.AppEngineTarget{RelativeURI: "/hook"},
	})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	clock.SetGlobalClock(clock.FixedClock{T: created.ScheduleTime})
	engine.TickNow(ctx)
	got, _ := core.GetJob(ctx, "proj", "us-central1", created.Name)
	if got.Status == nil || got.Status.Code == 0 {
		t.Fatalf("appEngine status = %+v, want a recorded failure", got.Status)
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
