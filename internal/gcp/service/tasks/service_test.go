package tasks

import (
	"context"
	"errors"
	"testing"

	tasksstore "jaiscloud/internal/gcp/store/tasks"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func newService() *Service {
	return NewService(tasksstore.NewMemoryStore(), store.NewMemoryResourceStore())
}

func httpTask(name, url string) tasksstore.Task {
	return tasksstore.Task{Name: name, Target: tasksstore.TargetHTTP, HTTP: &tasksstore.HttpRequest{URL: url, HTTPMethod: "POST"}}
}

func TestServiceQueueLifecycle(t *testing.T) {
	ctx := context.Background()
	s := newService()

	q, err := s.CreateQueue(ctx, "p", "l", tasksstore.Queue{Name: "q1"})
	if err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if q.State != tasksstore.StateRunning || q.RateLimits == nil || q.RetryConfig == nil {
		t.Fatalf("defaults not applied: %+v", q)
	}
	if _, err := s.CreateQueue(ctx, "p", "l", tasksstore.Queue{Name: "q1"}); err == nil {
		t.Fatalf("duplicate queue accepted")
	}
	if _, err := s.GetQueue(ctx, "p", "l", "missing"); err == nil {
		t.Fatalf("missing queue not NotFound")
	}

	if _, err := s.PauseQueue(ctx, "p", "l", "q1"); err != nil {
		t.Fatalf("PauseQueue: %v", err)
	}
	if _, err := s.PauseQueue(ctx, "p", "l", "q1"); err == nil {
		t.Fatalf("double pause accepted")
	}
	if _, err := s.ResumeQueue(ctx, "p", "l", "q1"); err != nil {
		t.Fatalf("ResumeQueue: %v", err)
	}

	if _, err := s.UpdateQueue(ctx, "p", "l", "q1", tasksstore.Queue{RateLimits: &tasksstore.RateLimits{MaxDispatchesPerSecond: 5}}, []string{"rateLimits"}); err != nil {
		t.Fatalf("UpdateQueue: %v", err)
	}
	got, _ := s.GetQueue(ctx, "p", "l", "q1")
	if got.RateLimits.MaxDispatchesPerSecond != 5 {
		t.Fatalf("rate limit not updated: %+v", got.RateLimits)
	}
	if got.RetryConfig == nil {
		t.Fatalf("masked update cleared retryConfig")
	}

	// UpdateQueue creates a missing queue (UpsertQueue semantics).
	if _, err := s.UpdateQueue(ctx, "p", "l", "q2", tasksstore.Queue{}, []string{"rateLimits"}); err != nil {
		t.Fatalf("UpdateQueue create-if-missing: %v", err)
	}
	if _, err := s.GetQueue(ctx, "p", "l", "q2"); err != nil {
		t.Fatalf("queue not created: %v", err)
	}

	if err := s.DeleteQueue(ctx, "p", "l", "q1"); err != nil {
		t.Fatalf("DeleteQueue: %v", err)
	}
	if _, err := s.GetQueue(ctx, "p", "l", "q1"); err == nil {
		t.Fatalf("queue survived delete")
	}
}

// TestServiceQueueRateLimitDefaults pins the documented RateLimits defaults
// (TF3-4-3). RateLimits is proto3 without field presence, so a create whose
// rateLimits omits a field must read back Cloud Tasks' default for it — not 0 —
// while a caller-supplied value is echoed. The update cases pin the two mask
// semantics: `rateLimits` replaces the whole message (so its unset subfields
// take the defaults), any other mask leaves the stored message alone.
func TestServiceQueueRateLimitDefaults(t *testing.T) {
	ctx := context.Background()
	s := newService()
	full := func(rate float64, burst, conc int32) tasksstore.RateLimits {
		return tasksstore.RateLimits{MaxDispatchesPerSecond: rate, MaxBurstSize: burst, MaxConcurrentDispatches: conc}
	}
	// want asserts the queue's stored and read-back rate limits.
	want := func(t *testing.T, id string, exp tasksstore.RateLimits) tasksstore.Queue {
		t.Helper()
		got, err := s.GetQueue(ctx, "p", "l", id)
		if err != nil {
			t.Fatalf("GetQueue(%s): %v", id, err)
		}
		if got.RateLimits == nil || *got.RateLimits != exp {
			t.Fatalf("queue %s rateLimits = %+v, want %+v", id, got.RateLimits, exp)
		}
		return got
	}

	// Create: rateLimits omitted entirely, and an empty message, both take every
	// default.
	if _, err := s.CreateQueue(ctx, "p", "l", tasksstore.Queue{Name: "nil"}); err != nil {
		t.Fatalf("CreateQueue(no rateLimits): %v", err)
	}
	want(t, "nil", full(500, 100, 1000))
	if _, err := s.CreateQueue(ctx, "p", "l", tasksstore.Queue{Name: "empty", RateLimits: &tasksstore.RateLimits{}}); err != nil {
		t.Fatalf("CreateQueue(empty rateLimits): %v", err)
	}
	want(t, "empty", full(500, 100, 1000))

	// Create: a partial message keeps the supplied value and defaults the rest —
	// the TF3-4-3 reader gap (maxBurstSize read back 0 where real Cloud Tasks
	// answers 100).
	if _, err := s.CreateQueue(ctx, "p", "l", tasksstore.Queue{
		Name:       "partial",
		RateLimits: &tasksstore.RateLimits{MaxDispatchesPerSecond: 12},
	}); err != nil {
		t.Fatalf("CreateQueue(partial rateLimits): %v", err)
	}
	want(t, "partial", full(12, 100, 1000))
	if _, err := s.CreateQueue(ctx, "p", "l", tasksstore.Queue{
		Name:       "burst-only",
		RateLimits: &tasksstore.RateLimits{MaxBurstSize: 7},
	}); err != nil {
		t.Fatalf("CreateQueue(burst-only rateLimits): %v", err)
	}
	want(t, "burst-only", full(500, 7, 1000))

	// Create: every caller-supplied value is echoed unchanged.
	supplied := full(20, 7, 9)
	if _, err := s.CreateQueue(ctx, "p", "l", tasksstore.Queue{Name: "supplied", RateLimits: &supplied}); err != nil {
		t.Fatalf("CreateQueue(supplied rateLimits): %v", err)
	}
	want(t, "supplied", full(20, 7, 9))
	// The core must not mutate the caller's message through the shared pointer.
	if supplied != full(20, 7, 9) {
		t.Fatalf("CreateQueue mutated the caller's rateLimits: %+v", supplied)
	}

	// Update with a mask that does not name rateLimits preserves the stored
	// message, defaulted values included.
	if _, err := s.UpdateQueue(ctx, "p", "l", "partial", tasksstore.Queue{}, []string{"labels"}); err != nil {
		t.Fatalf("UpdateQueue(labels): %v", err)
	}
	want(t, "partial", full(12, 100, 1000))

	// Update that names rateLimits replaces the whole message: the supplied
	// rate is kept and the omitted subfields take the defaults, exactly as a
	// create does (they are not inherited from the stored message and not 0).
	if _, err := s.UpdateQueue(ctx, "p", "l", "partial", tasksstore.Queue{
		RateLimits: &tasksstore.RateLimits{MaxDispatchesPerSecond: 3},
	}, []string{"rateLimits"}); err != nil {
		t.Fatalf("UpdateQueue(rateLimits partial): %v", err)
	}
	want(t, "partial", full(3, 100, 1000))

	// A full rateLimits update is echoed as given.
	if _, err := s.UpdateQueue(ctx, "p", "l", "partial", tasksstore.Queue{
		RateLimits: &tasksstore.RateLimits{MaxDispatchesPerSecond: 25, MaxBurstSize: 30, MaxConcurrentDispatches: 7},
	}, []string{"rateLimits"}); err != nil {
		t.Fatalf("UpdateQueue(rateLimits full): %v", err)
	}
	want(t, "partial", full(25, 30, 7))

	// An update mask with no rateLimits at all (empty mask = replace every
	// mutable field) falls back to the documented defaults.
	if _, err := s.UpdateQueue(ctx, "p", "l", "supplied", tasksstore.Queue{}, nil); err != nil {
		t.Fatalf("UpdateQueue(empty mask): %v", err)
	}
	want(t, "supplied", full(500, 100, 1000))
}

func TestServiceListQueuesFilter(t *testing.T) {
	ctx := context.Background()
	s := newService()
	for _, id := range []string{"alpha", "alphabet", "beta"} {
		if _, err := s.CreateQueue(ctx, "p", "l", tasksstore.Queue{Name: id}); err != nil {
			t.Fatalf("CreateQueue(%s): %v", id, err)
		}
	}
	full := func(id string) string { return "projects/p/locations/l/queues/" + id }
	listIDs := func(filter string) []string {
		t.Helper()
		queues, err := s.ListQueues(ctx, "p", "l", filter)
		if err != nil {
			t.Fatalf("ListQueues(%q): %v", filter, err)
		}
		ids := make([]string, 0, len(queues))
		for _, q := range queues {
			ids = append(ids, q.Name)
		}
		return ids
	}
	eqStrings := func(a, b []string) bool {
		if len(a) != len(b) {
			return false
		}
		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}

	// "=" is equality on the field value: a full resource name matches exactly
	// one queue, and the bare id (not the field value) matches none.
	if got := listIDs(`name="` + full("alpha") + `"`); !eqStrings(got, []string{"alpha"}) {
		t.Errorf(`name="<full name>" listed %v, want [alpha]`, got)
	}
	if got := listIDs(`name="alpha"`); len(got) != 0 {
		t.Errorf(`name="alpha" listed %v, want none (equality is on the full name)`, got)
	}
	// ":" is the contains operator (Stackdriver syntax), not equality.
	if got := listIDs(`name:"alpha"`); !eqStrings(got, []string{"alpha", "alphabet"}) {
		t.Errorf(`name:"alpha" listed %v, want [alpha alphabet]`, got)
	}
	if got := listIDs(`name:"beta" AND state="RUNNING"`); !eqStrings(got, []string{"beta"}) {
		t.Errorf(`name:"beta" AND state="RUNNING" listed %v, want [beta]`, got)
	}

	// state is a documented Queue field; a state: filter is the Discovery
	// document's own sample.
	if got := listIDs(`state:RUNNING`); !eqStrings(got, []string{"alpha", "alphabet", "beta"}) {
		t.Errorf(`state:RUNNING listed %v, want all queues`, got)
	}
	if _, err := s.PauseQueue(ctx, "p", "l", "beta"); err != nil {
		t.Fatalf("PauseQueue: %v", err)
	}
	if got := listIDs(`state=PAUSED`); !eqStrings(got, []string{"beta"}) {
		t.Errorf("state=PAUSED listed %v, want [beta]", got)
	}
	if got := listIDs(`state:PAUS`); !eqStrings(got, []string{"beta"}) {
		t.Errorf("state:PAUS listed %v, want [beta]", got)
	}
	// purgeTime is a timestamp field; "*" tests presence, and no queue has been
	// purged yet.
	if got := listIDs(`purgeTime:*`); len(got) != 0 {
		t.Errorf("purgeTime:* listed %v, want none", got)
	}
	// An empty filter lists everything.
	if got := listIDs("  "); len(got) != 3 {
		t.Errorf("empty filter listed %v, want all three queues", got)
	}

	// A filter outside the emulator's supported subset is INVALID_ARGUMENT, never
	// silently ignored (OR/NOT and ordering comparisons are documented but not
	// implemented — see the DC32 deferral).
	for _, bad := range []string{`bogus="x"`, `name`, `state<RUNNING`, `name="alpha" OR name="beta"`} {
		_, err := s.ListQueues(ctx, "p", "l", bad)
		if err == nil {
			t.Errorf("ListQueues(%q) accepted a filter outside the supported subset", bad)
			continue
		}
		var pe *model.ProviderError
		if !errors.As(err, &pe) || pe.HTTPStatus != 400 || pe.Code != "InvalidArgument" {
			t.Errorf("ListQueues(%q) error = %v, want 400 InvalidArgument", bad, err)
		}
	}
}

func TestServiceTaskLifecycleAndPurge(t *testing.T) {
	ctx := context.Background()
	s := newService()
	_, _ = s.CreateQueue(ctx, "p", "l", tasksstore.Queue{Name: "q1"})

	tk, err := s.CreateTask(ctx, "p", "l", "q1", httpTask("", "http://example.test/hook"))
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if tk.Name == "" || tk.CreateTime.IsZero() || tk.DispatchDeadline == 0 {
		t.Fatalf("task defaults missing: %+v", tk)
	}
	if _, err := s.CreateTask(ctx, "p", "nope", "q1", httpTask("t2", "http://x")); err == nil {
		t.Fatalf("task created in missing queue")
	}
	if _, err := s.CreateTask(ctx, "p", "l", "q1", tasksstore.Task{Name: "bad"}); err == nil {
		t.Fatalf("task with no target accepted")
	}
	if _, err := s.CreateTask(ctx, "p", "l", "q1", httpTask("bad", "ftp://x")); err == nil {
		t.Fatalf("non-http url accepted")
	}

	if _, err := s.GetTask(ctx, "p", "l", "q1", tk.Name); err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if list, err := s.ListTasks(ctx, "p", "l", "q1"); err != nil || len(list) != 1 {
		t.Fatalf("ListTasks = %v, %v", list, err)
	}
	if ran, err := s.RunTask(ctx, "p", "l", "q1", tk.Name); err != nil || ran.Name != tk.Name {
		t.Fatalf("RunTask = %+v, %v", ran, err)
	}
	if _, err := s.PurgeQueue(ctx, "p", "l", "q1"); err != nil {
		t.Fatalf("PurgeQueue: %v", err)
	}
	if list, _ := s.ListTasks(ctx, "p", "l", "q1"); len(list) != 0 {
		t.Fatalf("purge left tasks: %v", list)
	}
}

func TestServiceAppEngineTask(t *testing.T) {
	ctx := context.Background()
	s := newService()
	_, _ = s.CreateQueue(ctx, "p", "l", tasksstore.Queue{Name: "q1"})
	tk := tasksstore.Task{Name: "t1", Target: tasksstore.TargetAppEngine, AppEngine: &tasksstore.AppEngineHttpRequest{RelativeURI: "/do", HTTPMethod: "POST"}}
	if _, err := s.CreateTask(ctx, "p", "l", "q1", tk); err != nil {
		t.Fatalf("CreateTask appEngine: %v", err)
	}
	bad := tasksstore.Task{Name: "t2", Target: tasksstore.TargetAppEngine, AppEngine: &tasksstore.AppEngineHttpRequest{RelativeURI: "do"}}
	if _, err := s.CreateTask(ctx, "p", "l", "q1", bad); err == nil {
		t.Fatalf("relativeUri without / accepted")
	}
}

func TestServiceQueueIAM(t *testing.T) {
	ctx := context.Background()
	s := newService()
	_, _ = s.CreateQueue(ctx, "p", "l", tasksstore.Queue{Name: "q1"})

	pol, err := s.QueueGetIamPolicy(ctx, "p", "l", "q1")
	if err != nil {
		t.Fatalf("QueueGetIamPolicy: %v", err)
	}
	if pol.Etag == "" {
		t.Fatalf("default policy missing etag")
	}
	body := map[string]any{"policy": map[string]any{"bindings": []any{map[string]any{"role": "roles/owner", "members": []any{"user:a@b"}}}}}
	if _, err := s.QueueSetIamPolicy(ctx, "p", "l", "q1", body); err != nil {
		t.Fatalf("QueueSetIamPolicy: %v", err)
	}
	perms, err := s.QueueTestIamPermissions(ctx, "p", "l", "q1", []string{"cloudtasks.queues.get"})
	if err != nil || len(perms) != 1 {
		t.Fatalf("TestIamPermissions = %v, %v", perms, err)
	}
	if _, err := s.QueueGetIamPolicy(ctx, "p", "l", "missing"); err == nil {
		t.Fatalf("IAM on missing queue accepted")
	}
}
