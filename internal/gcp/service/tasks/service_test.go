package tasks

import (
	"context"
	"testing"

	tasksstore "jaiscloud/internal/gcp/store/tasks"
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
