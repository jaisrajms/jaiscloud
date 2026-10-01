package tasks

import (
	"context"
	"testing"

	core "jaiscloud/internal/gcp/service/tasks"
	tasksstore "jaiscloud/internal/gcp/store/tasks"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func newProvider(t *testing.T) *Provider {
	t.Helper()
	c := core.NewService(tasksstore.NewMemoryStore(), store.NewMemoryResourceStore())
	return NewProvider(c, "p")
}

func req(params map[string]any) *model.NormalizedRequest {
	return &model.NormalizedRequest{Service: ServiceName, Params: params}
}

func queueBody() map[string]any {
	return map[string]any{"name": "projects/p/locations/l/queues/q1"}
}

func taskBody() map[string]any {
	return map[string]any{"httpRequest": map[string]any{"url": "http://example.test/hook", "httpMethod": "POST"}}
}

func TestProviderQueueLifecycle(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)

	if _, err := p.CreateQueue(ctx, req(map[string]any{"project": "p", "location": "l", "body": queueBody()})); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if _, err := p.GetQueue(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1"})); err != nil {
		t.Fatalf("GetQueue: %v", err)
	}
	resp, err := p.ListQueues(ctx, req(map[string]any{"project": "p", "location": "l"}))
	if err != nil {
		t.Fatalf("ListQueues: %v", err)
	}
	if got := resp.Data["queues"].([]any); len(got) != 1 {
		t.Fatalf("queues = %v", resp.Data["queues"])
	}
	if _, err := p.PauseQueue(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1"})); err != nil {
		t.Fatalf("PauseQueue: %v", err)
	}
	if _, err := p.ResumeQueue(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1"})); err != nil {
		t.Fatalf("ResumeQueue: %v", err)
	}
	if _, err := p.PurgeQueue(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1"})); err != nil {
		t.Fatalf("PurgeQueue: %v", err)
	}
	if _, err := p.UpdateQueue(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1", "body": map[string]any{"rateLimits": map[string]any{"maxDispatchesPerSecond": float64(9)}}, "updateMask": "rateLimits"})); err != nil {
		t.Fatalf("UpdateQueue: %v", err)
	}
	if _, err := p.DeleteQueue(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1"})); err != nil {
		t.Fatalf("DeleteQueue: %v", err)
	}
	if _, err := p.GetQueue(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1"})); err == nil {
		t.Fatalf("GetQueue after delete: expected NotFound")
	}
}

func TestProviderTaskLifecycleAndBatch(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	if _, err := p.CreateQueue(ctx, req(map[string]any{"project": "p", "location": "l", "body": queueBody()})); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	created, err := p.CreateTask(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1", "body": taskBody()}))
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	name, _ := created.Data["name"].(string)
	if name == "" {
		t.Fatalf("task name missing: %v", created.Data)
	}

	resp, err := p.ListTasks(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1"}))
	if err != nil || len(resp.Data["tasks"].([]any)) != 1 {
		t.Fatalf("ListTasks = %v, %v", resp, err)
	}

	// Batch create.
	batch, err := p.BatchCreateTasks(ctx, req(map[string]any{
		"project": "p", "location": "l", "queue": "q1",
		"body": map[string]any{"requests": []any{
			map[string]any{"parent": "projects/p/locations/l/queues/q1", "task": map[string]any{"httpRequest": map[string]any{"url": "http://example.test/b", "httpMethod": "POST"}}},
		}},
	}))
	if err != nil {
		t.Fatalf("BatchCreateTasks: %v", err)
	}
	if len(batch.Data["tasks"].([]any)) != 1 {
		t.Fatalf("batch = %v", batch.Data)
	}

	// Batch delete the just-created task by full name.
	batchName, _ := batch.Data["tasks"].([]any)[0].(map[string]any)["name"].(string)
	if _, err := p.BatchDeleteTasks(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1", "body": map[string]any{"names": []any{batchName}}})); err != nil {
		t.Fatalf("BatchDeleteTasks: %v", err)
	}

	if _, err := p.GetTask(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1", "task": taskNameOf(t, name)})); err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if ran, err := p.RunTask(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1", "task": taskNameOf(t, name)})); err != nil {
		t.Fatalf("RunTask: %v", err)
	} else if ran.Data["name"] != name {
		t.Fatalf("RunTask name = %v, want %v", ran.Data["name"], name)
	}
	if _, err := p.DeleteTask(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1", "task": taskNameOf(t, name)})); err != nil {
		t.Fatalf("DeleteTask: %v", err)
	}
}

func TestProviderQueueIAM(t *testing.T) {
	ctx := context.Background()
	p := newProvider(t)
	if _, err := p.CreateQueue(ctx, req(map[string]any{"project": "p", "location": "l", "body": queueBody()})); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if _, err := p.GetQueueIamPolicy(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1"})); err != nil {
		t.Fatalf("GetQueueIamPolicy: %v", err)
	}
	body := map[string]any{"policy": map[string]any{"bindings": []any{map[string]any{"role": "roles/owner", "members": []any{"user:a@b"}}}}}
	if _, err := p.SetQueueIamPolicy(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1", "body": body})); err != nil {
		t.Fatalf("SetQueueIamPolicy: %v", err)
	}
	resp, err := p.TestQueueIamPermissions(ctx, req(map[string]any{"project": "p", "location": "l", "queue": "q1", "body": map[string]any{"permissions": []any{"cloudtasks.queues.get"}}}))
	if err != nil || len(resp.Data["permissions"].([]string)) != 1 {
		t.Fatalf("TestQueueIamPermissions = %v, %v", resp, err)
	}
}

func taskNameOf(t *testing.T, full string) string {
	t.Helper()
	_, _, _, id, ok := core.ParseTaskName(full)
	if !ok {
		t.Fatalf("bad task name %q", full)
	}
	return id
}
