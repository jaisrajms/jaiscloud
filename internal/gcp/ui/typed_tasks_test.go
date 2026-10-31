package ui

import (
	"context"
	"reflect"
	"testing"

	"jaiscloud/internal/gcp/service/tasks"
	tasksstore "jaiscloud/internal/gcp/store/tasks"
	resttasks "jaiscloud/internal/gcp/transport/rest/tasks"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

func newTasks() (*tasks.Service, map[string]provider.HandlerFunc) {
	core := tasks.NewService(tasksstore.NewMemoryStore(), store.NewMemoryResourceStore())
	return core, resttasks.NewProvider(core, "proj").Routes()
}

func TestTasksConsolePassthroughMatchesDirect(t *testing.T) {
	core, builtins := newTasks()
	var seen []seenCall
	wrap := map[string]func(provider.HandlerFunc) provider.HandlerFunc{}
	for k := range builtins {
		wrap[k] = passthrough(&seen, k)
	}
	p := routeTasks(typedFixture(builtins, wrap), core)
	ctx := context.Background()

	q, err := p.CreateQueue(ctx, "proj", "us-central1", tasksstore.Queue{Name: "q1"})
	if err != nil || q.Name != "q1" || q.ProjectID != "proj" || q.Location != "us-central1" {
		t.Fatalf("CreateQueue = %+v, %v", q, err)
	}
	direct, err := core.GetQueue(ctx, "proj", "us-central1", "q1")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := p.GetQueue(ctx, "proj", "us-central1", "q1"); err != nil || !reflect.DeepEqual(got, direct) {
		t.Fatalf("queue through the chain:\n  %+v\ndirect:\n  %+v\n%v", got, direct, err)
	}
	if list, err := p.ListQueuesByProject(ctx, "proj"); err != nil || len(list) != 1 || !reflect.DeepEqual(list[0], direct) {
		t.Fatalf("ListQueuesByProject = %+v, %v", list, err)
	}
	if got, err := p.PauseQueue(ctx, "proj", "us-central1", "q1"); err != nil || got.State != tasksstore.StatePaused {
		t.Fatalf("PauseQueue = %+v, %v", got, err)
	}
	if got, err := p.ResumeQueue(ctx, "proj", "us-central1", "q1"); err != nil || got.State != tasksstore.StateRunning {
		t.Fatalf("ResumeQueue = %+v, %v", got, err)
	}
	if _, err := p.UpdateQueue(ctx, "proj", "us-central1", "q1", tasksstore.Queue{RateLimits: &tasksstore.RateLimits{MaxDispatchesPerSecond: 7}}, []string{"rateLimits"}); err != nil {
		t.Fatalf("UpdateQueue: %v", err)
	}
	if pol, err := p.QueueGetIamPolicy(ctx, "proj", "us-central1", "q1"); err != nil {
		t.Fatalf("QueueGetIamPolicy = %+v, %v", pol, err)
	}

	task, err := p.CreateTask(ctx, "proj", "us-central1", "q1", tasksstore.Task{Target: tasksstore.TargetHTTP, HTTP: &tasksstore.HttpRequest{URL: "https://example.com/t", HTTPMethod: "POST"}})
	if err != nil || task.Name == "" || task.Queue != "q1" {
		t.Fatalf("CreateTask = %+v, %v", task, err)
	}
	directTask, err := core.GetTask(ctx, "proj", "us-central1", "q1", task.Name)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := p.GetTask(ctx, "proj", "us-central1", "q1", task.Name); err != nil || !reflect.DeepEqual(got, directTask) {
		t.Fatalf("task through the chain:\n  %+v\ndirect:\n  %+v\n%v", got, directTask, err)
	}
	if list, err := p.ListTasks(ctx, "proj", "us-central1", "q1"); err != nil || len(list) != 1 || !reflect.DeepEqual(list[0], directTask) {
		t.Fatalf("ListTasks = %+v, %v", list, err)
	}
	if err := p.DeleteTask(ctx, "proj", "us-central1", "q1", task.Name); err != nil {
		t.Fatalf("DeleteTask: %v", err)
	}
	if _, err := p.PurgeQueue(ctx, "proj", "us-central1", "q1"); err != nil {
		t.Fatalf("PurgeQueue: %v", err)
	}
	if err := p.DeleteQueue(ctx, "proj", "us-central1", "q1"); err != nil {
		t.Fatalf("DeleteQueue: %v", err)
	}
	for _, s := range seen {
		if s.origin != model.OriginUI {
			t.Errorf("%s: plugin saw origin %q, want ui", s.key, s.origin)
		}
	}
	if len(seen) != 13 {
		t.Fatalf("the plugin saw only %d calls", len(seen))
	}
}

func TestTasksConsoleShowsWhatThePluginServes(t *testing.T) {
	core, builtins := newTasks()
	p := routeTasks(typedFixture(builtins, map[string]func(provider.HandlerFunc) provider.HandlerFunc{
		"Tasks.QueuesList": replaceWith(map[string]any{"queues": []any{map[string]any{
			"name": "projects/proj/locations/europe-west1/queues/from-plugin", "state": "PAUSED"}}}),
	}), core)
	qs, err := p.ListQueuesByProject(context.Background(), "proj")
	if err != nil || len(qs) != 1 {
		t.Fatalf("ListQueuesByProject = %+v, %v", qs, err)
	}
	if q := qs[0]; q.Name != "from-plugin" || q.Location != "europe-west1" || q.ProjectID != "proj" || q.State != tasksstore.StatePaused {
		t.Fatalf("the console must show the plugin's queue, got %+v", q)
	}
}
