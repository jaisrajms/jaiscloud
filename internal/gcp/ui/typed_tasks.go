package ui

import (
	"context"
	"encoding/json"
	"strings"

	"jaiscloud/internal/gcp/policy"
	tasksstore "jaiscloud/internal/gcp/store/tasks"
	resttasks "jaiscloud/internal/gcp/transport/rest/tasks"
	tasksui "jaiscloud/internal/gcp/ui/tasks"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
)

// typedTasks serves the Cloud Tasks console through the plugin chain when a
// plugin covers the key (see typed.go).
type typedTasks struct {
	tasksui.ProviderInterface
	c typedCaller
}

func routeTasks(c typedCaller, p tasksui.ProviderInterface) tasksui.ProviderInterface {
	return typedTasks{ProviderInterface: p, c: c}
}

func decodeQueue(m map[string]any) (tasksstore.Queue, error) {
	q, err := decodeTyped(m, resttasks.QueueFromBody)
	if err != nil {
		return q, err
	}
	if n, _ := m["name"].(string); n != "" {
		p := nameParts(n)
		q.ProjectID, q.Location = p["projects"], p["locations"]
	}
	return q, nil
}

func decodeTask(m map[string]any) (tasksstore.Task, error) {
	t, err := decodeTyped(m, resttasks.TaskFromBody)
	if err != nil {
		return t, err
	}
	if n, _ := m["name"].(string); n != "" {
		p := nameParts(n)
		t.ProjectID, t.Location, t.Queue = p["projects"], p["locations"], p["queues"]
	}
	return t, nil
}

func decodePolicy(m map[string]any) (policy.Policy, error) {
	var pol policy.Policy
	b, err := json.Marshal(m)
	if err != nil {
		return pol, err
	}
	err = json.Unmarshal(b, &pol)
	return pol, err
}

func (p typedTasks) ListQueuesByProject(ctx context.Context, project string) ([]tasksstore.Queue, error) {
	all := func(ctx context.Context, _ *model.NormalizedRequest) (*model.ProviderResponse, error) {
		qs, err := p.ProviderInterface.ListQueuesByProject(ctx, project)
		if err != nil {
			return nil, err
		}
		out := make([]any, 0, len(qs))
		for _, q := range qs {
			out = append(out, resttasks.QueueToJSON(q))
		}
		return provider.OK(map[string]any{"queues": out}), nil
	}
	data, covered, err := p.c.do(ctx, "Tasks.QueuesList", project, "", map[string]any{"project": project, "location": "-"}, all)
	if !covered {
		return p.ProviderInterface.ListQueuesByProject(ctx, project)
	}
	if err != nil {
		return nil, err
	}
	var out []tasksstore.Queue
	for _, m := range items(data, "queues") {
		q, err := decodeQueue(m)
		if err != nil {
			return nil, err
		}
		out = append(out, q)
	}
	return out, nil
}

func (p typedTasks) queue(ctx context.Context, key, project, location, name string, direct func() (tasksstore.Queue, error)) (tasksstore.Queue, error) {
	data, covered, err := p.c.do(ctx, key, project, location, map[string]any{"project": project, "location": location, "queue": name}, nil)
	if !covered {
		return direct()
	}
	if err != nil {
		return tasksstore.Queue{}, err
	}
	return decodeQueue(data)
}

func (p typedTasks) GetQueue(ctx context.Context, project, location, name string) (tasksstore.Queue, error) {
	return p.queue(ctx, "Tasks.QueuesGet", project, location, name, func() (tasksstore.Queue, error) {
		return p.ProviderInterface.GetQueue(ctx, project, location, name)
	})
}

func (p typedTasks) PauseQueue(ctx context.Context, project, location, name string) (tasksstore.Queue, error) {
	return p.queue(ctx, "Tasks.QueuesPause", project, location, name, func() (tasksstore.Queue, error) {
		return p.ProviderInterface.PauseQueue(ctx, project, location, name)
	})
}

func (p typedTasks) ResumeQueue(ctx context.Context, project, location, name string) (tasksstore.Queue, error) {
	return p.queue(ctx, "Tasks.QueuesResume", project, location, name, func() (tasksstore.Queue, error) {
		return p.ProviderInterface.ResumeQueue(ctx, project, location, name)
	})
}

func (p typedTasks) PurgeQueue(ctx context.Context, project, location, name string) (tasksstore.Queue, error) {
	return p.queue(ctx, "Tasks.QueuesPurge", project, location, name, func() (tasksstore.Queue, error) {
		return p.ProviderInterface.PurgeQueue(ctx, project, location, name)
	})
}

func (p typedTasks) DeleteQueue(ctx context.Context, project, location, name string) error {
	_, covered, err := p.c.do(ctx, "Tasks.QueuesDelete", project, location, map[string]any{"project": project, "location": location, "queue": name}, nil)
	if !covered {
		return p.ProviderInterface.DeleteQueue(ctx, project, location, name)
	}
	return err
}

func (p typedTasks) CreateQueue(ctx context.Context, project, location string, q tasksstore.Queue) (tasksstore.Queue, error) {
	data, covered, err := p.c.do(ctx, "Tasks.QueuesCreate", project, location,
		map[string]any{"project": project, "location": location, "body": resttasks.QueueToJSON(q)}, nil)
	if !covered {
		return p.ProviderInterface.CreateQueue(ctx, project, location, q)
	}
	if err != nil {
		return tasksstore.Queue{}, err
	}
	return decodeQueue(data)
}

func (p typedTasks) UpdateQueue(ctx context.Context, project, location, name string, upd tasksstore.Queue, mask []string) (tasksstore.Queue, error) {
	upd.ProjectID, upd.Location, upd.Name = project, location, name // the body names the queue it patches
	params := map[string]any{"project": project, "location": location, "queue": name, "body": resttasks.QueueToJSON(upd)}
	if len(mask) > 0 {
		params["updateMask"] = strings.Join(mask, ",")
	}
	data, covered, err := p.c.do(ctx, "Tasks.QueuesPatch", project, location, params, nil)
	if !covered {
		return p.ProviderInterface.UpdateQueue(ctx, project, location, name, upd, mask)
	}
	if err != nil {
		return tasksstore.Queue{}, err
	}
	return decodeQueue(data)
}

func (p typedTasks) QueueGetIamPolicy(ctx context.Context, project, location, queue string) (policy.Policy, error) {
	data, covered, err := p.c.do(ctx, "Tasks.QueuesGetIamPolicy", project, location, map[string]any{"project": project, "location": location, "queue": queue}, nil)
	if !covered {
		return p.ProviderInterface.QueueGetIamPolicy(ctx, project, location, queue)
	}
	if err != nil {
		return policy.Policy{}, err
	}
	return decodePolicy(data)
}

func (p typedTasks) QueueSetIamPolicy(ctx context.Context, project, location, queue string, body map[string]any) (policy.Policy, error) {
	data, covered, err := p.c.do(ctx, "Tasks.QueuesSetIamPolicy", project, location, map[string]any{"project": project, "location": location, "queue": queue, "body": body}, nil)
	if !covered {
		return p.ProviderInterface.QueueSetIamPolicy(ctx, project, location, queue, body)
	}
	if err != nil {
		return policy.Policy{}, err
	}
	return decodePolicy(data)
}

func (p typedTasks) ListTasks(ctx context.Context, project, location, queue string) ([]tasksstore.Task, error) {
	data, covered, err := p.c.do(ctx, "Tasks.TasksList", project, location, map[string]any{"project": project, "location": location, "queue": queue}, nil)
	if !covered {
		return p.ProviderInterface.ListTasks(ctx, project, location, queue)
	}
	if err != nil {
		return nil, err
	}
	var out []tasksstore.Task
	for _, m := range items(data, "tasks") {
		t, err := decodeTask(m)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, nil
}

func (p typedTasks) task(ctx context.Context, key, project, location, queue, name string, direct func() (tasksstore.Task, error)) (tasksstore.Task, error) {
	data, covered, err := p.c.do(ctx, key, project, location, map[string]any{"project": project, "location": location, "queue": queue, "task": name}, nil)
	if !covered {
		return direct()
	}
	if err != nil {
		return tasksstore.Task{}, err
	}
	return decodeTask(data)
}

func (p typedTasks) GetTask(ctx context.Context, project, location, queue, name string) (tasksstore.Task, error) {
	return p.task(ctx, "Tasks.TasksGet", project, location, queue, name, func() (tasksstore.Task, error) {
		return p.ProviderInterface.GetTask(ctx, project, location, queue, name)
	})
}

func (p typedTasks) RunTask(ctx context.Context, project, location, queue, name string) (tasksstore.Task, error) {
	return p.task(ctx, "Tasks.TasksRun", project, location, queue, name, func() (tasksstore.Task, error) {
		return p.ProviderInterface.RunTask(ctx, project, location, queue, name)
	})
}

func (p typedTasks) DeleteTask(ctx context.Context, project, location, queue, name string) error {
	_, covered, err := p.c.do(ctx, "Tasks.TasksDelete", project, location, map[string]any{"project": project, "location": location, "queue": queue, "task": name}, nil)
	if !covered {
		return p.ProviderInterface.DeleteTask(ctx, project, location, queue, name)
	}
	return err
}

func (p typedTasks) CreateTask(ctx context.Context, project, location, queue string, t tasksstore.Task) (tasksstore.Task, error) {
	body := resttasks.TaskToJSON(t)
	if t.Name == "" {
		delete(body, "name") // the service names a task the caller did not
	}
	data, covered, err := p.c.do(ctx, "Tasks.TasksCreate", project, location,
		map[string]any{"project": project, "location": location, "queue": queue, "body": map[string]any{"task": body}}, nil)
	if !covered {
		return p.ProviderInterface.CreateTask(ctx, project, location, queue, t)
	}
	if err != nil {
		return tasksstore.Task{}, err
	}
	return decodeTask(data)
}
