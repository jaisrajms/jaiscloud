package tasks

import (
	"context"
	"strconv"

	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"

	"jaiscloud/internal/gcp/policy"
	core "jaiscloud/internal/gcp/service/tasks"
)

// Provider handles the Cloud Tasks v2 REST data plane. It is a thin adapter:
// every handler resolves the NormalizedRequest params into the core's typed
// API, calls the shared core Service, and encodes the result as Discovery-shaped
// JSON.
type Provider struct {
	core           *core.Service
	defaultProject string
}

// NewProvider returns a Cloud Tasks REST provider over the shared core.
func NewProvider(c *core.Service, defaultProject string) *Provider {
	return &Provider{core: c, defaultProject: defaultProject}
}

// Routes maps "Tasks.<Action>" keys to their handlers.
func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"Tasks.QueuesList":               p.ListQueues,
		"Tasks.QueuesCreate":             p.CreateQueue,
		"Tasks.QueuesGet":                p.GetQueue,
		"Tasks.QueuesPatch":              p.UpdateQueue,
		"Tasks.QueuesDelete":             p.DeleteQueue,
		"Tasks.QueuesPause":              p.PauseQueue,
		"Tasks.QueuesResume":             p.ResumeQueue,
		"Tasks.QueuesPurge":              p.PurgeQueue,
		"Tasks.QueuesGetIamPolicy":       p.GetQueueIamPolicy,
		"Tasks.QueuesSetIamPolicy":       p.SetQueueIamPolicy,
		"Tasks.QueuesTestIamPermissions": p.TestQueueIamPermissions,
		"Tasks.TasksList":                p.ListTasks,
		"Tasks.TasksCreate":              p.CreateTask,
		"Tasks.TasksBatchCreate":         p.BatchCreateTasks,
		"Tasks.TasksBatchDelete":         p.BatchDeleteTasks,
		"Tasks.TasksGet":                 p.GetTask,
		"Tasks.TasksDelete":              p.DeleteTask,
		"Tasks.TasksRun":                 p.RunTask,
	}
}

// Reset delegates to the core so /_jaiscloud/reset clears Cloud Tasks state.
func (p *Provider) Reset(ctx context.Context) { p.core.Reset(ctx) }

func (p *Provider) ListQueues(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	queues, err := p.core.ListQueues(ctx, p.project(nr), strParam(nr, "location"), strParam(nr, "filter"))
	if err != nil {
		return nil, err
	}
	pageSize := intFrom(nr.Params["pageSize"])
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 1000
	}
	offset := pageOffset(nr)
	if offset > len(queues) {
		offset = len(queues)
	}
	end := offset + pageSize
	if end > len(queues) {
		end = len(queues)
	}
	items := make([]any, 0, end-offset)
	for _, q := range queues[offset:end] {
		items = append(items, queueToJSON(q))
	}
	resp := map[string]any{"queues": items}
	if end < len(queues) {
		resp["nextPageToken"] = strconv.Itoa(end)
	}
	return provider.OK(resp), nil
}

func (p *Provider) CreateQueue(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	q, err := queueFromBody(bodyOf(nr))
	if err != nil {
		return nil, err
	}
	created, err := p.core.CreateQueue(ctx, p.project(nr), strParam(nr, "location"), q)
	if err != nil {
		return nil, err
	}
	return provider.OK(queueToJSON(created)), nil
}

func (p *Provider) GetQueue(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	q, err := p.core.GetQueue(ctx, p.project(nr), strParam(nr, "location"), strParam(nr, "queue"))
	if err != nil {
		return nil, err
	}
	return provider.OK(queueToJSON(q)), nil
}

func (p *Provider) UpdateQueue(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	upd, err := queueFromBody(bodyOf(nr))
	if err != nil {
		return nil, err
	}
	mask := splitMask(nr.Params["updateMask"])
	q, err := p.core.UpdateQueue(ctx, p.project(nr), strParam(nr, "location"), strParam(nr, "queue"), upd, mask)
	if err != nil {
		return nil, err
	}
	return provider.OK(queueToJSON(q)), nil
}

func (p *Provider) DeleteQueue(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := p.core.DeleteQueue(ctx, p.project(nr), strParam(nr, "location"), strParam(nr, "queue")); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) PauseQueue(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	q, err := p.core.PauseQueue(ctx, p.project(nr), strParam(nr, "location"), strParam(nr, "queue"))
	if err != nil {
		return nil, err
	}
	return provider.OK(queueToJSON(q)), nil
}

func (p *Provider) ResumeQueue(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	q, err := p.core.ResumeQueue(ctx, p.project(nr), strParam(nr, "location"), strParam(nr, "queue"))
	if err != nil {
		return nil, err
	}
	return provider.OK(queueToJSON(q)), nil
}

func (p *Provider) PurgeQueue(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	q, err := p.core.PurgeQueue(ctx, p.project(nr), strParam(nr, "location"), strParam(nr, "queue"))
	if err != nil {
		return nil, err
	}
	return provider.OK(queueToJSON(q)), nil
}

func (p *Provider) GetQueueIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	pol, err := p.core.QueueGetIamPolicy(ctx, p.project(nr), strParam(nr, "location"), strParam(nr, "queue"))
	if err != nil {
		return nil, err
	}
	return provider.OK(policyMap(pol)), nil
}

func (p *Provider) SetQueueIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	pol, err := p.core.QueueSetIamPolicy(ctx, p.project(nr), strParam(nr, "location"), strParam(nr, "queue"), bodyOf(nr))
	if err != nil {
		return nil, err
	}
	return provider.OK(policyMap(pol)), nil
}

func (p *Provider) TestQueueIamPermissions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	perms := permissionList(bodyOf(nr))
	granted, err := p.core.QueueTestIamPermissions(ctx, p.project(nr), strParam(nr, "location"), strParam(nr, "queue"), perms)
	if err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{"permissions": granted}), nil
}

func (p *Provider) ListTasks(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	tasks, err := p.core.ListTasks(ctx, p.project(nr), strParam(nr, "location"), strParam(nr, "queue"))
	if err != nil {
		return nil, err
	}
	pageSize := intFrom(nr.Params["pageSize"])
	if pageSize <= 0 || pageSize > 1000 {
		pageSize = 1000
	}
	offset := pageOffset(nr)
	if offset > len(tasks) {
		offset = len(tasks)
	}
	end := offset + pageSize
	if end > len(tasks) {
		end = len(tasks)
	}
	items := make([]any, 0, end-offset)
	for _, t := range tasks[offset:end] {
		items = append(items, taskToJSON(t))
	}
	resp := map[string]any{"tasks": items}
	if end < len(tasks) {
		resp["nextPageToken"] = strconv.Itoa(end)
	}
	return provider.OK(resp), nil
}

func (p *Provider) CreateTask(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	t, err := taskFromBody(bodyOf(nr))
	if err != nil {
		return nil, err
	}
	created, err := p.core.CreateTask(ctx, p.project(nr), strParam(nr, "location"), strParam(nr, "queue"), t)
	if err != nil {
		return nil, err
	}
	return provider.OK(taskToJSON(created)), nil
}

// BatchCreateTasks handles POST .../tasks:batchCreate.
func (p *Provider) BatchCreateTasks(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body := bodyOf(nr)
	reqs, _ := body["requests"].([]any)
	created := make([]any, 0, len(reqs))
	for _, raw := range reqs {
		req, _ := raw.(map[string]any)
		taskBody, _ := req["task"].(map[string]any)
		q := strParam(nr, "queue")
		if parent := str(req, "parent"); parent != "" {
			if _, _, pq, ok := core.ParseTaskParent(parent); ok {
				q = pq
			}
		}
		t, err := taskFromBody(taskBody)
		if err != nil {
			return nil, err
		}
		tk, err := p.core.CreateTask(ctx, p.project(nr), strParam(nr, "location"), q, t)
		if err != nil {
			return nil, err
		}
		created = append(created, taskToJSON(tk))
	}
	return provider.OK(map[string]any{"tasks": created}), nil
}

// BatchDeleteTasks handles POST .../tasks:batchDelete.
func (p *Provider) BatchDeleteTasks(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	body := bodyOf(nr)
	names, _ := body["names"].([]any)
	for _, raw := range names {
		name, _ := raw.(string)
		if name == "" {
			continue
		}
		project, location, queue, task, ok := core.ParseTaskName(name)
		if !ok {
			return nil, model.NewProviderError("InvalidArgument", "invalid task name: "+name, 400)
		}
		if err := p.core.DeleteTask(ctx, project, location, queue, task); err != nil {
			return nil, err
		}
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) GetTask(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	t, err := p.core.GetTask(ctx, p.project(nr), strParam(nr, "location"), strParam(nr, "queue"), strParam(nr, "task"))
	if err != nil {
		return nil, err
	}
	return provider.OK(taskToJSON(t)), nil
}

func (p *Provider) DeleteTask(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	if err := p.core.DeleteTask(ctx, p.project(nr), strParam(nr, "location"), strParam(nr, "queue"), strParam(nr, "task")); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

// RunTask forces the task to run now and returns the dispatched task.
func (p *Provider) RunTask(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	t, err := p.core.RunTask(ctx, p.project(nr), strParam(nr, "location"), strParam(nr, "queue"), strParam(nr, "task"))
	if err != nil {
		return nil, err
	}
	return provider.OK(taskToJSON(t)), nil
}

// --- helpers ---

func (p *Provider) project(nr *model.NormalizedRequest) string {
	if proj := strParam(nr, "project"); proj != "" {
		return proj
	}
	return p.defaultProject
}

func pageOffset(nr *model.NormalizedRequest) int {
	offset := 0
	if t := strParam(nr, "pageToken"); t != "" {
		offset, _ = strconv.Atoi(t)
	}
	if offset < 0 {
		offset = 0
	}
	return offset
}

func policyMap(pol policy.Policy) map[string]any {
	if pol.Bindings == nil {
		pol.Bindings = []any{}
	}
	return map[string]any{"version": pol.Version, "etag": pol.Etag, "bindings": pol.Bindings}
}

func permissionList(body map[string]any) []string {
	items, _ := body["permissions"].([]any)
	out := make([]string, 0, len(items))
	for _, it := range items {
		if s, ok := it.(string); ok {
			out = append(out, s)
		}
	}
	return out
}
