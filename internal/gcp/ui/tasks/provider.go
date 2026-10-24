package tasksui

import (
	"context"

	"jaiscloud/internal/gcp/policy"
	taskscore "jaiscloud/internal/gcp/service/tasks"
	tasksstore "jaiscloud/internal/gcp/store/tasks"
)

// ProviderInterface is the subset of *tasks.Service used by the Cloud Tasks UI
// handlers. The core is transport-neutral and addressable directly, so the UI
// reuses its typed API rather than going through the REST adapter. It keeps the
// UI decoupled from the core's concrete type.
type ProviderInterface interface {
	// ListQueuesByProject lists every queue in a project across all locations,
	// sorted by location then id. It backs the location-optional console list.
	ListQueuesByProject(ctx context.Context, project string) ([]tasksstore.Queue, error)

	// Queue CRUD + lifecycle.
	GetQueue(ctx context.Context, project, location, name string) (tasksstore.Queue, error)
	CreateQueue(ctx context.Context, project, location string, q tasksstore.Queue) (tasksstore.Queue, error)
	UpdateQueue(ctx context.Context, project, location, name string, upd tasksstore.Queue, mask []string) (tasksstore.Queue, error)
	DeleteQueue(ctx context.Context, project, location, name string) error
	PauseQueue(ctx context.Context, project, location, name string) (tasksstore.Queue, error)
	ResumeQueue(ctx context.Context, project, location, name string) (tasksstore.Queue, error)
	PurgeQueue(ctx context.Context, project, location, name string) (tasksstore.Queue, error)

	// Queue IAM.
	QueueGetIamPolicy(ctx context.Context, project, location, queue string) (policy.Policy, error)
	QueueSetIamPolicy(ctx context.Context, project, location, queue string, body map[string]any) (policy.Policy, error)

	// Task CRUD + run.
	ListTasks(ctx context.Context, project, location, queue string) ([]tasksstore.Task, error)
	GetTask(ctx context.Context, project, location, queue, name string) (tasksstore.Task, error)
	CreateTask(ctx context.Context, project, location, queue string, t tasksstore.Task) (tasksstore.Task, error)
	DeleteTask(ctx context.Context, project, location, queue, name string) error
	RunTask(ctx context.Context, project, location, queue, name string) (tasksstore.Task, error)
}

// compile-time check that the core satisfies the UI seam.
var _ ProviderInterface = (*taskscore.Service)(nil)
