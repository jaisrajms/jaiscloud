package tasks

import (
	"context"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	tasksstore "jaiscloud/internal/gcp/store/tasks"
)

var (
	validMethods    = map[string]bool{"": true, "POST": true, "GET": true, "HEAD": true, "PUT": true, "DELETE": true, "PATCH": true, "OPTIONS": true}
	bodyMethods     = map[string]bool{"POST": true, "PUT": true, "PATCH": true}
	appEngineMethod = map[string]bool{"": true, "POST": true, "GET": true, "HEAD": true, "PUT": true, "DELETE": true}
)

const defaultDispatchDeadline = 10 * time.Minute

// CreateTask validates and stores a new task under an existing queue. t.Name is
// the task id; when empty a random id is generated.
func (s *Service) CreateTask(ctx context.Context, project, location, queue string, t tasksstore.Task) (tasksstore.Task, error) {
	if project == "" || location == "" || queue == "" {
		return tasksstore.Task{}, invalidArgument("project, location and queue are required")
	}
	if _, err := s.store.GetQueue(ctx, project, location, queue); err != nil {
		return tasksstore.Task{}, mapStoreErr(err)
	}
	if t.Name == "" {
		t.Name = newTaskID()
	}
	if !taskIDPattern.MatchString(t.Name) || len(t.Name) > 500 {
		return tasksstore.Task{}, invalidArgument("invalid task id")
	}
	if err := validateTask(&t); err != nil {
		return tasksstore.Task{}, err
	}
	now := clock.Now()
	if t.ScheduleTime.IsZero() {
		t.ScheduleTime = now
	}
	if t.DispatchDeadline == 0 {
		t.DispatchDeadline = defaultDispatchDeadline
	}
	t.CreateTime = now
	t.DispatchCount = 0
	t.ResponseCount = 0
	t.FirstAttempt = nil
	t.LastAttempt = nil
	if err := s.store.CreateTask(ctx, project, location, queue, t); err != nil {
		return tasksstore.Task{}, mapStoreErr(err)
	}
	t.ProjectID = project
	t.Location = location
	t.Queue = queue
	return t, nil
}

// GetTask returns a task by id.
func (s *Service) GetTask(ctx context.Context, project, location, queue, name string) (tasksstore.Task, error) {
	t, err := s.store.GetTask(ctx, project, location, queue, name)
	if err != nil {
		return tasksstore.Task{}, mapStoreErr(err)
	}
	return t, nil
}

// ListTasks lists tasks under a queue, sorted by id.
func (s *Service) ListTasks(ctx context.Context, project, location, queue string) ([]tasksstore.Task, error) {
	if _, err := s.store.GetQueue(ctx, project, location, queue); err != nil {
		return nil, mapStoreErr(err)
	}
	tasks, err := s.store.ListTasks(ctx, project, location, queue)
	if err != nil {
		return nil, mapStoreErr(err)
	}
	return tasks, nil
}

// DeleteTask removes a task.
func (s *Service) DeleteTask(ctx context.Context, project, location, queue, name string) error {
	if err := s.store.DeleteTask(ctx, project, location, queue, name); err != nil {
		return mapStoreErr(err)
	}
	return nil
}

// HttpBody is the google.api.HttpBody payload a `tasks:buffer` request carries:
// a content type and raw bytes. The emulator has no queue-level `httpTarget`
// (the field the real service resolves the delivery URL from), so a buffer
// request cannot be turned into a deliverable task on its own — see BufferTask.
type HttpBody struct {
	ContentType string
	Data        []byte
}

// BufferTask creates a task from a `tasks:buffer` request
// (POST {queue}/tasks/{taskId}:buffer).
//
// Real Cloud Tasks buffers the request body onto the queue's HTTP target: the
// queue must have an `httpTarget`, and the buffered body becomes that target's
// body. The emulator does not model queue-level `httpTarget`, so there is no
// URL to resolve. Two shapes are accepted:
//
//   - a full task (the emulator's documented limitation): the caller passes the
//     task it wants created, and BufferTask behaves exactly like CreateTask with
//     the path's task id (when supplied);
//   - a bare HttpBody with no task: rejected with INVALID_ARGUMENT, which is the
//     error real Cloud Tasks returns for a queue without an HTTP target.
//
// The bucket is REST-only: `BufferTask` is declared by the REST Discovery
// document but the service proto declares no such RPC (docs/gcp/wire-contract.md
// §5).
func (s *Service) BufferTask(ctx context.Context, project, location, queue, taskID string, t tasksstore.Task, body *HttpBody) (tasksstore.Task, error) {
	if taskID != "" {
		t.Name = taskID
	}
	if t.Target == "" {
		if body != nil {
			return tasksstore.Task{}, invalidArgument(
				"queue has no httpTarget: the emulator does not model queue-level httpTarget, so tasks:buffer requires a full task in the request body")
		}
		return tasksstore.Task{}, invalidArgument("exactly one of httpRequest or appEngineHttpRequest is required")
	}
	return s.CreateTask(ctx, project, location, queue, t)
}

// RunTask forces the task to run now. It bypasses the queue's paused state and
// rate limits, matching the Cloud Tasks contract.
func (s *Service) RunTask(ctx context.Context, project, location, queue, name string) (tasksstore.Task, error) {
	t, err := s.store.GetTask(ctx, project, location, queue, name)
	if err != nil {
		return tasksstore.Task{}, mapStoreErr(err)
	}
	if s.engine == nil {
		return t, nil
	}
	return s.engine.runNow(ctx, t)
}

// --- validation ---

func validateTask(t *tasksstore.Task) error {
	if t.RetryConfig != nil {
		if err := validateRetryConfig(t.RetryConfig); err != nil {
			return err
		}
	}
	switch t.Target {
	case tasksstore.TargetHTTP:
		if t.HTTP == nil {
			return invalidArgument("httpRequest is required")
		}
		return validateHTTPRequest(t.HTTP)
	case tasksstore.TargetAppEngine:
		if t.AppEngine == nil {
			return invalidArgument("appEngineHttpRequest is required")
		}
		return validateAppEngineRequest(t.AppEngine)
	default:
		return invalidArgument("exactly one of httpRequest or appEngineHttpRequest is required")
	}
}

func validateHTTPRequest(r *tasksstore.HttpRequest) error {
	if !strings.HasPrefix(r.URL, "http://") && !strings.HasPrefix(r.URL, "https://") {
		return invalidArgument("httpRequest.url must begin with http:// or https://")
	}
	if !validMethods[r.HTTPMethod] {
		return invalidArgument("invalid httpRequest.httpMethod")
	}
	if len(r.Body) > 0 && r.HTTPMethod != "" && !bodyMethods[r.HTTPMethod] {
		return invalidArgument("httpRequest.body is only allowed for POST, PUT, or PATCH")
	}
	return nil
}

func validateAppEngineRequest(r *tasksstore.AppEngineHttpRequest) error {
	if r.RelativeURI != "" && !strings.HasPrefix(r.RelativeURI, "/") {
		return invalidArgument("appEngineHttpRequest.relativeUri must begin with /")
	}
	if !appEngineMethod[r.HTTPMethod] {
		return invalidArgument("appEngineHttpRequest.httpMethod PATCH/OPTIONS are not permitted")
	}
	return nil
}
