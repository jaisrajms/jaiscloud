// Package tasks is the transport-neutral Cloud Tasks v2 core
// (google.cloud.tasks.v2.CloudTasks). Both the REST and gRPC adapters share one
// instance (and one store), so the two transports cannot drift.
//
// It implements the control plane (queue CRUD + pause/resume/purge + queue IAM,
// and task CRUD + REST batch) and the HTTP dispatch engine: a created task is
// delivered to its httpRequest target with per-queue rate limits and
// exponential-backoff retries, and RunTask forces a synchronous attempt.
// appEngineHttpRequest targets are stored and echoed but never delivered (no
// App Engine router); their attempts are recorded as Unimplemented failures.
package tasks

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"regexp"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/policy"
	tasksstore "jaiscloud/internal/gcp/store/tasks"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

const rtQueuePolicy = "gcp_tasks_queue_policy"

var (
	queueIDPattern = regexp.MustCompile(`^[A-Za-z0-9-]{1,100}$`)
	taskIDPattern  = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
)

// Service is the transport-neutral Cloud Tasks core.
type Service struct {
	store     tasksstore.Store
	resources store.ResourceStore
	engine    *Engine
}

// NewService returns a Cloud Tasks core over the store. resources backs queue
// IAM policies (metadata only; authz is not enforced).
func NewService(s tasksstore.Store, resources store.ResourceStore) *Service {
	return &Service{store: s, resources: resources}
}

// SetEngine attaches the dispatch engine so RunTask shares its dispatcher and
// retry state with the background worker. Optional: a nil engine disables
// delivery (unit tests).
func (s *Service) SetEngine(e *Engine) { s.engine = e }

// Reset clears all queues, tasks, and queue IAM policies.
func (s *Service) Reset(ctx context.Context) {
	s.store.Reset(ctx)
	if s.engine != nil {
		s.engine.ResetState()
	}
	if s.resources != nil {
		_ = s.resources.Purge(ctx, "", store.GlobalRegion, rtQueuePolicy)
	}
}

// CreateQueue validates and stores a new queue. q.Name is the queue id.
func (s *Service) CreateQueue(ctx context.Context, project, location string, q tasksstore.Queue) (tasksstore.Queue, error) {
	if project == "" || location == "" {
		return tasksstore.Queue{}, invalidArgument("project and location are required")
	}
	if q.Name == "" {
		return tasksstore.Queue{}, invalidArgument("queue name is required")
	}
	if !queueIDPattern.MatchString(q.Name) {
		return tasksstore.Queue{}, invalidArgument("invalid queue id")
	}
	if err := validateQueue(&q); err != nil {
		return tasksstore.Queue{}, err
	}
	applyQueueDefaults(&q)
	q.State = tasksstore.StateRunning
	q.PurgeTime = time.Time{}
	if err := s.store.CreateQueue(ctx, project, location, q); err != nil {
		return tasksstore.Queue{}, mapStoreErr(err)
	}
	q.ProjectID = project
	q.Location = location
	return q, nil
}

// GetQueue returns a queue by id.
func (s *Service) GetQueue(ctx context.Context, project, location, name string) (tasksstore.Queue, error) {
	q, err := s.store.GetQueue(ctx, project, location, name)
	if err != nil {
		return tasksstore.Queue{}, mapStoreErr(err)
	}
	return q, nil
}

// ListQueues lists queues under a location, sorted by id. filter is applied as
// a best-effort substring match on the queue name (empty filter lists all).
func (s *Service) ListQueues(ctx context.Context, project, location, filter string) ([]tasksstore.Queue, error) {
	queues, err := s.store.ListQueues(ctx, project, location)
	if err != nil {
		return nil, mapStoreErr(err)
	}
	if filter == "" {
		return queues, nil
	}
	out := queues[:0]
	for _, q := range queues {
		if matchQueueFilter(filter, q) {
			out = append(out, q)
		}
	}
	return out, nil
}

// UpdateQueue applies a partial update. An empty mask replaces every mutable
// field from upd; otherwise only the named fields are copied. A queue that does
// not yet exist is created, matching Cloud Tasks' UpsertQueue semantics.
func (s *Service) UpdateQueue(ctx context.Context, project, location, name string, upd tasksstore.Queue, mask []string) (tasksstore.Queue, error) {
	full := len(mask) == 0
	set := make(map[string]bool, len(mask))
	for _, f := range mask {
		set[normalizeField(f)] = true
	}
	updated, err := s.store.UpdateQueueAtomic(ctx, project, location, name, func(cur tasksstore.Queue) (tasksstore.Queue, error) {
		next := applyQueueUpdate(cur, upd, set, full)
		if err := validateQueue(&next); err != nil {
			return cur, err
		}
		applyQueueDefaults(&next)
		return next, nil
	})
	if errors.Is(err, tasksstore.ErrNoSuchQueue) {
		// Create-if-missing, as the proto documents for UpdateQueue.
		created := tasksstore.Queue{}
		created = applyQueueUpdate(created, upd, set, full)
		created.Name = name
		return s.CreateQueue(ctx, project, location, created)
	}
	if err != nil {
		return tasksstore.Queue{}, mapStoreErr(err)
	}
	return updated, nil
}

// DeleteQueue removes a queue and cascades to its tasks.
func (s *Service) DeleteQueue(ctx context.Context, project, location, name string) error {
	if err := s.store.DeleteQueue(ctx, project, location, name); err != nil {
		return mapStoreErr(err)
	}
	if s.resources != nil {
		_ = s.resources.Delete(ctx, project, store.GlobalRegion, rtQueuePolicy, iamID(location, name))
	}
	return nil
}

// PurgeQueue deletes all tasks in a queue and records purgeTime.
func (s *Service) PurgeQueue(ctx context.Context, project, location, name string) (tasksstore.Queue, error) {
	if _, err := s.store.GetQueue(ctx, project, location, name); err != nil {
		return tasksstore.Queue{}, mapStoreErr(err)
	}
	if _, err := s.store.DeleteTasks(ctx, project, location, name); err != nil {
		return tasksstore.Queue{}, err
	}
	return s.store.UpdateQueueAtomic(ctx, project, location, name, func(cur tasksstore.Queue) (tasksstore.Queue, error) {
		cur.PurgeTime = clock.Now()
		return cur, nil
	})
}

// PauseQueue pauses a RUNNING queue.
func (s *Service) PauseQueue(ctx context.Context, project, location, name string) (tasksstore.Queue, error) {
	return s.mutateQueueState(ctx, project, location, name, func(cur tasksstore.Queue) (tasksstore.Queue, error) {
		if cur.State != tasksstore.StateRunning {
			return cur, failedPrecondition("queue must be RUNNING to pause")
		}
		cur.State = tasksstore.StatePaused
		return cur, nil
	})
}

// ResumeQueue resumes a PAUSED or DISABLED queue.
func (s *Service) ResumeQueue(ctx context.Context, project, location, name string) (tasksstore.Queue, error) {
	return s.mutateQueueState(ctx, project, location, name, func(cur tasksstore.Queue) (tasksstore.Queue, error) {
		if cur.State == tasksstore.StateRunning {
			return cur, failedPrecondition("queue is already RUNNING")
		}
		cur.State = tasksstore.StateRunning
		return cur, nil
	})
}

func (s *Service) mutateQueueState(ctx context.Context, project, location, name string, mutate func(tasksstore.Queue) (tasksstore.Queue, error)) (tasksstore.Queue, error) {
	q, err := s.store.UpdateQueueAtomic(ctx, project, location, name, mutate)
	if err != nil {
		return tasksstore.Queue{}, mapStoreErr(err)
	}
	return q, nil
}

// --- helpers ---

func newTaskID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return "task-" + hex.EncodeToString(b[:])
}

func invalidArgument(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, 400)
}

func failedPrecondition(msg string) error {
	return model.NewProviderError("FailedPrecondition", msg, 400)
}

func mapStoreErr(err error) error {
	switch {
	case errors.Is(err, tasksstore.ErrNoSuchQueue):
		return model.NewProviderError("NotFound", "queue not found", 404)
	case errors.Is(err, tasksstore.ErrNoSuchTask):
		return model.NewProviderError("NotFound", "task not found", 404)
	case errors.Is(err, tasksstore.ErrAlreadyExists):
		return model.NewProviderError("AlreadyExists", "resource already exists", 409)
	default:
		return err
	}
}

func iamID(location, queue string) string { return location + "/" + queue }

func (s *Service) loadPolicy(ctx context.Context, project, location, queue string) policy.Policy {
	return policy.Load(ctx, s.resources, project, rtQueuePolicy, iamID(location, queue))
}
