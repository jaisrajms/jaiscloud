package scheduler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"regexp"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	schedstore "jaiscloud/internal/gcp/store/scheduler"
	"jaiscloud/internal/model"
)

var (
	jobIDPattern    = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
	validMethods    = map[string]bool{"": true, "POST": true, "GET": true, "HEAD": true, "PUT": true, "DELETE": true, "PATCH": true, "OPTIONS": true}
	bodyMethods     = map[string]bool{"POST": true, "PUT": true, "PATCH": true}
	appEngineMethod = map[string]bool{"": true, "POST": true, "GET": true, "HEAD": true, "PUT": true, "DELETE": true}
)

// Service is the transport-neutral Cloud Scheduler core. Both the REST and gRPC
// adapters share one instance (and one store), so the two transports cannot
// drift.
type Service struct {
	store  schedstore.Store
	engine *Engine
}

// NewService returns a Cloud Scheduler core over the store.
func NewService(store schedstore.Store) *Service {
	return &Service{store: store}
}

// SetEngine attaches the cron engine so pause/resume/run and the engine share
// retry state. Optional: a nil engine disables delivery (unit tests).
func (s *Service) SetEngine(e *Engine) { s.engine = e }

// Reset clears all jobs and the engine's retry counters.
func (s *Service) Reset(ctx context.Context) {
	s.store.Reset(ctx)
	if s.engine != nil {
		s.engine.ResetRetries()
	}
}

// CreateJob validates and stores a new job. j.Name is the job id; when empty a
// random id is generated. The parent project/location are the request's.
func (s *Service) CreateJob(ctx context.Context, project, location string, j schedstore.Job) (schedstore.Job, error) {
	if project == "" || location == "" {
		return schedstore.Job{}, invalidArgument("project and location are required")
	}
	if j.Name == "" {
		j.Name = newJobID()
	}
	if !jobIDPattern.MatchString(j.Name) || len(j.Name) > 500 {
		return schedstore.Job{}, invalidArgument("invalid job id")
	}
	now := clock.Now()
	j.ProjectID = project
	j.Location = location
	j.State = schedstore.StateEnabled
	j.UserUpdateTime = now
	j.LastAttemptTime = time.Time{}
	j.Status = nil
	if err := validateJob(j); err != nil {
		return schedstore.Job{}, err
	}
	j.ScheduleTime = nextFireFor(j, now)
	if err := s.store.CreateJob(ctx, project, location, j); err != nil {
		return schedstore.Job{}, mapStoreErr(err)
	}
	return j, nil
}

// GetJob returns a job by id.
func (s *Service) GetJob(ctx context.Context, project, location, name string) (schedstore.Job, error) {
	j, err := s.store.GetJob(ctx, project, location, name)
	if err != nil {
		return schedstore.Job{}, mapStoreErr(err)
	}
	return j, nil
}

// ListJobs lists jobs under a location, sorted by id.
func (s *Service) ListJobs(ctx context.Context, project, location string) ([]schedstore.Job, error) {
	jobs, err := s.store.ListJobs(ctx, project, location)
	if err != nil {
		return nil, mapStoreErr(err)
	}
	return jobs, nil
}

// UpdateJob applies a partial update. An empty mask replaces every mutable
// field from upd; otherwise only the named (camelCase Discovery or snake_case
// proto) fields are copied. Nested target fields are replaced as a whole.
func (s *Service) UpdateJob(ctx context.Context, project, location, name string, upd schedstore.Job, mask []string) (schedstore.Job, error) {
	full := len(mask) == 0
	set := make(map[string]bool, len(mask))
	for _, f := range mask {
		set[normalizeField(f)] = true
	}
	updated, err := s.store.UpdateJobAtomic(ctx, project, location, name, func(cur schedstore.Job) (schedstore.Job, error) {
		next := applyUpdate(cur, upd, set, full)
		if err := validateJob(next); err != nil {
			return cur, err
		}
		next.UserUpdateTime = clock.Now()
		if next.State == schedstore.StateEnabled {
			next.ScheduleTime = nextFireFor(next, clock.Now())
		}
		return next, nil
	})
	if err != nil {
		return schedstore.Job{}, mapStoreErr(err)
	}
	return updated, nil
}

// DeleteJob removes a job.
func (s *Service) DeleteJob(ctx context.Context, project, location, name string) error {
	if err := s.store.DeleteJob(ctx, project, location, name); err != nil {
		return mapStoreErr(err)
	}
	return nil
}

// PauseJob pauses an ENABLED job.
func (s *Service) PauseJob(ctx context.Context, project, location, name string) (schedstore.Job, error) {
	return s.mutateState(ctx, project, location, name, func(cur schedstore.Job) (schedstore.Job, error) {
		if cur.State != schedstore.StateEnabled {
			return cur, failedPrecondition("job must be ENABLED to pause")
		}
		cur.State = schedstore.StatePaused
		return cur, nil
	})
}

// ResumeJob resumes a PAUSED job and recomputes its next fire time.
func (s *Service) ResumeJob(ctx context.Context, project, location, name string) (schedstore.Job, error) {
	return s.mutateState(ctx, project, location, name, func(cur schedstore.Job) (schedstore.Job, error) {
		if cur.State != schedstore.StatePaused {
			return cur, failedPrecondition("job must be PAUSED to resume")
		}
		cur.State = schedstore.StateEnabled
		cur.ScheduleTime = nextFireFor(cur, clock.Now())
		return cur, nil
	})
}

// RunJob forces an immediate delivery attempt and records its outcome.
func (s *Service) RunJob(ctx context.Context, project, location, name string) (schedstore.Job, error) {
	cur, err := s.store.GetJob(ctx, project, location, name)
	if err != nil {
		return schedstore.Job{}, mapStoreErr(err)
	}
	if s.engine == nil || s.engine.disp == nil {
		return cur, nil
	}
	now := clock.Now()
	status := s.engine.disp.Deliver(ctx, cur)
	return s.store.UpdateJobAtomic(ctx, project, location, name, func(j schedstore.Job) (schedstore.Job, error) {
		j.LastAttemptTime = now
		if status.Code == 0 {
			j.Status = nil
		} else {
			st := status
			j.Status = &st
		}
		return j, nil
	})
}

func (s *Service) mutateState(ctx context.Context, project, location, name string, mutate func(schedstore.Job) (schedstore.Job, error)) (schedstore.Job, error) {
	j, err := s.store.UpdateJobAtomic(ctx, project, location, name, func(cur schedstore.Job) (schedstore.Job, error) {
		next, err := mutate(cur)
		if err != nil {
			return cur, err
		}
		next.UserUpdateTime = clock.Now()
		return next, nil
	})
	if err != nil {
		return schedstore.Job{}, mapStoreErr(err)
	}
	return j, nil
}

// --- validation ---

func validateJob(j schedstore.Job) error {
	if _, err := ParseSchedule(j.Schedule, j.TimeZone); err != nil {
		return err
	}
	switch j.Target {
	case schedstore.TargetHTTP:
		if j.HTTP == nil {
			return invalidArgument("httpTarget is required")
		}
		if err := validateHTTP(j.HTTP); err != nil {
			return err
		}
	case schedstore.TargetPubSub:
		if j.PubSub == nil || j.PubSub.TopicName == "" {
			return invalidArgument("pubsubTarget.topicName is required")
		}
	case schedstore.TargetAppEngine:
		if j.AppEngine == nil {
			return invalidArgument("appEngineHttpTarget is required")
		}
		if j.AppEngine.RelativeURI != "" && !strings.HasPrefix(j.AppEngine.RelativeURI, "/") {
			return invalidArgument("appEngineHttpTarget.relativeUri must begin with /")
		}
		if !appEngineMethod[j.AppEngine.HTTPMethod] {
			return invalidArgument("appEngineHttpTarget.httpMethod PATCH/OPTIONS are not permitted")
		}
	default:
		return invalidArgument("exactly one of httpTarget, pubsubTarget, or appEngineHttpTarget is required")
	}
	if j.RetryConfig != nil {
		if j.RetryConfig.RetryCount < 0 || j.RetryConfig.RetryCount > 5 {
			return invalidArgument("retryConfig.retryCount must be between 0 and 5")
		}
		if j.RetryConfig.MaxDoublings < 0 || j.RetryConfig.MaxDoublings > 5 {
			return invalidArgument("retryConfig.maxDoublings must be between 0 and 5")
		}
	}
	if j.AttemptDeadline > 0 && j.Target == schedstore.TargetHTTP {
		if j.AttemptDeadline < 15*time.Second || j.AttemptDeadline > 30*time.Minute {
			return invalidArgument("attemptDeadline must be between 15s and 30m for HTTP targets")
		}
	}
	return nil
}

func validateHTTP(t *schedstore.HttpTarget) error {
	if !strings.HasPrefix(t.URI, "http://") && !strings.HasPrefix(t.URI, "https://") {
		return invalidArgument("httpTarget.uri must begin with http:// or https://")
	}
	if !validMethods[t.HTTPMethod] {
		return invalidArgument("invalid httpTarget.httpMethod")
	}
	if len(t.Body) > 0 && t.HTTPMethod != "" && !bodyMethods[t.HTTPMethod] {
		return invalidArgument("httpTarget.body is only allowed for POST, PUT, or PATCH")
	}
	return nil
}

// --- update merge ---

func normalizeField(f string) string {
	f = strings.ToLower(strings.TrimSpace(f))
	if i := strings.IndexByte(f, '.'); i >= 0 {
		f = f[:i]
	}
	return strings.ReplaceAll(f, "_", "")
}

func applyUpdate(cur, upd schedstore.Job, set map[string]bool, full bool) schedstore.Job {
	next := cur
	has := func(f string) bool { return full || set[f] }
	if has("description") {
		next.Description = upd.Description
	}
	if has("schedule") {
		next.Schedule = upd.Schedule
	}
	if has("timezone") {
		next.TimeZone = upd.TimeZone
	}
	if has("httptarget") {
		next.HTTP = upd.HTTP
	}
	if has("pubsubtarget") {
		next.PubSub = upd.PubSub
	}
	if has("appenginehttptarget") {
		next.AppEngine = upd.AppEngine
	}
	if has("retryconfig") {
		next.RetryConfig = upd.RetryConfig
	}
	if has("attemptdeadline") {
		next.AttemptDeadline = upd.AttemptDeadline
	}
	next.Target = targetOf(next)
	return next
}

func targetOf(j schedstore.Job) schedstore.TargetKind {
	switch {
	case j.HTTP != nil:
		return schedstore.TargetHTTP
	case j.PubSub != nil:
		return schedstore.TargetPubSub
	case j.AppEngine != nil:
		return schedstore.TargetAppEngine
	default:
		return ""
	}
}

// --- helpers ---

func newJobID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return "job-" + hex.EncodeToString(b[:])
}

func invalidArgument(msg string) error {
	return model.NewProviderError("InvalidArgument", msg, 400)
}

func failedPrecondition(msg string) error {
	return model.NewProviderError("FailedPrecondition", msg, 400)
}

func mapStoreErr(err error) error {
	switch {
	case errors.Is(err, schedstore.ErrNoSuchJob):
		return model.NewProviderError("NotFound", "job not found", 404)
	case errors.Is(err, schedstore.ErrAlreadyExists):
		return model.NewProviderError("AlreadyExists", "job already exists", 409)
	default:
		return err
	}
}

// IsNotFound reports whether err is the core's job-not-found error.
func IsNotFound(err error) bool {
	var perr *model.ProviderError
	if errors.As(err, &perr) {
		return perr.Code == "NotFound"
	}
	return false
}
