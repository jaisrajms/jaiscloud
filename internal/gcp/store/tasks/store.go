// Package tasks provides the Cloud Tasks v2 store. Queues are
// project+location scoped with canonical names
// projects/{project}/locations/{location}/queues/{queue}; tasks nest under a
// queue as
// projects/{project}/locations/{location}/queues/{queue}/tasks/{task}. The
// store persists the full queue (rateLimits, retryConfig, state) and task
// (target, scheduleTime, attempts, counters) records; the control plane in
// internal/gcp/service/tasks reads and writes them, and the dispatch engine
// (a later phase) fires due tasks.
package tasks

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrNoSuchQueue is returned when a queue name is unknown.
	ErrNoSuchQueue = errors.New("NoSuchQueue")
	// ErrNoSuchTask is returned when a task name is unknown.
	ErrNoSuchTask = errors.New("NoSuchTask")
	// ErrAlreadyExists is returned when creating a queue or task that already
	// exists.
	ErrAlreadyExists = errors.New("AlreadyExists")
)

// QueueState mirrors the Queue.State enum.
type QueueState string

const (
	StateRunning  QueueState = "RUNNING"
	StatePaused   QueueState = "PAUSED"
	StateDisabled QueueState = "DISABLED"
)

// TargetKind is the discriminator for a task's delivery target.
type TargetKind string

const (
	// TargetHTTP delivers an HTTP request to the task's url.
	TargetHTTP TargetKind = "http"
	// TargetAppEngine targets App Engine. The emulator stores the target but
	// cannot deliver it (no App Engine router).
	TargetAppEngine TargetKind = "appengine"
)

// AppEngineRouting is the App Engine routing configuration.
type AppEngineRouting struct {
	Service  string `json:"service,omitempty"`
	Version  string `json:"version,omitempty"`
	Instance string `json:"instance,omitempty"`
	Host     string `json:"host,omitempty"`
}

// RateLimits mirrors the Queue.RateLimits message.
type RateLimits struct {
	MaxDispatchesPerSecond  float64 `json:"maxDispatchesPerSecond,omitempty"`
	MaxBurstSize            int32   `json:"maxBurstSize,omitempty"`
	MaxConcurrentDispatches int32   `json:"maxConcurrentDispatches,omitempty"`
}

// RetryConfig mirrors the Queue.RetryConfig message. Durations are stored as
// Go durations (JSON int64 nanoseconds).
type RetryConfig struct {
	MaxAttempts      int32         `json:"maxAttempts,omitempty"`
	MaxRetryDuration time.Duration `json:"maxRetryDuration,omitempty"`
	MinBackoff       time.Duration `json:"minBackoff,omitempty"`
	MaxBackoff       time.Duration `json:"maxBackoff,omitempty"`
	MaxDoublings     int32         `json:"maxDoublings,omitempty"`
}

// StackdriverLoggingConfig mirrors Queue.StackdriverLoggingConfig.
type StackdriverLoggingConfig struct {
	SamplingRatio float64 `json:"samplingRatio,omitempty"`
}

// Queue is a Cloud Tasks queue. Name is the queue id (the last path segment);
// the canonical resource name is derived from ProjectID/Location/Name.
type Queue struct {
	ProjectID string `json:"projectId"`
	Location  string `json:"location"`
	Name      string `json:"name"`

	AppEngineRoutingOverride *AppEngineRouting         `json:"appEngineRoutingOverride,omitempty"`
	RateLimits               *RateLimits               `json:"rateLimits,omitempty"`
	RetryConfig              *RetryConfig              `json:"retryConfig,omitempty"`
	StackdriverLoggingConfig *StackdriverLoggingConfig `json:"stackdriverLoggingConfig,omitempty"`

	State     QueueState `json:"state"`
	PurgeTime time.Time  `json:"purgeTime,omitempty"`
}

// OAuthToken and OidcToken carry the auth configuration for an HTTP target. The
// emulator mints no real Google token; the fields are persisted and echoed.
type OAuthToken struct {
	ServiceAccountEmail string `json:"serviceAccountEmail,omitempty"`
	Scope               string `json:"scope,omitempty"`
}

// OidcToken carries an OpenID Connect token configuration.
type OidcToken struct {
	ServiceAccountEmail string `json:"serviceAccountEmail,omitempty"`
	Audience            string `json:"audience,omitempty"`
}

// HttpRequest is an HTTP delivery target.
type HttpRequest struct {
	URL        string            `json:"url"`
	HTTPMethod string            `json:"httpMethod,omitempty"`
	Headers    map[string]string `json:"headers,omitempty"`
	Body       []byte            `json:"body,omitempty"`
	OAuthToken *OAuthToken       `json:"oauthToken,omitempty"`
	OidcToken  *OidcToken        `json:"oidcToken,omitempty"`
}

// AppEngineHttpRequest is an App Engine HTTP delivery target. The emulator
// stores it but cannot deliver it.
type AppEngineHttpRequest struct {
	HTTPMethod       string            `json:"httpMethod,omitempty"`
	AppEngineRouting *AppEngineRouting `json:"appEngineRouting,omitempty"`
	RelativeURI      string            `json:"relativeUri,omitempty"`
	Headers          map[string]string `json:"headers,omitempty"`
	Body             []byte            `json:"body,omitempty"`
}

// Status is the result of a delivery attempt, shaped like a google.rpc.Status.
type Status struct {
	Code    int32  `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

// Attempt records one delivery attempt of a task.
type Attempt struct {
	ScheduleTime   time.Time `json:"scheduleTime,omitempty"`
	DispatchTime   time.Time `json:"dispatchTime,omitempty"`
	ResponseTime   time.Time `json:"responseTime,omitempty"`
	ResponseStatus *Status   `json:"responseStatus,omitempty"`
}

// Task is a Cloud Tasks task. Name is the task id (the last path segment); the
// canonical resource name derives from ProjectID/Location/Queue/Name.
type Task struct {
	ProjectID string `json:"projectId"`
	Location  string `json:"location"`
	Queue     string `json:"queue"`
	Name      string `json:"name"`

	Target    TargetKind            `json:"target"`
	HTTP      *HttpRequest          `json:"httpRequest,omitempty"`
	AppEngine *AppEngineHttpRequest `json:"appEngineHttpRequest,omitempty"`

	ScheduleTime     time.Time     `json:"scheduleTime,omitempty"`
	CreateTime       time.Time     `json:"createTime,omitempty"`
	DispatchDeadline time.Duration `json:"dispatchDeadline,omitempty"`
	DispatchCount    int32         `json:"dispatchCount,omitempty"`
	ResponseCount    int32         `json:"responseCount,omitempty"`
	// ExecutionCount is the number of attempts whose handler returned a
	// response other than 5XX (Cloud Tasks' X-CloudTasks-TaskExecutionCount
	// header). It is internal: real Cloud Tasks does not expose it on the Task
	// resource.
	ExecutionCount int32    `json:"executionCount,omitempty"`
	FirstAttempt   *Attempt `json:"firstAttempt,omitempty"`
	LastAttempt    *Attempt `json:"lastAttempt,omitempty"`
}

// Store is the Cloud Tasks store.
type Store interface {
	CreateQueue(ctx context.Context, projectID, location string, q Queue) error
	GetQueue(ctx context.Context, projectID, location, name string) (Queue, error)
	UpdateQueue(ctx context.Context, projectID, location string, q Queue) error
	// UpdateQueueAtomic performs a locked get-mutate-set cycle so a pause
	// racing an update cannot lose a write.
	UpdateQueueAtomic(ctx context.Context, projectID, location, name string, mutate func(Queue) (Queue, error)) (Queue, error)
	DeleteQueue(ctx context.Context, projectID, location, name string) error
	ListQueues(ctx context.Context, projectID, location string) ([]Queue, error)
	// ListQueuesByProject returns every queue in a project across all
	// locations, sorted by location then name. It backs the location-optional
	// console list.
	ListQueuesByProject(ctx context.Context, projectID string) ([]Queue, error)
	// ListAllQueues returns every queue across all projects and locations. The
	// dispatch engine enumerates queues with it (it has no project/location
	// input).
	ListAllQueues(ctx context.Context) ([]Queue, error)

	CreateTask(ctx context.Context, projectID, location, queue string, t Task) error
	GetTask(ctx context.Context, projectID, location, queue, name string) (Task, error)
	// UpdateTaskAtomic is the locked read-modify-write cycle the dispatch
	// engine uses to claim a task.
	UpdateTaskAtomic(ctx context.Context, projectID, location, queue, name string, mutate func(Task) (Task, error)) (Task, error)
	DeleteTask(ctx context.Context, projectID, location, queue, name string) error
	ListTasks(ctx context.Context, projectID, location, queue string) ([]Task, error)
	// DeleteTasks purges every task in a queue and returns the count removed.
	// It is used both by PurgeQueue and by queue delete (cascade); it does not
	// fail when the queue is empty.
	DeleteTasks(ctx context.Context, projectID, location, queue string) (int, error)

	Reset(ctx context.Context)
}
