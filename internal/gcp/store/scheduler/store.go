// Package scheduler provides the Cloud Scheduler v1 store. Jobs are
// project+location scoped with canonical names
// projects/{project}/locations/{location}/jobs/{job}. The emulator is the
// authoritative scheduler for these jobs: the store persists the full job
// record (target, schedule, retryConfig, state, output-only fields) and the
// engine in internal/gcp/service/scheduler fires the due ones.
package scheduler

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrNoSuchJob is returned when a job name is unknown.
	ErrNoSuchJob = errors.New("NoSuchJob")
	// ErrAlreadyExists is returned when creating a job that already exists.
	ErrAlreadyExists = errors.New("AlreadyExists")
)

// TargetKind is the discriminator for a job's delivery target.
type TargetKind string

const (
	// TargetHTTP delivers an HTTP request to the job's uri.
	TargetHTTP TargetKind = "http"
	// TargetPubSub publishes a message to the job's topic.
	TargetPubSub TargetKind = "pubsub"
	// TargetAppEngine targets App Engine. The emulator stores the target but
	// cannot deliver it (no App Engine router); attempts are recorded as
	// failures and the target is documented limited.
	TargetAppEngine TargetKind = "appengine"
)

// State mirrors the Job.State enum.
type State string

const (
	StateEnabled      State = "ENABLED"
	StatePaused       State = "PAUSED"
	StateDisabled     State = "DISABLED"
	StateUpdateFailed State = "UPDATE_FAILED"
)

// OAuthToken and OidcToken carry the auth configuration for an HTTP target.
// The emulator mints no real Google token (like the STS/downscope surfaces it
// is an emulator-local convention); the fields are persisted and echoed.
type OAuthToken struct {
	Scope               string `json:"scope,omitempty"`
	ServiceAccountEmail string `json:"serviceAccountEmail,omitempty"`
}

// OidcToken carries an OpenID Connect token configuration.
type OidcToken struct {
	ServiceAccountEmail string `json:"serviceAccountEmail,omitempty"`
	Audience            string `json:"audience,omitempty"`
}

// HttpTarget is an HTTP delivery target.
type HttpTarget struct {
	URI        string            `json:"uri"`
	HTTPMethod string            `json:"httpMethod,omitempty"`
	Headers    map[string]string `json:"headers,omitempty"`
	Body       []byte            `json:"body,omitempty"`
	OAuthToken *OAuthToken       `json:"oauthToken,omitempty"`
	OidcToken  *OidcToken        `json:"oidcToken,omitempty"`
}

// PubsubTarget is a Pub/Sub delivery target.
type PubsubTarget struct {
	TopicName  string            `json:"topicName"`
	Data       []byte            `json:"data,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// AppEngineRouting is the App Engine routing configuration.
type AppEngineRouting struct {
	Service  string `json:"service,omitempty"`
	Version  string `json:"version,omitempty"`
	Instance string `json:"instance,omitempty"`
	Host     string `json:"host,omitempty"`
}

// AppEngineTarget is an App Engine HTTP delivery target. The emulator stores it
// but cannot deliver it.
type AppEngineTarget struct {
	HTTPMethod  string            `json:"httpMethod,omitempty"`
	RelativeURI string            `json:"relativeUri,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
	Body        []byte            `json:"body,omitempty"`
	Routing     *AppEngineRouting `json:"appEngineRouting,omitempty"`
}

// RetryConfig is the job retry policy.
type RetryConfig struct {
	RetryCount         int32         `json:"retryCount,omitempty"`
	MaxRetryDuration   time.Duration `json:"maxRetryDuration,omitempty"`
	MinBackoffDuration time.Duration `json:"minBackoffDuration,omitempty"`
	MaxBackoffDuration time.Duration `json:"maxBackoffDuration,omitempty"`
	MaxDoublings       int32         `json:"maxDoublings,omitempty"`
}

// Status is the result of the last delivery attempt, shaped like a
// google.rpc.Status (its numeric code is the gRPC code).
type Status struct {
	Code    int32  `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

// Job is a Cloud Scheduler job. Name is the job id (the last path segment);
// the canonical resource name is derived from ProjectID/Location/Name.
type Job struct {
	ProjectID   string `json:"projectId"`
	Location    string `json:"location"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`

	Schedule string `json:"schedule,omitempty"`
	TimeZone string `json:"timeZone,omitempty"`

	Target    TargetKind       `json:"target"`
	HTTP      *HttpTarget      `json:"httpTarget,omitempty"`
	PubSub    *PubsubTarget    `json:"pubsubTarget,omitempty"`
	AppEngine *AppEngineTarget `json:"appEngineHttpTarget,omitempty"`

	RetryConfig     *RetryConfig  `json:"retryConfig,omitempty"`
	AttemptDeadline time.Duration `json:"attemptDeadline,omitempty"`

	State           State     `json:"state"`
	ScheduleTime    time.Time `json:"scheduleTime,omitempty"`
	LastAttemptTime time.Time `json:"lastAttemptTime,omitempty"`
	UserUpdateTime  time.Time `json:"userUpdateTime,omitempty"`
	Status          *Status   `json:"status,omitempty"`
}

// Store is the Cloud Scheduler job store.
type Store interface {
	CreateJob(ctx context.Context, projectID, location string, j Job) error
	GetJob(ctx context.Context, projectID, location, name string) (Job, error)
	UpdateJob(ctx context.Context, projectID, location string, j Job) error
	// UpdateJobAtomic performs a locked get-mutate-set cycle. Unlike a separate
	// GetJob followed by UpdateJob, this is atomic with respect to concurrent
	// updates on the same job, so a pause racing a patch cannot lose a write.
	UpdateJobAtomic(ctx context.Context, projectID, location, name string, mutate func(Job) (Job, error)) (Job, error)
	DeleteJob(ctx context.Context, projectID, location, name string) error
	ListJobs(ctx context.Context, projectID, location string) ([]Job, error)
	// ListJobsByProject returns every job in a project across all locations,
	// sorted by location then id. It backs the location-optional console list.
	ListJobsByProject(ctx context.Context, projectID string) ([]Job, error)
	// ListAllJobs returns every job across all projects and locations. The
	// engine uses it to discover jobs to schedule.
	ListAllJobs(ctx context.Context) ([]Job, error)
	Reset(ctx context.Context)
}
