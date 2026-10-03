// Package functions provides the Cloud Functions v1 store. Functions live in a
// dedicated jc_functions table (mirroring the AWS Lambda function store),
// scoped by project + location (the GCP resource name is
// projects/{project}/locations/{location}/functions/{name}).
package functions

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNoSuchFunction  = errors.New("NoSuchFunction")
	ErrAlreadyExists   = errors.New("AlreadyExists")
	ErrNoSuchOperation = errors.New("NoSuchOperation")
	ErrNoSuchDelivery  = errors.New("NoSuchDelivery")
)

// EventTrigger is a source that fires events in response to a condition in
// another service (mirrors the CloudFunction.eventTrigger wire shape).
type EventTrigger struct {
	EventType string `json:"eventType"`
	Resource  string `json:"resource"`
	Service   string `json:"service,omitempty"`

	// Retry is the v1 failurePolicy.retry flag: true when the create/update body
	// carried a failurePolicy.retry object, meaning a failed invocation is
	// retried rather than dropped (matching the v1 FailurePolicy default of
	// "ignore failures" when absent). It is not rendered for v2.
	Retry bool `json:"retry,omitempty"`
	// RetryPolicy is the v2 EventTrigger.retryPolicy enum value
	// (RETRY_POLICY_RETRY / RETRY_POLICY_DO_NOT_RETRY / RETRY_POLICY_UNSPECIFIED).
	// Empty means unset (do not retry). It is not rendered for v1.
	RetryPolicy string `json:"retryPolicy,omitempty"`

	// Trigger is the resource name of the backing Eventarc trigger the platform
	// provisions for this Pub/Sub event trigger — real GCP's output-only
	// eventTrigger.trigger (FD9). Empty when no backing trigger was provisioned
	// (a non-Pub/Sub source, a missing topic, or no provisioner wired).
	Trigger string `json:"trigger,omitempty"`
	// Subscription is the short id of the backing Pub/Sub subscription that
	// carries this trigger's transport and holds its user-configurable
	// deadLetterPolicy. It is internal (real GCP does not expose it on the
	// function); the user reaches it through the Eventarc trigger's
	// transport.pubsub.subscription or subscriptions.list.
	Subscription string `json:"subscription,omitempty"`
}

// Retries reports whether an event trigger's failure policy retries a failed
// invocation. v1 enables it with a failurePolicy.retry object, v2 with the
// RETRY_POLICY_RETRY enum; every other value drops the event (matching the
// Cloud Functions default of ignoring failures).
func (e *EventTrigger) Retries() bool {
	if e == nil {
		return false
	}
	return e.Retry || e.RetryPolicy == "RETRY_POLICY_RETRY"
}

// Function is Cloud Functions v1 function metadata.
type Function struct {
	ID                   string            // function ID (last segment of name)
	Location             string            // region
	Runtime              string            // e.g. "nodejs20"
	EntryPoint           string            // the source function to execute
	SourceUploadURL      string            // generated upload URL (fake)
	SourceArchiveURL     string            // gs:// source archive
	HttpsTriggerURL      string            // derived deployed URL (output-only)
	EventTrigger         *EventTrigger     // nil for HTTP-triggered functions
	EnvironmentVariables map[string]string // env vars available during execution
	Status               string            // e.g. "ACTIVE"
	CreateTime           time.Time
	UpdateTime           time.Time
	Labels               map[string]string
	AvailableMemoryMB    int
	Timeout              string // e.g. "60s"
	Description          string

	// MinInstanceCount, MaxInstanceCount, MaxInstanceRequestConcurrency, and
	// AvailableCPU are the Cloud Functions v2 ServiceConfig instance/concurrency
	// settings (FD6). They are v2-only on the wire and render under
	// serviceConfig; zero/empty means the client did not configure the field.
	// MaxInstanceCount == 0 means "no configured limit" (real GCP's default
	// behaviour), not a limit of zero.
	MinInstanceCount              int
	MaxInstanceCount              int
	MaxInstanceRequestConcurrency int
	AvailableCPU                  string

	// SourceSHA256 is the hex sha256 of the persisted source archive — the
	// function's revision hash — or "" when no archive has been stored.
	SourceSHA256 string
	// SourceSize is the persisted archive size in bytes (0 when absent).
	SourceSize int64
	// SourceBlobKey is the blobfs key of the archive in the "functions-source"
	// namespace (see service/functions/source.go). "" when absent.
	SourceBlobKey string

	// Revision is the 1-based deploy revision counter, bumped each time a new
	// source archive is deployed. It is rendered as serviceConfig.revision; 0
	// means the function has no deployed revision (the metadata-only case).
	Revision int
	// UpgradeState is the persisted UpgradeInfo.upgradeState enum value driven
	// by the v2 1st→2nd gen upgrade methods (FD5). "" means unspecified (the
	// function has not entered the upgrade flow).
	UpgradeState string
	// UpgradeRuntime and UpgradeMaxInstances are the Gen2 config overrides
	// captured by setupFunctionUpgradeConfig (buildConfigOverrides.runtime and
	// serviceConfigOverrides.maxInstanceCount). They are reverted by
	// abortFunctionUpgrade.
	UpgradeRuntime      string
	UpgradeMaxInstances int
	// UpgradeTrafficGen2 is true once redirectFunctionUpgradeTraffic has moved
	// traffic to the Gen2 copy; allTrafficOnLatestRevision renders false while
	// it is set (rollbackFunctionUpgradeTraffic clears it).
	UpgradeTrafficGen2 bool
}

// Operation is a persisted Cloud Functions long-running operation returned by a
// create/update/delete mutation. Function is the version-independent response
// snapshot carried by a create/update (nil for a delete, whose response is a
// google.protobuf.Empty); storing the record — rather than pre-rendered JSON —
// lets the shared renderer emit either the v1 or v2 wire shape on read.
type Operation struct {
	ID         string    `json:"id"`
	Location   string    `json:"location"`
	Done       bool      `json:"done"`
	Verb       string    `json:"verb"`
	Target     string    `json:"target"`
	Function   *Function `json:"function,omitempty"`
	CreateTime time.Time `json:"createTime"`
	EndTime    time.Time `json:"endTime"`
}

// Delivery status values. A delivery starts pending, becomes delivered when the
// function invocation succeeds, failed when it fails and the trigger's policy
// does not retry, or dead_letter when retries are exhausted.
const (
	DeliveryPending    = "pending"
	DeliveryDelivered  = "delivered"
	DeliveryFailed     = "failed"
	DeliveryDeadLetter = "dead_letter"
)

// Delivery is a persisted record of one event delivered (or attempted) to a
// function. Real Cloud Functions does not expose delivery records; the emulator
// persists them so retries and dead-letter outcomes are observable and survive a
// restart under --dsn.
type Delivery struct {
	ID         string            `json:"id"`
	Project    string            `json:"project"`
	Location   string            `json:"location"`
	FunctionID string            `json:"functionId"`
	Source     string            `json:"source"`    // "pubsub" | "storage" | "eventarc"
	EventType  string            `json:"eventType"` // canonical event type
	Resource   string            `json:"resource"`  // normalized source resource
	EventID    string            `json:"eventId,omitempty"`
	Data       string            `json:"data,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
	Attempts   int               `json:"attempts"`
	Status     string            `json:"status"`
	Error      string            `json:"error,omitempty"`
	Result     string            `json:"result,omitempty"`
	// DeadLetterTopic is the short id of the dead-letter topic the event was
	// forwarded to when retries were exhausted under a subscription
	// deadLetterPolicy, or "" when no policy was set (FD9).
	DeadLetterTopic string    `json:"deadLetterTopic,omitempty"`
	CreateTime      time.Time `json:"createTime"`
	UpdateTime      time.Time `json:"updateTime"`
}

// Store is the Cloud Functions v1 store.
type Store interface {
	CreateFunction(ctx context.Context, projectID, location, id string, f Function) error
	GetFunction(ctx context.Context, projectID, location, id string) (Function, error)
	UpdateFunction(ctx context.Context, projectID, location, id string, f Function) error
	// UpdateFunctionAtomic performs a locked get-mutate-set cycle: mutate
	// receives the current function and returns the version to persist, or an
	// error to abort without writing. Unlike a separate GetFunction followed
	// by UpdateFunction, this is atomic with respect to concurrent updates on
	// the same function, so a PATCH that merges only a subset of fields can't
	// lose a concurrent PATCH's changes to other fields.
	UpdateFunctionAtomic(ctx context.Context, projectID, location, id string, mutate func(Function) (Function, error)) (Function, error)
	DeleteFunction(ctx context.Context, projectID, location, id string) error
	ListFunctions(ctx context.Context, projectID, location string) ([]Function, error)
	// ListFunctionsAllLocations returns every function for a project across all
	// locations, for the "locations/-/functions" (all-locations) wildcard.
	ListFunctionsAllLocations(ctx context.Context, projectID string) ([]Function, error)

	// Operations persist the done google.longrunning.Operation returned by a
	// function create/update/delete so a poll can read it back. They are
	// project+location scoped and keyed by their opaque id.
	CreateOperation(ctx context.Context, projectID, location string, op Operation) error
	GetOperation(ctx context.Context, projectID, location, id string) (Operation, error)
	// GetOperationByID finds an operation by its (globally unique) id. The v1
	// Cloud Functions operations surface is top-level (operations/{id}) with no
	// project or location segment, so the lookup is NOT scoped to a project: it
	// returns the operation wherever it lives. ErrNoSuchOperation when absent.
	GetOperationByID(ctx context.Context, projectID, id string) (Operation, error)
	// DeleteOperationByID removes an operation by its globally unique id (the
	// v1 top-level name carries no project/location). ErrNoSuchOperation when
	// absent.
	DeleteOperationByID(ctx context.Context, id string) error
	DeleteOperation(ctx context.Context, projectID, location, id string) error
	ListOperations(ctx context.Context, projectID, location string) ([]Operation, error)

	// Deliveries persist one event-trigger delivery per (project, location,
	// function). They are project+location scoped and keyed by their opaque id;
	// ListDeliveries returns every delivery in a location so an operator can
	// observe retries and dead-letter outcomes.
	CreateDelivery(ctx context.Context, projectID, location string, d Delivery) error
	UpdateDelivery(ctx context.Context, projectID, location string, d Delivery) error
	GetDelivery(ctx context.Context, projectID, location, id string) (Delivery, error)
	ListDeliveries(ctx context.Context, projectID, location string) ([]Delivery, error)

	Reset(ctx context.Context)
}
