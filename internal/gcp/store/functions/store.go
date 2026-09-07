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
	ErrNoSuchFunction = errors.New("NoSuchFunction")
	ErrAlreadyExists  = errors.New("AlreadyExists")
)

// EventTrigger is a source that fires events in response to a condition in
// another service (mirrors the CloudFunction.eventTrigger wire shape).
type EventTrigger struct {
	EventType string `json:"eventType"`
	Resource  string `json:"resource"`
	Service   string `json:"service,omitempty"`
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

	Reset(ctx context.Context)
}
