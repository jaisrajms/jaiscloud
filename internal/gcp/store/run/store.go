// Package run provides the Cloud Run Admin v2 (run.googleapis.com) store.
// Services are project+location scoped with canonical names
// projects/{project}/locations/{location}/services/{service}; revisions nest
// under a service and operations are the google.longrunning.Operation records
// under projects/{project}/locations/{location}/operations/{operation}.
//
// The emulator's control plane is behavioural: a Service and its Revision are
// stored records whose output-only fields (uri, conditions, trafficStatuses,
// latestReadyRevision) are derived on read, and each mutation records an
// Operation so operations.get/list and the REST :wait custom method can read it
// back. The caller-supplied JSON (template, labels, annotations, description,
// ingress, traffic) is kept verbatim in Data and echoed back, so arbitrary
// template fields survive a round trip without the emulator modelling every
// proto field.
package run

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrNoSuchService is returned when a service name is unknown.
	ErrNoSuchService = errors.New("NoSuchService")
	// ErrNoSuchRevision is returned when a revision name is unknown.
	ErrNoSuchRevision = errors.New("NoSuchRevision")
	// ErrNoSuchOperation is returned when an operation name is unknown.
	ErrNoSuchOperation = errors.New("NoSuchOperation")
	// ErrAlreadyExists is returned when creating a resource that already exists.
	ErrAlreadyExists = errors.New("AlreadyExists")
)

// Service is the persisted subset of google.cloud.run.v2.Service. ID is the
// short service id (the last path segment); the canonical resource name is
// derived from ProjectID/Location/ID. Data is the caller-supplied writable JSON
// (template, labels, annotations, description, ingress, traffic, ...).
type Service struct {
	ProjectID string `json:"projectId"`
	Location  string `json:"location"`
	ID        string `json:"id"`

	UID        string `json:"uid,omitempty"`
	Generation int64  `json:"generation,omitempty"`
	Etag       string `json:"etag,omitempty"`
	Uri        string `json:"uri,omitempty"`

	LatestReadyRevision   string `json:"latestReadyRevision,omitempty"`
	LatestCreatedRevision string `json:"latestCreatedRevision,omitempty"`

	CreateTime time.Time `json:"createTime,omitempty"`
	UpdateTime time.Time `json:"updateTime,omitempty"`
	DeleteTime time.Time `json:"deleteTime,omitempty"`

	Data map[string]any `json:"data,omitempty"`
}

// Revision is the persisted subset of google.cloud.run.v2.Revision. Service is
// the parent service id and ID is the revision id (e.g. "{service}-00001"). Data
// is the revision template the service supplied (containers, volumes,
// serviceAccount, ...).
type Revision struct {
	ProjectID string `json:"projectId"`
	Location  string `json:"location"`
	Service   string `json:"service"`
	ID        string `json:"id"`

	UID        string `json:"uid,omitempty"`
	Generation int64  `json:"generation,omitempty"`
	Etag       string `json:"etag,omitempty"`

	CreateTime time.Time `json:"createTime,omitempty"`
	UpdateTime time.Time `json:"updateTime,omitempty"`

	Data map[string]any `json:"data,omitempty"`
}

// Operation is the persisted google.longrunning.Operation record. ID is the
// operation id ("operation-run-<uuid>"); the canonical resource name is derived
// from ProjectID/Location/ID. Service is the create/update/delete response
// snapshot (render-time only, populated on read via the referenced service).
type Operation struct {
	ProjectID string `json:"projectId"`
	Location  string `json:"location"`
	ID        string `json:"id"`

	Verb   string `json:"verb,omitempty"`
	Target string `json:"target,omitempty"`
	Done   bool   `json:"done"`

	CreateTime time.Time `json:"createTime,omitempty"`
	EndTime    time.Time `json:"endTime,omitempty"`

	// Service is the operation's response/metadata snapshot. It is stored so a
	// delete operation can still render the removed service.
	Service *Service `json:"service,omitempty"`
}

// Store is the Cloud Run service/revision/operation store.
type Store interface {
	CreateService(ctx context.Context, project, location string, s Service) error
	GetService(ctx context.Context, project, location, id string) (Service, error)
	UpdateService(ctx context.Context, project, location string, s Service) error
	DeleteService(ctx context.Context, project, location, id string) error
	ListServices(ctx context.Context, project, location string) ([]Service, error)

	CreateRevision(ctx context.Context, project, location, service string, r Revision) error
	GetRevision(ctx context.Context, project, location, service, id string) (Revision, error)
	ListRevisions(ctx context.Context, project, location, service string) ([]Revision, error)
	DeleteRevisions(ctx context.Context, project, location, service string) error

	CreateOperation(ctx context.Context, project, location string, op Operation) error
	GetOperation(ctx context.Context, project, location, id string) (Operation, error)
	ListOperations(ctx context.Context, project, location string) ([]Operation, error)
	DeleteOperation(ctx context.Context, project, location, id string) error

	// Reset clears all services, revisions and operations (/_jaiscloud/reset).
	Reset(ctx context.Context)
}
