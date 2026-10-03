// Package metastore provides the Dataproc Metastore (Glue Data Catalog
// analogue) control-plane store. Resources are project+location scoped with
// canonical names projects/{project}/locations/{location}/services/{service},
// .../services/{service}/backups/{backup}, and
// .../services/{service}/metadataImports/{import}; long-running operations are
// stored under .../locations/{location}/operations/{id}.
//
// This is the thin management-plane CRUD only — the emulator never stands up a
// Hive Thrift / Iceberg metadata endpoint (Phase 3). A service is a logical
// record only; its request body (hiveMetastoreConfig, networkConfig, ...) is
// stored verbatim as JSONB so read-back echoes what the caller sent.
package metastore

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrNoSuchService        = errors.New("NoSuchService")
	ErrNoSuchBackup         = errors.New("NoSuchBackup")
	ErrNoSuchMetadataImport = errors.New("NoSuchMetadataImport")
	ErrNoSuchOperation      = errors.New("NoSuchOperation")
	ErrAlreadyExists        = errors.New("AlreadyExists")
)

// ServiceState records one service state transition for the internal status
// history. The metastore Service wire type has no statusHistory field, so this
// is persisted but never rendered (on synchronous create the current state is
// ACTIVE with CREATING recorded as the first history entry, mirroring Dataproc).
type ServiceState struct {
	State          string    `json:"state"`
	StateStartTime time.Time `json:"stateStartTime,omitempty"`
}

// Service is a logical Dataproc Metastore service. Config holds the full wire
// request body (hiveMetastoreConfig, network, networkConfig, encryptionConfig,
// databaseType, telemetryConfig, scalingConfig, ...) verbatim as JSON; Labels
// are the extracted labels map.
type Service struct {
	ProjectID    string            `json:"projectId"`
	Location     string            `json:"location"`
	Name         string            `json:"serviceName"`
	Config       json.RawMessage   `json:"config,omitempty"`
	Labels       map[string]string `json:"labels,omitempty"`
	State        string            `json:"state"`
	StateHistory []ServiceState    `json:"stateHistory,omitempty"`
	CreateTime   time.Time         `json:"createTime"`
	UpdateTime   time.Time         `json:"updateTime"`
}

// Backup is a logical metastore backup under a service. Config holds the wire
// request body verbatim; Description is the extracted description field.
type Backup struct {
	ProjectID   string          `json:"projectId"`
	Location    string          `json:"location"`
	ServiceName string          `json:"serviceName"`
	Name        string          `json:"backupName"`
	Config      json.RawMessage `json:"config,omitempty"`
	Description string          `json:"description,omitempty"`
	State       string          `json:"state"`
	CreateTime  time.Time       `json:"createTime"`
	EndTime     time.Time       `json:"endTime"`
}

// MetadataImport is a logical metadata import under a service. Config holds the
// wire request body (databaseDump) verbatim; Description is extracted.
type MetadataImport struct {
	ProjectID   string          `json:"projectId"`
	Location    string          `json:"location"`
	ServiceName string          `json:"serviceName"`
	Name        string          `json:"importName"`
	Config      json.RawMessage `json:"config,omitempty"`
	Description string          `json:"description,omitempty"`
	State       string          `json:"state"`
	CreateTime  time.Time       `json:"createTime"`
	UpdateTime  time.Time       `json:"updateTime"`
	EndTime     time.Time       `json:"endTime"`
}

// Operation is a done long-running operation. Metadata and Response are the
// already-rendered JSON wire objects (Metadata carries its @type) stored
// verbatim, mirroring the Dataproc operation shape.
type Operation struct {
	ID         string    `json:"id"`
	ProjectID  string    `json:"projectId"`
	Location   string    `json:"location"`
	Done       bool      `json:"done"`
	Metadata   string    `json:"metadata,omitempty"`
	Response   string    `json:"response,omitempty"`
	Verb       string    `json:"verb"`
	Target     string    `json:"target"`
	CreateTime time.Time `json:"createTime"`
	EndTime    time.Time `json:"endTime"`
}

// Store is the Dataproc Metastore store.
type Store interface {
	CreateService(ctx context.Context, projectID, location string, s Service) error
	GetService(ctx context.Context, projectID, location, name string) (Service, error)
	UpdateService(ctx context.Context, projectID, location string, s Service) error
	// UpdateServiceAtomic reads, mutates, and writes a service under one lock
	// (memory) or a Serializable SELECT ... FOR UPDATE transaction (postgres),
	// so a concurrent writer can't land between the read and the write. mutate
	// returns the next Service, or an error to abort without writing.
	UpdateServiceAtomic(ctx context.Context, projectID, location, name string, mutate func(Service) (Service, error)) (Service, error)
	DeleteService(ctx context.Context, projectID, location, name string) error
	ListServices(ctx context.Context, projectID, location string) ([]Service, error)

	CreateBackup(ctx context.Context, projectID, location, serviceName string, b Backup) error
	GetBackup(ctx context.Context, projectID, location, serviceName, name string) (Backup, error)
	DeleteBackup(ctx context.Context, projectID, location, serviceName, name string) error
	ListBackups(ctx context.Context, projectID, location, serviceName string) ([]Backup, error)

	CreateMetadataImport(ctx context.Context, projectID, location, serviceName string, m MetadataImport) error
	GetMetadataImport(ctx context.Context, projectID, location, serviceName, name string) (MetadataImport, error)
	UpdateMetadataImport(ctx context.Context, projectID, location, serviceName string, m MetadataImport) error
	// UpdateMetadataImportAtomic mirrors UpdateServiceAtomic for a metadata
	// import under a service.
	UpdateMetadataImportAtomic(ctx context.Context, projectID, location, serviceName, name string, mutate func(MetadataImport) (MetadataImport, error)) (MetadataImport, error)
	ListMetadataImports(ctx context.Context, projectID, location, serviceName string) ([]MetadataImport, error)

	CreateOperation(ctx context.Context, projectID, location string, op Operation) error
	GetOperation(ctx context.Context, projectID, location, id string) (Operation, error)
	ListOperations(ctx context.Context, projectID, location string) ([]Operation, error)

	Reset(ctx context.Context)
}
