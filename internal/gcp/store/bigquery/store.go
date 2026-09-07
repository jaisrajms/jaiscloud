// Package bigquery provides the BigQuery store. Resources are project-scoped
// and mirror the REST surface: datasets (projects/{project}/datasets/{id}),
// tables (…/datasets/{id}/tables/{tid}), jobs (projects/{project}/jobs/{id}),
// and streamed rows (jc_bq_rows). The emulator is metadata-only: a table's
// request body is stored verbatim as JSONB, its schema and labels extracted
// as structured JSONB, and tabledata.insertAll rows are stored as ordered
// JSONB objects so tabledata.list can read them back. Jobs are never executed.
package bigquery

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrNoSuchDataset = errors.New("NoSuchDataset")
	ErrNoSuchTable   = errors.New("NoSuchTable")
	ErrNoSuchJob     = errors.New("NoSuchJob")
	ErrAlreadyExists = errors.New("AlreadyExists")
)

// Dataset is a logical BigQuery dataset. Config holds the wire request body
// verbatim; Labels is the extracted labels map.
type Dataset struct {
	ProjectID  string            `json:"projectId"`
	DatasetID  string            `json:"datasetId"`
	Config     json.RawMessage   `json:"config,omitempty"`
	Labels     map[string]string `json:"labels,omitempty"`
	CreateTime time.Time         `json:"createTime"`
	UpdateTime time.Time         `json:"updateTime"`
}

// Table is a logical BigQuery table. Schema holds the extracted table.schema
// JSON; NumRows tracks how many rows tabledata.insertAll has streamed in.
type Table struct {
	ProjectID  string            `json:"projectId"`
	DatasetID  string            `json:"datasetId"`
	TableID    string            `json:"tableId"`
	Config     json.RawMessage   `json:"config,omitempty"`
	Schema     json.RawMessage   `json:"schema,omitempty"`
	Labels     map[string]string `json:"labels,omitempty"`
	NumRows    int64             `json:"numRows"`
	CreateTime time.Time         `json:"createTime"`
	UpdateTime time.Time         `json:"updateTime"`
}

// Job is a logical BigQuery job. It is never executed — Config holds the wire
// request body verbatim and the provider reports status.state=DONE.
type Job struct {
	ProjectID  string          `json:"projectId"`
	JobID      string          `json:"jobId"`
	Config     json.RawMessage `json:"config,omitempty"`
	CreateTime time.Time       `json:"createTime"`
}

// Row is one streamed row under a table. Data holds the row's json object
// verbatim; Seq is the per-table insertion ordinal used for ordering and
// pagination.
type Row struct {
	ProjectID string          `json:"projectId"`
	DatasetID string          `json:"datasetId"`
	TableID   string          `json:"tableId"`
	Seq       int64           `json:"seq"`
	Data      json.RawMessage `json:"data,omitempty"`
}

// Store is the BigQuery store.
type Store interface {
	CreateDataset(ctx context.Context, projectID string, d Dataset) error
	GetDataset(ctx context.Context, projectID, datasetID string) (Dataset, error)
	UpdateDataset(ctx context.Context, projectID string, d Dataset) error
	// UpdateDatasetAtomic performs a locked get-mutate-set cycle: mutate
	// receives the current dataset and returns the version to persist, or an
	// error to abort without writing. Unlike a separate GetDataset followed
	// by UpdateDataset, this is atomic with respect to concurrent updates on
	// the same dataset, so two concurrent PATCH requests merging different
	// fields can't lose one or the other.
	UpdateDatasetAtomic(ctx context.Context, projectID, datasetID string, mutate func(Dataset) (Dataset, error)) (Dataset, error)
	DeleteDataset(ctx context.Context, projectID, datasetID string) error
	ListDatasets(ctx context.Context, projectID string) ([]Dataset, error)

	CreateTable(ctx context.Context, projectID, datasetID string, t Table) error
	GetTable(ctx context.Context, projectID, datasetID, tableID string) (Table, error)
	UpdateTable(ctx context.Context, projectID, datasetID string, t Table) error
	// UpdateTableAtomic performs a locked get-mutate-set cycle: mutate
	// receives the current table and returns the version to persist, or an
	// error to abort without writing. Unlike a separate GetTable followed by
	// UpdateTable, this is atomic with respect to concurrent updates on the
	// same table, so two concurrent PATCH requests merging different fields
	// can't lose one or the other.
	UpdateTableAtomic(ctx context.Context, projectID, datasetID, tableID string, mutate func(Table) (Table, error)) (Table, error)
	DeleteTable(ctx context.Context, projectID, datasetID, tableID string) error
	ListTables(ctx context.Context, projectID, datasetID string) ([]Table, error)

	CreateJob(ctx context.Context, projectID string, j Job) error
	GetJob(ctx context.Context, projectID, jobID string) (Job, error)
	DeleteJob(ctx context.Context, projectID, jobID string) error
	ListJobs(ctx context.Context, projectID string) ([]Job, error)

	InsertRows(ctx context.Context, projectID, datasetID, tableID string, rows []Row) error
	ListRows(ctx context.Context, projectID, datasetID, tableID string) ([]Row, error)

	Reset(ctx context.Context)
}
