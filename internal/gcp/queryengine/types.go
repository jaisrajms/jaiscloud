// Package queryengine is the transport-neutral SQL scratch engine that backs
// bigquery jobs.query/getQueryResults. It translates a bounded BigQuery
// Standard SQL subset to SQLite (pure-Go modernc.org/sqlite), hydrates the
// referenced tables into a per-query in-memory SQLite scratch database, runs
// the query and returns the ordered result set. SQLite is disposable scratch:
// it is never persisted and the caller's catalog remains the source of truth.
//
// The engine deliberately implements only the frozen v1 subset (see
// docs/gcp-bigquery-sql-engine.md). Any construct it cannot translate fails
// loud (UnsupportedError) rather than silently returning wrong rows.
package queryengine

import (
	"context"
	"errors"
)

// Field is one BigQuery table field. Type uses the BigQuery type names
// (STRING, INT64, FLOAT64, BOOL, BYTES, DATE, ...); Mode is NULLABLE,
// REQUIRED or REPEATED (empty is treated as NULLABLE).
type Field struct {
	Name string
	Type string
	Mode string
}

// Table is a source table resolved through a Catalog: its flat schema and all
// rows as JSON-decoded objects keyed by field name. Nested and REPEATED columns
// are not supported by v1.
type Table struct {
	Project string
	Dataset string
	Table   string
	Fields  []Field
	Rows    []map[string]any
}

// Catalog resolves a fully-qualified table to its schema and rows. The engine
// decorates a not-found table with the reference it came from; a Catalog should
// return ErrTableNotFound (wrapped is fine) for a missing table.
type Catalog interface {
	Table(ctx context.Context, project, dataset, table string) (Table, error)
}

// Statement type values, matching the Discovery QueryResponse.statementType
// enum for the subset the engine executes.
const (
	StatementSelect              = "SELECT"
	StatementCreateTable         = "CREATE_TABLE"
	StatementCreateTableAsSelect = "CREATE_TABLE_AS_SELECT"
	StatementCreateSchema        = "CREATE_SCHEMA"
	StatementDropTable           = "DROP_TABLE"
	StatementDropSchema          = "DROP_SCHEMA"
	StatementInsert              = "INSERT"
	StatementUpdate              = "UPDATE"
	StatementDelete              = "DELETE"
	StatementTruncateTable       = "TRUNCATE_TABLE"
)

// Mutator is the write side of the catalog, used by DDL/DML statements. The
// source of truth is the store behind it: SQLite remains disposable scratch,
// and every mutation is written back through these methods so a subsequent
// query, tables.get or tabledata.list observes it.
type Mutator interface {
	Catalog
	// ListTables returns the table IDs in a dataset (used to enforce DROP
	// SCHEMA RESTRICT unless CASCADE is given).
	ListTables(ctx context.Context, project, dataset string) ([]string, error)
	// CreateTable creates a table with the given schema and rows (rows may be
	// empty for a schema-only CREATE TABLE). It returns ErrTableExists when the
	// table already exists.
	CreateTable(ctx context.Context, t Table) error
	// DropTable removes a table. It returns ErrTableNotFound when missing.
	DropTable(ctx context.Context, project, dataset, table string) error
	// CreateDataset creates a dataset. It returns ErrDatasetExists when it
	// already exists.
	CreateDataset(ctx context.Context, project, dataset string) error
	// DropDataset removes a dataset and everything in it. It returns
	// ErrDatasetNotFound when missing.
	DropDataset(ctx context.Context, project, dataset string) error
	// ReplaceRows atomically replaces every row of a table with rows. It
	// returns ErrTableNotFound when the table is missing.
	ReplaceRows(ctx context.Context, project, dataset, table string, rows []map[string]any) error
}

// Sentinel errors the Mutator returns so the engine can map them to BigQuery
// status codes (AlreadyExists -> 409, NotFound -> 404) instead of a generic
// InvalidQuery. Other errors become ErrUnsupported. Missing tables reuse
// ErrTableNotFound (already defined above for the Catalog).
var (
	ErrTableExists     = errors.New("table already exists")
	ErrDatasetExists   = errors.New("dataset already exists")
	ErrDatasetNotFound = errors.New("dataset not found")
)

// Request is one jobs.query/getQueryResults execution.
type Request struct {
	// Project is the project the query job runs in.
	Project string
	// Query is the BigQuery Standard SQL text.
	Query string
	// DefaultProject/DefaultDataset resolve unqualified and dataset-qualified
	// table references (the jobs.query defaultDataset). DefaultProject falls
	// back to Project when empty.
	DefaultProject string
	DefaultDataset string
	// DryRun validates the statement (and infers the result schema for a
	// SELECT) but performs no DDL/DML mutation.
	DryRun bool
}

// Result is the executed result set in output-column order.
type Result struct {
	// StatementType is the executed statement kind (SELECT, CREATE_TABLE, ...),
	// matching the Discovery QueryResponse.statementType enum.
	StatementType string
	Fields        []Field
	// Rows holds one slice per output column, aligned with Fields. Scalar
	// values are normalized to their BigQuery representation (BOOL is a Go
	// bool, INT64 an int64, FLOAT64 a float64, BYTES a []byte, everything else
	// a string) so the transport can render the wire TableCell shape.
	Rows [][]any
	// NumDMLAffectedRows is the row count affected by an INSERT/UPDATE/DELETE
	// (Discovery QueryResponse.numDmlAffectedRows). Zero for other statements.
	NumDMLAffectedRows int64
}

// ErrUnsupported marks a construct the v1 subset does not implement (or a
// runtime execution failure); the caller must map it to an InvalidQuery error,
// never to a successful response.
var ErrUnsupported = errors.New("unsupported query")

// ErrTableNotFound marks a referenced table that does not exist in the catalog.
var ErrTableNotFound = errors.New("table not found")

// UnsupportedError is returned for a BigQuery construct the v1 subset does not
// translate, or for an execution failure. It is errors.Is(ErrUnsupported).
type UnsupportedError struct {
	Reason string
}

func (e *UnsupportedError) Error() string { return ErrUnsupported.Error() + ": " + e.Reason }

// Is lets errors.Is(err, ErrUnsupported) match.
func (e *UnsupportedError) Is(target error) bool { return target == ErrUnsupported }

// TableNotFoundError is returned for a referenced table missing from the
// catalog. It is errors.Is(ErrTableNotFound).
type TableNotFoundError struct {
	Project string
	Dataset string
	Table   string
}

func (e *TableNotFoundError) Error() string {
	return ErrTableNotFound.Error() + ": table " + e.Project + "." + e.Dataset + "." + e.Table
}

// Is lets errors.Is(err, ErrTableNotFound) match.
func (e *TableNotFoundError) Is(target error) bool { return target == ErrTableNotFound }

// DatasetNotFoundError is returned by DDL when a referenced dataset is missing
// (for example DROP SCHEMA without IF EXISTS). It is errors.Is(ErrDatasetNotFound).
type DatasetNotFoundError struct {
	Project string
	Dataset string
}

func (e *DatasetNotFoundError) Error() string {
	return ErrDatasetNotFound.Error() + ": dataset " + e.Project + "." + e.Dataset
}

// Is lets errors.Is(err, ErrDatasetNotFound) match.
func (e *DatasetNotFoundError) Is(target error) bool { return target == ErrDatasetNotFound }

func unsupported(reason string) error { return &UnsupportedError{Reason: reason} }
