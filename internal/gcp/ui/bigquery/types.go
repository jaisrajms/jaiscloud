// Package bigqueryui serves the BigQuery UI API. Handlers call the BigQuery
// provider directly (in-process) rather than over the wire, and reuse its
// Discovery-shaped response maps so the console shows exactly what the API
// returns.
//
// Scope is the control plane: datasets, tables and jobs. A table page previews
// rows via tabledata.list and can stream rows in via tabledata.insertAll; the
// SQL workspace runs jobs.query. Every response is the provider's
// Discovery-shaped body, passed through verbatim so the console renders exactly
// what the wire API returns.
package bigqueryui

// Dataset is the UI summary of a BigQuery dataset (the datasets.list shape).
type Dataset struct {
	ID           string            `json:"id"`
	DatasetID    string            `json:"datasetId"`
	ProjectID    string            `json:"projectId,omitempty"`
	Location     string            `json:"location,omitempty"`
	FriendlyName string            `json:"friendlyName,omitempty"`
	Labels       map[string]string `json:"labels,omitempty"`
}

// ListDatasetsResponse is the response for GET /datasets.
type ListDatasetsResponse struct {
	Datasets      []Dataset `json:"datasets"`
	Total         int       `json:"total"`
	NextPageToken string    `json:"nextPageToken,omitempty"`
}

// CreateDatasetRequest is the body for POST /datasets. Only datasetId is
// required; the rest mirror the writable fields of the Dataset resource.
type CreateDatasetRequest struct {
	DatasetID    string            `json:"datasetId"`
	Location     string            `json:"location,omitempty"`
	FriendlyName string            `json:"friendlyName,omitempty"`
	Description  string            `json:"description,omitempty"`
	Labels       map[string]string `json:"labels,omitempty"`
}

// Table is the UI summary of a BigQuery table (the tables.list shape).
type Table struct {
	ID           string `json:"id"`
	DatasetID    string `json:"datasetId"`
	TableID      string `json:"tableId"`
	Type         string `json:"type,omitempty"`
	FriendlyName string `json:"friendlyName,omitempty"`
	CreationTime string `json:"creationTime,omitempty"`
}

// CreateTableRequest is the body for POST /datasets/{dataset}/tables. Only
// tableId is required; schema is a TableSchema object (its `fields` array).
type CreateTableRequest struct {
	TableID      string         `json:"tableId"`
	Schema       map[string]any `json:"schema,omitempty"`
	FriendlyName string         `json:"friendlyName,omitempty"`
	Description  string         `json:"description,omitempty"`
}

// ListTablesResponse is the response for GET /datasets/{dataset}/tables.
type ListTablesResponse struct {
	Tables        []Table `json:"tables"`
	Total         int     `json:"total"`
	NextPageToken string  `json:"nextPageToken,omitempty"`
}

// Job is the UI summary of a BigQuery job (the jobs.list shape, flattened).
type Job struct {
	ID            string `json:"id"`
	JobID         string `json:"jobId"`
	State         string `json:"state,omitempty"`
	StatementType string `json:"statementType,omitempty"`
	Query         string `json:"query,omitempty"`
}

// ListJobsResponse is the response for GET /jobs.
type ListJobsResponse struct {
	Jobs          []Job  `json:"jobs"`
	Total         int    `json:"total"`
	NextPageToken string `json:"nextPageToken,omitempty"`
}

// ListRowsResponse is the response for GET /datasets/{dataset}/tables/{table}/rows.
// Rows carry the provider's TableRow encoding verbatim so the preview matches
// the wire API.
type ListRowsResponse struct {
	Rows          []map[string]any `json:"rows"`
	TotalRows     string           `json:"totalRows"`
	NextPageToken string           `json:"nextPageToken,omitempty"`
}

// DatasetReference identifies a dataset for a query's defaultDataset. projectId
// defaults to the console's current project when omitted.
type DatasetReference struct {
	ProjectID string `json:"projectId,omitempty"`
	DatasetID string `json:"datasetId"`
}

// QueryRequest is the body for POST /query. It mirrors the writable fields of
// the jobs.query request: query is the SQL text, defaultDataset qualifies
// unqualified table references, dryRun validates without executing or mutating,
// and useLegacySql is rejected by the provider (standard SQL only).
type QueryRequest struct {
	Query          string            `json:"query"`
	DefaultDataset *DatasetReference `json:"defaultDataset,omitempty"`
	DryRun         bool              `json:"dryRun,omitempty"`
	UseLegacySQL   bool              `json:"useLegacySql,omitempty"`
	Location       string            `json:"location,omitempty"`
}

// InsertRowsRequest is the body for
// POST /datasets/{dataset}/tables/{table}/rows. rows carries the
// tabledata.insertAll rows; each row's json is one table row and its optional
// insertId deduplicates repeats.
type InsertRowsRequest struct {
	Rows                []InsertRow `json:"rows"`
	SkipInvalidRows     bool        `json:"skipInvalidRows,omitempty"`
	IgnoreUnknownValues bool        `json:"ignoreUnknownValues,omitempty"`
}

// InsertRow is one tabledata.insertAll row: the row's field values plus an
// optional insertId used for best-effort duplicate suppression.
type InsertRow struct {
	InsertID string         `json:"insertId,omitempty"`
	JSON     map[string]any `json:"json"`
}
