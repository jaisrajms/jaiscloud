// Package logging provides the Cloud Logging (v2) store. Log entries live in a
// dedicated jc_log_entries table (mirroring the Secret Manager / Functions
// store conventions: project-scoped, clock.Now for absent timestamps, and a
// snapshot pair for the admin export/import surface).
package logging

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// Sentinel errors returned by the store for the sink/exclusion/metric
// registries. The service maps them to gRPC status codes (errors.Is-compatible,
// matching the datastore/monitoring conventions).
var (
	ErrSinkNotFound      = errors.New("SinkNotFound")
	ErrSinkExists        = errors.New("SinkExists")
	ErrExclusionNotFound = errors.New("ExclusionNotFound")
	ErrExclusionExists   = errors.New("ExclusionExists")
	ErrMetricNotFound    = errors.New("MetricNotFound")
	ErrMetricExists      = errors.New("MetricExists")
)

// LogEntry is a stored Cloud Logging entry. LogName is the full resource name
// ("projects/{p}/logs/{l}"); Severity is the numeric google.logging.type
// LogSeverity value; ID is a monotonic sequence used to break ties when
// entries share a timestamp.
type LogEntry struct {
	ID             int64             `json:"id"`
	LogName        string            `json:"logName"`
	ResourceType   string            `json:"resourceType"`
	ResourceLabels map[string]string `json:"resourceLabels,omitempty"`
	Severity       int               `json:"severity"`
	PayloadType    string            `json:"payloadType,omitempty"` // "" or "text" or "json"
	TextPayload    string            `json:"textPayload,omitempty"`
	JsonPayload    map[string]any    `json:"jsonPayload,omitempty"`
	Timestamp      time.Time         `json:"timestamp"`
	// ReceiveTimestamp is the time Cloud Logging received the entry (output
	// only). It is stamped once at write and returned on every read.
	ReceiveTimestamp time.Time         `json:"receiveTimestamp,omitempty"`
	InsertID         string            `json:"insertId,omitempty"`
	Labels           map[string]string `json:"labels,omitempty"`
}

// LogSink is a stored Cloud Logging sink (an export route). Name is the
// client-assigned short sink id (the trailing segment of the resource name);
// the full resource name is built by the service. Exclusions are the sink's
// inline exclusion filters. CreateTime/UpdateTime are server-assigned.
type LogSink struct {
	Name            string         `json:"name"`
	Destination     string         `json:"destination"`
	Filter          string         `json:"filter,omitempty"`
	Description     string         `json:"description,omitempty"`
	Disabled        bool           `json:"disabled,omitempty"`
	Exclusions      []LogExclusion `json:"exclusions,omitempty"`
	WriterIdentity  string         `json:"writerIdentity,omitempty"`
	IncludeChildren bool           `json:"includeChildren,omitempty"`
	// InterceptChildren is the organization/folder-only legacy flag (REST-only:
	// the pinned gRPC proto dropped `intercept_children`).
	InterceptChildren   bool                `json:"interceptChildren,omitempty"`
	OutputVersionFormat string              `json:"outputVersionFormat,omitempty"` // deprecated: "V2" | "V1"
	BigQueryOptions     *LogBigQueryOptions `json:"bigqueryOptions,omitempty"`
	CreateTime          time.Time           `json:"createTime,omitempty"`
	UpdateTime          time.Time           `json:"updateTime,omitempty"`
}

// LogBigQueryOptions is a sink's BigQuery destination options.
type LogBigQueryOptions struct {
	UsePartitionedTables            bool `json:"usePartitionedTables,omitempty"`
	UsesTimestampColumnPartitioning bool `json:"usesTimestampColumnPartitioning,omitempty"`
}

// LogExclusion is a stored Cloud Logging exclusion. For a resource-level
// exclusion (projects.exclusions.*) Name is the short exclusion id; for a
// sink's inline exclusion it is also the short id. The service builds the full
// resource name where the wire form carries one.
type LogExclusion struct {
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	Filter      string    `json:"filter"`
	Disabled    bool      `json:"disabled,omitempty"`
	CreateTime  time.Time `json:"createTime,omitempty"`
	UpdateTime  time.Time `json:"updateTime,omitempty"`
}

// LogMetricLabel is one label of a logs-based metric's descriptor.
type LogMetricLabel struct {
	Key         string `json:"key"`
	ValueType   string `json:"valueType,omitempty"` // STRING | BOOL | INT64
	Description string `json:"description,omitempty"`
}

// LogMetricDescriptor is the configurable part of a logs-based metric's
// MetricDescriptor. Name/Type/Description are output-only and synthesized by
// the service from the metric's name/description and its scope; they are
// carried here only so a fetched metric can be rendered without the caller
// rebuilding the invariant fields.
type LogMetricDescriptor struct {
	Name        string           `json:"name,omitempty"`
	Type        string           `json:"type,omitempty"`
	Description string           `json:"description,omitempty"`
	MetricKind  string           `json:"metricKind,omitempty"` // DELTA | GAUGE | CUMULATIVE
	ValueType   string           `json:"valueType,omitempty"`  // INT64 | DOUBLE | DISTRIBUTION
	Unit        string           `json:"unit,omitempty"`
	DisplayName string           `json:"displayName,omitempty"`
	Labels      []LogMetricLabel `json:"labels,omitempty"`
}

// LogMetric is a stored logs-based metric. Name is the decoded client-assigned
// metric id (the [METRIC_ID] part of the resource name, which may contain
// slashes); the full resource name is built by the service. BucketOptions is
// the opaque, canonical-JSON encoded google.api.Distribution.BucketOptions
// (stored verbatim so both transports round-trip it without the store
// depending on protobuf).
type LogMetric struct {
	Name            string              `json:"name"`
	Description     string              `json:"description,omitempty"`
	Filter          string              `json:"filter"`
	Disabled        bool                `json:"disabled,omitempty"`
	BucketName      string              `json:"bucketName,omitempty"`
	ValueExtractor  string              `json:"valueExtractor,omitempty"`
	LabelExtractors map[string]string   `json:"labelExtractors,omitempty"`
	BucketOptions   json.RawMessage     `json:"bucketOptions,omitempty"`
	Descriptor      LogMetricDescriptor `json:"descriptor,omitempty"`
	CreateTime      time.Time           `json:"createTime,omitempty"`
	UpdateTime      time.Time           `json:"updateTime,omitempty"`
}

// Store is the Cloud Logging store. Entries are isolated by scope parent, the
// two-segment Cloud Logging resource container ("projects/p",
// "organizations/123", "folders/f", "billingAccounts/b"); queries return
// entries ordered by (timestamp, id) ascending. Sinks and resource-level
// exclusions are also scope-scoped.
type Store interface {
	Write(ctx context.Context, scope string, e LogEntry) error
	// List returns every entry in the scope, ordered by (timestamp, id).
	List(ctx context.Context, scope string) ([]LogEntry, error)
	// ListLogs returns the distinct full log names under the scope, sorted.
	ListLogs(ctx context.Context, scope string) ([]string, error)
	// DeleteLog deletes every entry whose log name equals logName.
	DeleteLog(ctx context.Context, scope, logName string) error

	// Sink registry. CreateSink returns ErrSinkExists when the scope already
	// holds a sink with the same name; GetSink returns ErrSinkNotFound.
	CreateSink(ctx context.Context, scope string, s LogSink) error
	GetSink(ctx context.Context, scope, name string) (LogSink, error)
	// ListSinks returns the scope's sinks ordered by name.
	ListSinks(ctx context.Context, scope string) ([]LogSink, error)
	// UpdateSink replaces the stored sink (matched by Name) or returns
	// ErrSinkNotFound.
	UpdateSink(ctx context.Context, scope string, s LogSink) error
	DeleteSink(ctx context.Context, scope, name string) error

	// Resource-level exclusion registry (projects.exclusions.*), keyed by the
	// short exclusion id.
	CreateExclusion(ctx context.Context, scope string, e LogExclusion) error
	GetExclusion(ctx context.Context, scope, name string) (LogExclusion, error)
	// ListExclusions returns the scope's exclusions ordered by name.
	ListExclusions(ctx context.Context, scope string) ([]LogExclusion, error)
	UpdateExclusion(ctx context.Context, scope string, e LogExclusion) error
	DeleteExclusion(ctx context.Context, scope, name string) error

	// Logs-based metric registry, keyed by the decoded metric id (which may
	// contain slashes). CreateMetric returns ErrMetricExists on a duplicate;
	// GetMetric/UpdateMetric/DeleteMetric return ErrMetricNotFound.
	CreateMetric(ctx context.Context, scope string, m LogMetric) error
	GetMetric(ctx context.Context, scope, name string) (LogMetric, error)
	// ListMetrics returns the scope's metrics ordered by name.
	ListMetrics(ctx context.Context, scope string) ([]LogMetric, error)
	UpdateMetric(ctx context.Context, scope string, m LogMetric) error
	DeleteMetric(ctx context.Context, scope, name string) error

	// Admin v2 registries (log buckets, views, links, log scopes, and the
	// per-scope Settings/CMEK records). scope is the owning resource parent and
	// the id/name the short client-assigned identifier. See admin.go.
	CreateBucket(ctx context.Context, scope string, b LogBucket) error
	GetBucket(ctx context.Context, scope, id string) (LogBucket, error)
	ListBuckets(ctx context.Context, scope string) ([]LogBucket, error)
	UpdateBucket(ctx context.Context, scope string, b LogBucket) error
	DeleteBucket(ctx context.Context, scope, id string) error

	CreateView(ctx context.Context, scope string, v LogView) error
	GetView(ctx context.Context, scope, id string) (LogView, error)
	ListViews(ctx context.Context, scope string) ([]LogView, error)
	UpdateView(ctx context.Context, scope string, v LogView) error
	DeleteView(ctx context.Context, scope, id string) error

	CreateLink(ctx context.Context, scope string, l LogLink) error
	GetLink(ctx context.Context, scope, id string) (LogLink, error)
	ListLinks(ctx context.Context, scope string) ([]LogLink, error)
	DeleteLink(ctx context.Context, scope, id string) error

	CreateLogScope(ctx context.Context, scope string, ls LogScope) error
	GetLogScope(ctx context.Context, scope, id string) (LogScope, error)
	ListLogScopes(ctx context.Context, scope string) ([]LogScope, error)
	UpdateLogScope(ctx context.Context, scope string, ls LogScope) error
	DeleteLogScope(ctx context.Context, scope, id string) error

	GetSettings(ctx context.Context, scope string) (LogSettings, bool, error)
	SetSettings(ctx context.Context, scope string, s LogSettings) error
	GetCmekSettings(ctx context.Context, scope string) (LogCmekSettings, bool, error)
	SetCmekSettings(ctx context.Context, scope string, c LogCmekSettings) error

	Reset(ctx context.Context)
}
