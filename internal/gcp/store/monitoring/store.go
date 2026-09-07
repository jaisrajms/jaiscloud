// Package monitoring provides the Cloud Monitoring (v3) store — the Amazon
// CloudWatch metrics+alarms analogue. It holds the metric descriptor catalog,
// the time-series data plane, and the alert-policy (alarm) registry. Metric
// descriptors are keyed by metric type, time series by their fully-specified
// (metric, resource, labels) identity, and alert policies by a server-assigned
// id. All state is project-scoped; timestamps default to clock.Now; a snapshot
// pair backs the admin export/import surface.
package monitoring

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"
)

// Sentinel errors returned by the store, mapped to gRPC status codes by the
// service (errors.Is-compatible, matching the datastore/logging conventions).
var (
	ErrMetricDescriptorNotFound = errors.New("MetricDescriptorNotFound")
	ErrAlertPolicyNotFound      = errors.New("AlertPolicyNotFound")
	ErrAlertPolicyExists        = errors.New("AlertPolicyExists")
)

// LabelDescriptor mirrors google.api.LabelDescriptor (the label key, its value
// type enum, and a description).
type LabelDescriptor struct {
	Key         string `json:"key"`
	ValueType   int32  `json:"valueType,omitempty"`
	Description string `json:"description,omitempty"`
}

// MetricDescriptor is the metric catalog entry: a metric type and its schema.
// MetricKind and ValueType carry the numeric google.api.MetricDescriptor enum
// values.
type MetricDescriptor struct {
	Type                   string            `json:"type"`
	MetricKind             int32             `json:"metricKind,omitempty"`
	ValueType              int32             `json:"valueType,omitempty"`
	Unit                   string            `json:"unit,omitempty"`
	Description            string            `json:"description,omitempty"`
	DisplayName            string            `json:"displayName,omitempty"`
	Labels                 []LabelDescriptor `json:"labels,omitempty"`
	MonitoredResourceTypes []string          `json:"monitoredResourceTypes,omitempty"`
}

// TypedValue is a single strongly-typed data-point value. Distribution values
// are not supported by the emulator and are rejected during transcoding.
type TypedValue struct {
	BoolValue   *bool    `json:"boolValue,omitempty"`
	Int64Value  *int64   `json:"int64Value,omitempty"`
	DoubleValue *float64 `json:"doubleValue,omitempty"`
	StringValue *string  `json:"stringValue,omitempty"`
}

// Point is a single data point: a time interval plus a typed value.
type Point struct {
	StartTime time.Time  `json:"startTime,omitempty"`
	EndTime   time.Time  `json:"endTime"`
	Value     TypedValue `json:"value"`
}

// TimeSeries is a collection of points identified by a fully-specified metric
// and monitored resource. The metric/resource label maps are the identity; the
// store appends points to an existing series with the same identity.
type TimeSeries struct {
	MetricType     string            `json:"metricType"`
	MetricLabels   map[string]string `json:"metricLabels,omitempty"`
	ResourceType   string            `json:"resourceType"`
	ResourceLabels map[string]string `json:"resourceLabels,omitempty"`
	MetricKind     int32             `json:"metricKind,omitempty"`
	ValueType      int32             `json:"valueType,omitempty"`
	Unit           string            `json:"unit,omitempty"`
	Points         []Point           `json:"points,omitempty"`
}

// AlertPolicy is a stored alerting policy (the CloudWatch alarm analogue). ID
// is the server-assigned policy id (the trailing segment of the resource name).
// Documentation and Conditions carry the proto sub-messages as opaque JSON so
// they round-trip faithfully without the store depending on the proto package;
// the emulator never evaluates them.
type AlertPolicy struct {
	ID                   string            `json:"id,omitempty"`
	DisplayName          string            `json:"displayName,omitempty"`
	Documentation        json.RawMessage   `json:"documentation,omitempty"`
	Conditions           []json.RawMessage `json:"conditions,omitempty"`
	Combiner             int32             `json:"combiner,omitempty"`
	Enabled              *bool             `json:"enabled,omitempty"`
	NotificationChannels []string          `json:"notificationChannels,omitempty"`
	UserLabels           map[string]string `json:"userLabels,omitempty"`
}

// Store is the Cloud Monitoring store. All state is project-scoped.
type Store interface {
	// Metric descriptor catalog.
	CreateMetricDescriptor(ctx context.Context, project string, d MetricDescriptor) (MetricDescriptor, error)
	GetMetricDescriptor(ctx context.Context, project, metricType string) (MetricDescriptor, error)
	ListMetricDescriptors(ctx context.Context, project string) ([]MetricDescriptor, error)
	DeleteMetricDescriptor(ctx context.Context, project, metricType string) error

	// Time series data plane.
	CreateTimeSeries(ctx context.Context, project string, ts TimeSeries) error
	ListTimeSeries(ctx context.Context, project string) ([]TimeSeries, error)

	// Alert policy (alarm) registry.
	CreateAlertPolicy(ctx context.Context, project string, p AlertPolicy) error
	GetAlertPolicy(ctx context.Context, project, id string) (AlertPolicy, error)
	ListAlertPolicies(ctx context.Context, project string) ([]AlertPolicy, error)
	UpdateAlertPolicy(ctx context.Context, project string, p AlertPolicy) error
	// UpdateAlertPolicyAtomic performs a locked get-mutate-set cycle: mutate
	// receives the current policy and returns the version to persist, or an
	// error to abort without writing. Unlike a separate GetAlertPolicy
	// followed by UpdateAlertPolicy, this is atomic with respect to
	// concurrent updates on the same policy, so a masked PATCH that merges
	// only a subset of fields can't lose a concurrent PATCH's changes to
	// other fields.
	UpdateAlertPolicyAtomic(ctx context.Context, project, id string, mutate func(AlertPolicy) (AlertPolicy, error)) (AlertPolicy, error)
	DeleteAlertPolicy(ctx context.Context, project, id string) error

	Reset(ctx context.Context)
}

// mergeMetricDescriptor computes the result of a CreateMetricDescriptor upsert
// (matching real Cloud Monitoring semantics): the incoming descriptor's fields
// win, but existing labels are unioned by key (never removed) and an empty
// incoming MonitoredResourceTypes keeps the existing value. The merged label
// set is sorted by key for deterministic storage.
func mergeMetricDescriptor(existing, incoming MetricDescriptor) MetricDescriptor {
	merged := incoming
	if len(merged.MonitoredResourceTypes) == 0 {
		merged.MonitoredResourceTypes = existing.MonitoredResourceTypes
	}
	byKey := make(map[string]LabelDescriptor, len(existing.Labels)+len(incoming.Labels))
	for _, l := range existing.Labels {
		byKey[l.Key] = l
	}
	for _, l := range incoming.Labels {
		byKey[l.Key] = l
	}
	merged.Labels = make([]LabelDescriptor, 0, len(byKey))
	for _, l := range byKey {
		merged.Labels = append(merged.Labels, l)
	}
	sort.Slice(merged.Labels, func(i, j int) bool { return merged.Labels[i].Key < merged.Labels[j].Key })
	return merged
}
