// Package monitoring provides the Cloud Monitoring (v3) store — the Amazon
// CloudWatch metrics+alarms analogue. It holds the metric descriptor catalog,
// the time-series data plane, the alert-policy (alarm) registry, the
// notification-channel registry, and the Service Monitoring services + SLOs.
// Metric descriptors are keyed by metric type, time series by their
// fully-specified (metric, resource, labels) identity, alert policies and
// notification channels by a server-assigned id, and SLOs by (service id, id).
// All state is project-scoped; timestamps default to clock.Now; a snapshot pair
// backs the admin export/import surface.
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
	ErrMetricDescriptorNotFound      = errors.New("MetricDescriptorNotFound")
	ErrAlertPolicyNotFound           = errors.New("AlertPolicyNotFound")
	ErrAlertPolicyExists             = errors.New("AlertPolicyExists")
	ErrNotificationChannelNotFound   = errors.New("NotificationChannelNotFound")
	ErrNotificationChannelExists     = errors.New("NotificationChannelExists")
	ErrIncidentNotFound              = errors.New("IncidentNotFound")
	ErrIncidentExists                = errors.New("IncidentExists")
	ErrServiceNotFound               = errors.New("ServiceNotFound")
	ErrServiceExists                 = errors.New("ServiceExists")
	ErrServiceLevelObjectiveNotFound = errors.New("ServiceLevelObjectiveNotFound")
	ErrServiceLevelObjectiveExists   = errors.New("ServiceLevelObjectiveExists")
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

// TypedValue is a single strongly-typed data-point value. At most one field is
// set. DistributionValue carries a histogram (google.api.Distribution); the
// emulator transcodes it losslessly to and from the wire form.
type TypedValue struct {
	BoolValue         *bool         `json:"boolValue,omitempty"`
	Int64Value        *int64        `json:"int64Value,omitempty"`
	DoubleValue       *float64      `json:"doubleValue,omitempty"`
	StringValue       *string       `json:"stringValue,omitempty"`
	DistributionValue *Distribution `json:"distributionValue,omitempty"`
}

// Distribution mirrors google.api.Distribution: a summary (count, mean, sum of
// squared deviations, optional range) plus an optional histogram described by
// one of the three BucketOptions shapes.
type Distribution struct {
	Count                 int64              `json:"count,omitempty"`
	Mean                  float64            `json:"mean,omitempty"`
	SumOfSquaredDeviation float64            `json:"sumOfSquaredDeviation,omitempty"`
	Range                 *DistributionRange `json:"range,omitempty"`
	BucketOptions         *BucketOptions     `json:"bucketOptions,omitempty"`
	BucketCounts          []int64            `json:"bucketCounts,omitempty"`
	Exemplars             []Exemplar         `json:"exemplars,omitempty"`
}

// DistributionRange is the optional [min, max] range of a Distribution.
type DistributionRange struct {
	Min float64 `json:"min,omitempty"`
	Max float64 `json:"max,omitempty"`
}

// BucketOptions is the Distribution histogram bucket-boundary oneof. Exactly
// one field is expected to be set.
type BucketOptions struct {
	Linear      *LinearBuckets      `json:"linearBuckets,omitempty"`
	Exponential *ExponentialBuckets `json:"exponentialBuckets,omitempty"`
	Explicit    *ExplicitBuckets    `json:"explicitBuckets,omitempty"`
}

// LinearBuckets describes a histogram with constant-width buckets.
type LinearBuckets struct {
	NumFiniteBuckets int32   `json:"numFiniteBuckets,omitempty"`
	Width            float64 `json:"width,omitempty"`
	Offset           float64 `json:"offset,omitempty"`
}

// ExponentialBuckets describes a histogram with geometrically-growing buckets.
type ExponentialBuckets struct {
	NumFiniteBuckets int32   `json:"numFiniteBuckets,omitempty"`
	GrowthFactor     float64 `json:"growthFactor,omitempty"`
	Scale            float64 `json:"scale,omitempty"`
}

// ExplicitBuckets describes a histogram with explicitly-listed upper bounds.
type ExplicitBuckets struct {
	Bounds []float64 `json:"bounds,omitempty"`
}

// Exemplar is a single sampled observation attached to a Distribution.
// Attachments carry the marshalled google.protobuf.Any payloads (opaque bytes
// so the store need not depend on the proto registry).
type Exemplar struct {
	Value       float64   `json:"value,omitempty"`
	Timestamp   time.Time `json:"timestamp,omitempty"`
	Attachments [][]byte  `json:"attachments,omitempty"`
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

// NotificationChannel is a stored notification destination (the CloudWatch
// SNS-topic analogue). ID is the server-assigned channel id (the trailing
// segment of the resource name). Labels carry the type-specific configuration
// (e.g. {"topic": "projects/p/topics/t"} for pubsub, {"email_address": "..."}
// for email). VerificationStatus carries the numeric
// google.monitoring.v3.NotificationChannel.VerificationStatus enum value.
type NotificationChannel struct {
	ID                 string            `json:"id,omitempty"`
	Type               string            `json:"type,omitempty"`
	DisplayName        string            `json:"displayName,omitempty"`
	Description        string            `json:"description,omitempty"`
	Labels             map[string]string `json:"labels,omitempty"`
	UserLabels         map[string]string `json:"userLabels,omitempty"`
	Enabled            *bool             `json:"enabled,omitempty"`
	VerificationStatus int32             `json:"verificationStatus,omitempty"`
	CreateTime         time.Time         `json:"createTime,omitempty"`
	UpdateTime         time.Time         `json:"updateTime,omitempty"`
}

// Service is a stored Service Monitoring service. ID is the service id (the
// trailing segment of the resource name); it is either client-supplied or
// server-generated. Identifier carries the proto Service identifier oneof as
// opaque JSON (e.g. {"custom":{...}} or {"cloudEndpoints":{...}}),
// BasicService carries the separate Service.BasicService field (a basic
// service is defined by service type + labels instead of the identifier
// oneof), and Telemetry carries Service.Telemetry. Each opaque field round-trips
// faithfully without the store depending on the proto package. A valid service
// sets exactly one identity: an identifier-oneof member or BasicService.
type Service struct {
	ID           string            `json:"id,omitempty"`
	DisplayName  string            `json:"displayName,omitempty"`
	Identifier   json.RawMessage   `json:"identifier,omitempty"`
	BasicService json.RawMessage   `json:"basicService,omitempty"`
	Telemetry    json.RawMessage   `json:"telemetry,omitempty"`
	UserLabels   map[string]string `json:"userLabels,omitempty"`
}

// ServiceLevelObjective is a stored SLO for a Service. ID is the trailing
// segment of the resource name; ServiceID is the owning service's id.
// ServiceLevelIndicator carries the proto ServiceLevelIndicator oneof as opaque
// JSON (basicSli/requestBased/windowsBased). Exactly one period field is set:
// RollingPeriod (a duration) or CalendarPeriod (the numeric CalendarPeriod enum).
// Neither RollingPeriod nor CalendarPeriod is a pointer, so a zero value means
// "unset"; a rolling period is always positive and the CalendarPeriod enum's
// zero value is CALENDAR_PERIOD_UNSPECIFIED.
type ServiceLevelObjective struct {
	ID                    string            `json:"id,omitempty"`
	ServiceID             string            `json:"serviceId,omitempty"`
	DisplayName           string            `json:"displayName,omitempty"`
	ServiceLevelIndicator json.RawMessage   `json:"serviceLevelIndicator,omitempty"`
	Goal                  float64           `json:"goal,omitempty"`
	RollingPeriod         time.Duration     `json:"rollingPeriod,omitempty"`
	CalendarPeriod        int32             `json:"calendarPeriod,omitempty"`
	UserLabels            map[string]string `json:"userLabels,omitempty"`
}

// IncidentState is the lifecycle state of a monitoring incident. GCP exposes
// incidents only on an internal API (not the v3 client library), so the
// emulator records them for observability via snapshots and slog.
type IncidentState string

const (
	IncidentOpen   IncidentState = "OPEN"
	IncidentClosed IncidentState = "CLOSED"
)

// IncidentNotification records one attempted notification delivery for an
// incident. Status is "delivered" (pubsub publish succeeded), "recorded"
// (email/webhook/sms — recorded but not sent by the emulator), "failed", or
// "skipped" (channel disabled/missing/unsupported type).
type IncidentNotification struct {
	ChannelName string    `json:"channelName,omitempty"`
	ChannelType string    `json:"channelType,omitempty"`
	Status      string    `json:"status"`
	Detail      string    `json:"detail,omitempty"`
	DeliveredAt time.Time `json:"deliveredAt"`
}

// Incident is a fired alert-policy incident. ID is a server-assigned uuid.
// ConditionName is the display name of the condition that fired (the combined
// policy state is tracked at policy granularity, so this is informational).
type Incident struct {
	ID            string                 `json:"id"`
	ProjectID     string                 `json:"projectId"`
	PolicyID      string                 `json:"policyId"`
	ConditionName string                 `json:"conditionName,omitempty"`
	State         IncidentState          `json:"state"`
	StartedAt     time.Time              `json:"startedAt"`
	EndedAt       time.Time              `json:"endedAt,omitempty"`
	Reason        string                 `json:"reason,omitempty"`
	Notifications []IncidentNotification `json:"notifications,omitempty"`
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

	// Notification channel registry.
	CreateNotificationChannel(ctx context.Context, project string, c NotificationChannel) error
	GetNotificationChannel(ctx context.Context, project, id string) (NotificationChannel, error)
	ListNotificationChannels(ctx context.Context, project string) ([]NotificationChannel, error)
	// UpdateNotificationChannelAtomic performs a locked get-mutate-set cycle so
	// a masked PATCH cannot lose a concurrent PATCH's changes (mirrors
	// UpdateAlertPolicyAtomic).
	UpdateNotificationChannelAtomic(ctx context.Context, project, id string, mutate func(NotificationChannel) (NotificationChannel, error)) (NotificationChannel, error)
	DeleteNotificationChannel(ctx context.Context, project, id string) error

	// Service Monitoring: service registry. DeleteService also removes every
	// ServiceLevelObjective owned by the service (the store owns the cascade).
	CreateService(ctx context.Context, project string, svc Service) error
	GetService(ctx context.Context, project, id string) (Service, error)
	ListServices(ctx context.Context, project string) ([]Service, error)
	// UpdateServiceAtomic performs a locked get-mutate-set cycle so a masked
	// PATCH cannot lose a concurrent PATCH's changes (mirrors
	// UpdateAlertPolicyAtomic).
	UpdateServiceAtomic(ctx context.Context, project, id string, mutate func(Service) (Service, error)) (Service, error)
	DeleteService(ctx context.Context, project, id string) error

	// Service Monitoring: service-level-objective registry, scoped to a parent
	// service id.
	CreateServiceLevelObjective(ctx context.Context, project string, slo ServiceLevelObjective) error
	GetServiceLevelObjective(ctx context.Context, project, serviceID, id string) (ServiceLevelObjective, error)
	ListServiceLevelObjectives(ctx context.Context, project, serviceID string) ([]ServiceLevelObjective, error)
	UpdateServiceLevelObjectiveAtomic(ctx context.Context, project, serviceID, id string, mutate func(ServiceLevelObjective) (ServiceLevelObjective, error)) (ServiceLevelObjective, error)
	DeleteServiceLevelObjective(ctx context.Context, project, serviceID, id string) error

	// Incident registry (recorded state for fired alert policies).
	// CreateIncident returns ErrIncidentExists when an OPEN incident already
	// exists for the policy.
	CreateIncident(ctx context.Context, inc Incident) error
	GetIncident(ctx context.Context, project, id string) (Incident, error)
	ListIncidents(ctx context.Context, project string) ([]Incident, error)
	// FindOpenIncident returns the OPEN incident for a policy, or
	// ErrIncidentNotFound when none is open.
	FindOpenIncident(ctx context.Context, project, policyID string) (Incident, error)
	UpdateIncidentAtomic(ctx context.Context, project, id string, mutate func(Incident) (Incident, error)) (Incident, error)

	// ListProjects returns the distinct project ids that hold any monitoring
	// state (descriptors, series, policies, channels, or incidents) so the
	// background evaluator can scope its work without a separate registry.
	ListProjects(ctx context.Context) ([]string, error)

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
