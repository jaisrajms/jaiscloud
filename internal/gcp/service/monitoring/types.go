package monitoring

import "time"

// LabelDescriptor is a transport-neutral google.api.LabelDescriptor. ValueType
// is the proto enum name ("STRING", "BOOL", or "INT64").
type LabelDescriptor struct {
	Key         string
	ValueType   string
	Description string
}

// MonitoredResourceDescriptor is a transport-neutral monitored resource
// descriptor (the Cloud Monitoring surface sets its resource name).
type MonitoredResourceDescriptor struct {
	Name        string
	Type        string
	DisplayName string
	Description string
	Labels      []LabelDescriptor
}

// NotificationChannelDescriptor is a transport-neutral notification channel
// type descriptor.
type NotificationChannelDescriptor struct {
	Name        string
	Type        string
	DisplayName string
	Description string
	Labels      []LabelDescriptor
}

// TimeInterval is a transport-neutral point/time-range interval. A zero field
// means unbounded on that side.
type TimeInterval struct {
	Start time.Time
	End   time.Time
}

// google.api.MetricDescriptor.MetricKind and ValueType numeric enum values.
// The transports cast the generated proto enums to these same numbers, so all
// persisted descriptors and aggregated points share one canonical encoding.
const (
	metricKindGauge       int32 = 1
	valueTypeBool         int32 = 1
	valueTypeInt64        int32 = 2
	valueTypeDouble       int32 = 3
	valueTypeString       int32 = 4
	valueTypeDistribution int32 = 5
)
