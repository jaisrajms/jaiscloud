package logging

import (
	loggingstore "jaiscloud/internal/gcp/store/logging"
)

// WriteRequest is a transport-neutral WriteLogEntries call. Entries carry the
// neutral stored form; the fields on the request are applied as defaults to any
// entry that omits them, exactly as real Cloud Logging does.
type WriteRequest struct {
	PartialSuccess bool
	DryRun         bool

	// LogName is the default log resource name applied to entries that carry
	// none.
	LogName string
	// ResourceType / ResourceLabels are the default monitored resource applied
	// to entries that carry none.
	ResourceType   string
	ResourceLabels map[string]string
	// Labels are default labels merged into each entry's labels (an entry's own
	// value for a key wins).
	Labels map[string]string

	Entries []loggingstore.LogEntry
}

// ListEntriesRequest is a transport-neutral ListLogEntries call. Scope is the
// resolved default scope used when ResourceNames is empty (each transport
// resolves it differently: gRPC from routing metadata, REST from the request
// project).
type ListEntriesRequest struct {
	ResourceNames []string
	Filter        string
	OrderBy       string
	PageSize      int
	PageToken     string
	Scope         string
}

// ListEntriesResult is the transport-neutral result of ListEntries.
type ListEntriesResult struct {
	Entries       []loggingstore.LogEntry
	NextPageToken string
}

// MonitoredResourceDescriptor is a transport-neutral monitored resource
// descriptor (the Logging surface leaves its resource name unset).
type MonitoredResourceDescriptor struct {
	Type        string
	DisplayName string
	Description string
	Labels      []MonitoredResourceLabel
}

// MonitoredResourceLabel is one label of a monitored resource descriptor.
// ValueType is the proto enum name ("STRING", "BOOL", or "INT64").
type MonitoredResourceLabel struct {
	Key         string
	ValueType   string
	Description string
}
