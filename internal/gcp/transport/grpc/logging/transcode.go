package logging

import (
	loggingpb "cloud.google.com/go/logging/apiv2/loggingpb"

	loggingstore "jaiscloud/internal/gcp/store/logging"

	monitoredres "google.golang.org/genproto/googleapis/api/monitoredres"
	ltype "google.golang.org/genproto/googleapis/logging/type"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// entryFromProto transcodes a proto LogEntry into the neutral stored form. The
// payload oneof is collapsed to PayloadType ("text"/"json") plus the matching
// field.
func entryFromProto(p *loggingpb.LogEntry) loggingstore.LogEntry {
	e := loggingstore.LogEntry{
		LogName:  p.GetLogName(),
		Severity: int(p.GetSeverity()),
		InsertID: p.GetInsertId(),
		Labels:   p.GetLabels(),
	}
	if p.GetResource() != nil {
		e.ResourceType = p.GetResource().GetType()
		e.ResourceLabels = p.GetResource().GetLabels()
	}
	if p.GetTimestamp() != nil {
		e.Timestamp = p.GetTimestamp().AsTime()
	}
	switch payload := p.GetPayload().(type) {
	case *loggingpb.LogEntry_TextPayload:
		e.PayloadType = "text"
		e.TextPayload = payload.TextPayload
	case *loggingpb.LogEntry_JsonPayload:
		e.PayloadType = "json"
		if payload.JsonPayload != nil {
			e.JsonPayload = payload.JsonPayload.AsMap()
		}
	}
	return e
}

// entryToProto transcodes a neutral stored entry back to the proto type. A JSON
// payload that cannot be represented as a Struct falls back to a text payload,
// matching the historical behavior.
func entryToProto(e loggingstore.LogEntry) *loggingpb.LogEntry {
	out := &loggingpb.LogEntry{
		LogName:  e.LogName,
		Severity: ltype.LogSeverity(e.Severity),
		InsertId: e.InsertID,
		Labels:   e.Labels,
	}
	if e.ResourceType != "" || len(e.ResourceLabels) > 0 {
		out.Resource = &monitoredres.MonitoredResource{Type: e.ResourceType, Labels: e.ResourceLabels}
	}
	if !e.Timestamp.IsZero() {
		out.Timestamp = timestamppb.New(e.Timestamp)
	}
	switch e.PayloadType {
	case "json":
		if st, err := structpb.NewStruct(e.JsonPayload); err == nil {
			out.Payload = &loggingpb.LogEntry_JsonPayload{JsonPayload: st}
		}
	default:
		out.Payload = &loggingpb.LogEntry_TextPayload{TextPayload: e.TextPayload}
	}
	return out
}
