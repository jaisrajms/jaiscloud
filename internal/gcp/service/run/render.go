package run

import (
	"net"
	"strconv"
	"strings"
	"time"

	runstore "jaiscloud/internal/gcp/store/run"
)

// google.protobuf.Any type URLs used on Cloud Run operation responses.
const (
	serviceTypeURL = "type.googleapis.com/google.cloud.run.v2.Service"
)

// conditionLastTransition returns the RFC3339 timestamp used on a Ready
// condition.
func conditionLastTransition(t time.Time) string { return formatTime(t) }

// formatTime renders a business timestamp as RFC3339 with nanoseconds, the
// google-datetime wire form.
func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// readyCondition is the terminal Ready condition the mock reports.
func readyCondition(t time.Time) map[string]any {
	return map[string]any{
		"type":               "Ready",
		"state":              "CONDITION_SUCCEEDED",
		"lastTransitionTime": conditionLastTransition(t),
	}
}

// ServiceJSON renders a stored service as the google.cloud.run.v2.Service wire
// map: the caller-supplied Data merged with the derived output-only fields.
func ServiceJSON(s runstore.Service) map[string]any {
	out := cloneMap(s.Data)
	out["name"] = ServiceName(s.ProjectID, s.Location, s.ID)
	out["uid"] = s.UID
	out["generation"] = strconv.FormatInt(s.Generation, 10)
	out["observedGeneration"] = strconv.FormatInt(s.Generation, 10)
	out["etag"] = s.Etag
	out["createTime"] = formatTime(s.CreateTime)
	out["updateTime"] = formatTime(s.UpdateTime)
	if !s.DeleteTime.IsZero() {
		out["deleteTime"] = formatTime(s.DeleteTime)
	}
	out["uri"] = s.Uri
	out["urls"] = []any{s.Uri}
	out["latestReadyRevision"] = s.LatestReadyRevision
	out["latestCreatedRevision"] = s.LatestCreatedRevision
	cond := readyCondition(s.UpdateTime)
	out["terminalCondition"] = cond
	out["conditions"] = []any{cond}
	out["reconciling"] = false
	out["trafficStatuses"] = trafficStatuses(s)
	return out
}

// trafficStatuses derives the status list from the caller's traffic targets, or
// a single latest target at 100% when none were supplied.
func trafficStatuses(s runstore.Service) []any {
	targets, _ := s.Data["traffic"].([]any)
	out := make([]any, 0, len(targets)+1)
	for _, t := range targets {
		m, ok := t.(map[string]any)
		if !ok {
			continue
		}
		status := map[string]any{"percent": intFrom(m["percent"]), "uri": s.Uri}
		if v, ok := m["type"]; ok {
			status["type"] = v
		}
		if v, ok := m["tag"]; ok {
			status["tag"] = v
		}
		if v, ok := m["revision"].(string); ok && v != "" {
			status["revision"] = v
		} else {
			status["revision"] = s.LatestReadyRevision
		}
		out = append(out, status)
	}
	if len(out) == 0 {
		out = append(out, map[string]any{
			"type":     "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST",
			"revision": s.LatestReadyRevision,
			"percent":  100,
			"uri":      s.Uri,
		})
	}
	return out
}

// RevisionJSON renders a stored revision as the google.cloud.run.v2.Revision
// wire map.
func RevisionJSON(r runstore.Revision) map[string]any {
	out := cloneMap(r.Data)
	out["name"] = RevisionName(r.ProjectID, r.Location, r.Service, r.ID)
	out["service"] = ServiceName(r.ProjectID, r.Location, r.Service)
	out["uid"] = r.UID
	out["generation"] = strconv.FormatInt(r.Generation, 10)
	out["observedGeneration"] = strconv.FormatInt(r.Generation, 10)
	out["etag"] = r.Etag
	out["createTime"] = formatTime(r.CreateTime)
	out["updateTime"] = formatTime(r.UpdateTime)
	out["conditions"] = []any{readyCondition(r.UpdateTime)}
	out["reconciling"] = false
	return out
}

// OperationJSON renders a stored operation as the google.longrunning.Operation
// wire map. An in-flight operation omits the response, matching real GCP. The
// response/metadata is the service snapshot wrapped as a typed Any (the @type
// discriminator gax clients require to unpack it).
func OperationJSON(op runstore.Operation) map[string]any {
	out := map[string]any{
		"name": OperationName(op.ProjectID, op.Location, op.ID),
		"done": op.Done,
	}
	if op.Done && op.Service != nil {
		out["metadata"] = serviceAny(*op.Service)
		out["response"] = serviceAny(*op.Service)
	}
	return out
}

// serviceAny wraps a rendered service body as the Any-shaped JSON a
// google.longrunning.Operation response carries.
func serviceAny(s runstore.Service) map[string]any {
	body := ServiceJSON(s)
	out := make(map[string]any, len(body)+1)
	out["@type"] = serviceTypeURL
	for k, v := range body {
		out[k] = v
	}
	return out
}

// IsInvocationHost reports whether host is a synthesized Cloud Run service host
// (*.run.app), which addresses the data plane rather than the control plane.
func IsInvocationHost(host string) bool {
	h := strings.ToLower(stripHostPort(host))
	suffix := "." + DefaultURLSuffix
	return strings.HasSuffix(h, suffix) && len(h) > len(suffix)
}

// stripHostPort removes a trailing ":port" from a request host, tolerating the
// port-less form and bracketed IPv6.
func stripHostPort(host string) string {
	if host == "" {
		return ""
	}
	if h, _, err := net.SplitHostPort(host); err == nil {
		return h
	}
	return host
}

// intFrom coerces a decoded JSON number to int.
func intFrom(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	default:
		return 0
	}
}
