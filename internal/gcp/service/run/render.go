package run

import (
	"net"
	"strconv"
	"strings"
	"time"

	runstore "jaiscloud/internal/gcp/store/run"
	"jaiscloud/internal/gcp/timestamp"
)

// google.protobuf.Any type URLs used on Cloud Run operation responses.
const (
	serviceTypeURL  = "type.googleapis.com/google.cloud.run.v2.Service"
	revisionTypeURL = "type.googleapis.com/google.cloud.run.v2.Revision"
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
	return timestamp.Canonical(t)
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
		tag, _ := m["tag"].(string)
		status := map[string]any{"percent": intFrom(m["percent"])}
		// Real Cloud Run sets a target's status `uri` only when the target
		// carries a tag, and then it is the tag-qualified URL
		// (https://{tag}---{service-host}); an untagged target has no `uri` key.
		if tag != "" {
			status["uri"] = taggedURI(s.Uri, tag)
		}
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
		})
	}
	return out
}

// taggedURI rewrites a service uri into the tag-qualified form real Cloud Run
// returns for the target carrying a traffic tag. The tag becomes the leading
// host label — https://{tag}---{service-host} (e.g.
// https://stable---svc-0123456789ab.us-central1.run.app) — the string the
// Discovery's TrafficTargetStatus.tag describes as "used in the URI to
// exclusively reference this target". An empty tag leaves the uri unchanged.
func taggedURI(uri, tag string) string {
	if tag == "" {
		return uri
	}
	if i := strings.Index(uri, "://"); i >= 0 {
		return uri[:i+3] + tag + "---" + uri[i+3:]
	}
	return uri
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
	if !r.DeleteTime.IsZero() {
		out["deleteTime"] = formatTime(r.DeleteTime)
	}
	out["conditions"] = []any{readyCondition(r.UpdateTime)}
	out["reconciling"] = false
	return out
}

// OperationJSON renders a stored operation as the google.longrunning.Operation
// wire map. The metadata snapshot is present from creation — an in-flight
// operation carries it too, matching real GCP (the mutation's
// google.longrunning.operation_info metadata_type: Service for service
// mutations, Revision for revision deletes) — while the response is only
// present once the operation is done. Both are the service snapshot wrapped as
// a typed Any (the @type discriminator gax clients require to unpack it).
func OperationJSON(op runstore.Operation) map[string]any {
	out := map[string]any{
		"name": OperationName(op.ProjectID, op.Location, op.ID),
		"done": op.Done,
	}
	switch {
	case op.Service != nil:
		out["metadata"] = typedAny(serviceTypeURL, ServiceJSON(*op.Service))
		if op.Done {
			out["response"] = typedAny(serviceTypeURL, ServiceJSON(*op.Service))
		}
	case op.Revision != nil:
		out["metadata"] = typedAny(revisionTypeURL, RevisionJSON(*op.Revision))
		if op.Done {
			out["response"] = typedAny(revisionTypeURL, RevisionJSON(*op.Revision))
		}
	}
	return out
}

// typedAny wraps a rendered body as the Any-shaped JSON a
// google.longrunning.Operation response carries (the @type discriminator gax
// clients require to unpack it).
func typedAny(typeURL string, body map[string]any) map[string]any {
	out := make(map[string]any, len(body)+1)
	out["@type"] = typeURL
	for k, v := range body {
		out[k] = v
	}
	return out
}

// IsInvocationHost reports whether host is a synthesized Cloud Run service host
// (*.run.app), which addresses the data plane rather than the control plane.
func IsInvocationHost(host string) bool {
	h := strings.ToLower(stripHostPort(host))
	suffix := "." + invocationHost.Suffix
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
