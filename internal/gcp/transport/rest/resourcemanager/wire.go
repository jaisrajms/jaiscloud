package resourcemanager

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	core "jaiscloud/internal/gcp/service/resourcemanager"
	"jaiscloud/internal/gcp/timestamp"
	"jaiscloud/internal/model"
)

// splitEscaped splits an escaped URL path on "/" and unescapes each segment.
func splitEscaped(path string) []string {
	raw := strings.Split(strings.TrimPrefix(path, "/"), "/")
	out := make([]string, 0, len(raw))
	for _, s := range raw {
		if u, err := url.PathUnescape(s); err == nil {
			out = append(out, u)
		} else {
			out = append(out, s)
		}
	}
	return out
}

// queryToParams copies single-valued query parameters into params as strings.
func queryToParams(r *http.Request, params map[string]any) {
	for k, vs := range r.URL.Query() {
		if len(vs) > 0 {
			params[k] = vs[0]
		}
	}
}

// parseJSON decodes a JSON object body, returning nil for an empty body.
func parseJSON(body []byte) (map[string]any, error) {
	if len(body) == 0 {
		return nil, nil
	}
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, err
	}
	return m, nil
}

func strParam(nr *model.NormalizedRequest, key string) string {
	s, _ := nr.Params[key].(string)
	return s
}

// mapStr reads a string field from a decoded JSON object, returning "" when
// absent or not a string.
func mapStr(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}

// bodyOf returns the decoded JSON request body, or an empty map when absent.
func bodyOf(nr *model.NormalizedRequest) map[string]any {
	if m, ok := nr.Params["body"].(map[string]any); ok && m != nil {
		return m
	}
	return map[string]any{}
}

func intOf(v any) int {
	switch x := v.(type) {
	case float64:
		return int(x)
	case int:
		return x
	case string:
		// Query parameters are stored as strings by queryToParams.
		n, _ := strconv.Atoi(x)
		return n
	case json.Number:
		if n, err := x.Int64(); err == nil {
			return int(n)
		}
	}
	return 0
}

// ─── v1 project + operation rendering ────────────────────────────────────────

// v1 Project and operation-metadata @type values (the legacy
// google.cloudresourcemanager.v1 proto message names the v1 JSON surface
// advertises). Operation.response/metadata are Any-shaped, so the Project and
// ProjectCreationStatus carried by a create operation need their @type.
const (
	v1ProjectType        = "type.googleapis.com/google.cloudresourcemanager.v1.Project"
	v1CreationStatusType = "type.googleapis.com/google.cloudresourcemanager.v1.ProjectCreationStatus"
)

// projectToV1JSON renders the core project as the v1 Project schema: projectId,
// projectNumber, name (= displayName), lifecycleState (= state), optional
// labels, and createTime. The v1 API has no separate resource name or etag
// field, so neither is emitted.
func projectToV1JSON(p core.Project) map[string]any {
	out := map[string]any{
		"projectId":      p.ProjectID,
		"projectNumber":  p.ProjectNumber,
		"name":           p.DisplayName,
		"lifecycleState": p.State,
	}
	if len(p.Labels) > 0 {
		out["labels"] = p.Labels
	}
	if p.Parent != "" {
		out["parent"] = parentToV1Resource(p.Parent)
	}
	if !p.CreateTime.IsZero() {
		out["createTime"] = timestamp.Canonical(p.CreateTime)
	}
	return out
}

// parentToV1Resource renders the canonical parent reference
// ("organizations/{id}"/"folders/{id}") as the v1 ResourceId ({type,id}) whose
// singular type values are "organization"/"folder".
func parentToV1Resource(parent string) map[string]any {
	typ, id := parent, ""
	if i := strings.IndexByte(parent, '/'); i >= 0 {
		typ, id = parent[:i], parent[i+1:]
	}
	return map[string]any{"type": strings.TrimSuffix(typ, "s"), "id": id}
}

// operationToJSON renders a core operation as a google.longrunning.Operation.
// response is the v1 Project envelope (nil to omit it, e.g. while in flight);
// it is wrapped with the v1 Project @type because Operation.response is an Any.
func operationToJSON(op core.Operation, response map[string]any) map[string]any {
	out := map[string]any{
		"name":     op.Name,
		"metadata": operationMetadata(op),
		"done":     op.Done,
	}
	if response != nil {
		typed := map[string]any{"@type": v1ProjectType}
		for k, v := range response {
			typed[k] = v
		}
		out["response"] = typed
	}
	return out
}

// operationMetadata renders the typed google.longrunning.Operation metadata for
// a project mutation. Only create has a v1 metadata message
// (ProjectCreationStatus); the v1 delete and undelete methods return Empty and
// never surface an operation, so their verbs fall back to an empty metadata
// object.
func operationMetadata(op core.Operation) map[string]any {
	if op.Verb == "create" {
		return map[string]any{
			"@type":      v1CreationStatusType,
			"createTime": timestamp.Canonical(op.CreateTime),
			"gettable":   op.Done,
			"ready":      op.Done,
		}
	}
	return map[string]any{}
}

// isTopLevelOperationName reports whether name is a top-level
// operations/{id} resource name (the shape Cloud Resource Manager v1 publishes).
func isTopLevelOperationName(name string) bool {
	parts := strings.Split(strings.Trim(name, "/"), "/")
	return len(parts) == 2 && parts[0] == "operations" && parts[1] != ""
}

// isNotFound reports whether err is the canonical NotFound provider error (as
// returned by GetOperation for an absent operation).
func isNotFound(err error) bool {
	var perr *model.ProviderError
	return errors.As(err, &perr) && perr.Code == "NotFound"
}

// parentString converts a v1 ResourceId ({type,id}) into the canonical
// "organizations/{id}"/"folders/{id}" parent name the core stores, or "" when
// absent/incomplete.
func parentString(v any) string {
	m, ok := v.(map[string]any)
	if !ok {
		return ""
	}
	typ, _ := m["type"].(string)
	id, _ := m["id"].(string)
	if typ == "" || id == "" {
		return ""
	}
	if !strings.HasSuffix(typ, "s") {
		typ += "s"
	}
	return typ + "/" + id
}

// labelMap converts a decoded JSON labels object into the core's label map. A
// non-string value is skipped rather than coerced.
func labelMap(v any) map[string]string {
	m, ok := v.(map[string]any)
	if !ok || len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, raw := range m {
		if s, ok := raw.(string); ok {
			out[k] = s
		}
	}
	return out
}
