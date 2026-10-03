package serviceusage

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	core "jaiscloud/internal/gcp/service/serviceusage"
	"jaiscloud/internal/model"
)

// Operation metadata / response @type values for the Service Usage v1 control
// plane. They are the proto message names the REST (grpc-gateway) surface
// advertises.
const (
	operationMetadataType = "type.googleapis.com/google.api.serviceusage.v1.OperationMetadata"
	enableResponseType    = "type.googleapis.com/google.api.serviceusage.v1.EnableServiceResponse"
	disableResponseType   = "type.googleapis.com/google.api.serviceusage.v1.DisableServiceResponse"
	batchEnableRespType   = "type.googleapis.com/google.api.serviceusage.v1.BatchEnableServicesResponse"
)

// ─── request helpers ──────────────────────────────────────────────────────────

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

func intFrom(v any) int {
	switch x := v.(type) {
	case float64:
		return int(x)
	case int:
		return x
	case string:
		n, _ := strconv.Atoi(x)
		return n
	default:
		return 0
	}
}

// bodyOf returns the decoded JSON request body, or an empty map when absent.
func bodyOf(nr *model.NormalizedRequest) map[string]any {
	if m, ok := nr.Params["body"].(map[string]any); ok && m != nil {
		return m
	}
	return map[string]any{}
}

// stringSlice extracts a []string from a decoded JSON array ([]any of string).
func stringSlice(v any) []string {
	items, _ := v.([]any)
	out := make([]string, 0, len(items))
	for _, it := range items {
		if s, ok := it.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// ─── wire encoding ────────────────────────────────────────────────────────────

// serviceToJSON renders a core API as the Discovery Service shape.
func serviceToJSON(a core.API) map[string]any {
	return map[string]any{
		"name":   a.Name,
		"parent": a.Parent,
		"config": map[string]any{"name": a.ConfigName},
		"state":  string(a.State),
	}
}

// operationToJSON renders a core Operation as a google.longrunning.Operation.
// response is the already-typed response envelope (nil to omit it). The metadata
// carries only resourceNames, matching google.api.serviceusage.v1.OperationMetadata.
func operationToJSON(op core.Operation, response map[string]any) map[string]any {
	out := map[string]any{
		"name": op.Name,
		"metadata": map[string]any{
			"@type":         operationMetadataType,
			"resourceNames": op.ResourceNames,
		},
		"done": op.Done,
	}
	if response != nil {
		out["response"] = response
	}
	return out
}

// typedResponse builds an Any-shaped JSON object with the given @type.
func typedResponse(typ string, fields map[string]any) map[string]any {
	out := map[string]any{"@type": typ}
	for k, v := range fields {
		out[k] = v
	}
	return out
}

// operationResponse renders the typed response envelope for a mutation from the
// operation's persisted service snapshot, or nil while the operation is in
// flight (real GCP omits an in-flight result). It is the read-time inverse of
// the envelope the enable/disable/batchEnable handlers return inline, so a poll
// and the original response cannot drift.
func operationResponse(op core.Operation) map[string]any {
	if !op.Done {
		return nil
	}
	switch op.Verb {
	case "enable":
		if len(op.Services) == 1 {
			return typedResponse(enableResponseType, map[string]any{"service": serviceToJSON(op.Services[0])})
		}
	case "disable":
		if len(op.Services) == 1 {
			return typedResponse(disableResponseType, map[string]any{"service": serviceToJSON(op.Services[0])})
		}
	case "batchEnable":
		services := make([]any, 0, len(op.Services))
		for _, a := range op.Services {
			services = append(services, serviceToJSON(a))
		}
		return typedResponse(batchEnableRespType, map[string]any{"services": services})
	}
	return nil
}
