package workflows

import (
	"strconv"

	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/model"
)

// pageSizeParam parses the pageSize query parameter as the int32 the Workflows
// Discovery document declares; a non-integer value is INVALID_ARGUMENT rather
// than a silent zero.
func pageSizeParam(nr *model.NormalizedRequest) (int, error) {
	return paging.ParseSize(nr.Params["pageSize"])
}

// bodyOf returns the decoded JSON request body, or an empty map when absent.
func bodyOf(nr *model.NormalizedRequest) map[string]any {
	if m, ok := nr.Params["body"].(map[string]any); ok && m != nil {
		return m
	}
	return map[string]any{}
}

func strParam(nr *model.NormalizedRequest, key string) string {
	s, _ := nr.Params[key].(string)
	return s
}

// boolParam reports whether a boolean query param is truthy. It accepts the
// string form a query string always yields ("true"/"1") and the decoded Go
// bool/number forms a test NormalizedRequest may carry.
func boolParam(nr *model.NormalizedRequest, key string) bool {
	switch v := nr.Params[key].(type) {
	case bool:
		return v
	case string:
		b, _ := strconv.ParseBool(v)
		return b
	case float64:
		return v != 0
	case int:
		return v != 0
	default:
		return false
	}
}

func strFrom(v any) string {
	s, _ := v.(string)
	return s
}

// stringMapFrom decodes a JSON object of string values. A non-map (including an
// absent field) returns nil so the core can distinguish "omitted" from "empty".
func stringMapFrom(v any) map[string]string {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, val := range m {
		if s, ok := val.(string); ok {
			out[k] = s
		}
	}
	return out
}
