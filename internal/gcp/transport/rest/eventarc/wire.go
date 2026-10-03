package eventarc

import (
	"encoding/json"
	"strconv"
	"strings"

	"jaiscloud/internal/model"
)

func strParam(nr *model.NormalizedRequest, key string) string {
	s, _ := nr.Params[key].(string)
	return s
}

// boolParam reports whether a query/body parameter is the boolean true. It
// accepts the "true" string the REST codec stores for ?validateOnly=true, and
// a native bool for in-process callers.
func boolParam(nr *model.NormalizedRequest, key string) bool {
	switch v := nr.Params[key].(type) {
	case bool:
		return v
	case string:
		return strings.EqualFold(v, "true")
	}
	return false
}

// rawBodyOf marshals the decoded JSON request body to raw JSON, returning nil
// for an absent or empty body.
func rawBodyOf(nr *model.NormalizedRequest) json.RawMessage {
	body, ok := nr.Params["body"].(map[string]any)
	if !ok || body == nil {
		return nil
	}
	data, err := json.Marshal(body)
	if err != nil {
		return nil
	}
	return data
}

// bodyOf returns the decoded JSON request body, or an empty map when absent.
func bodyOf(nr *model.NormalizedRequest) map[string]any {
	if m, ok := nr.Params["body"].(map[string]any); ok && m != nil {
		return m
	}
	return map[string]any{}
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
