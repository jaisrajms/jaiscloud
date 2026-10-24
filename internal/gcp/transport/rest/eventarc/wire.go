package eventarc

import (
	"encoding/json"
	"strings"

	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/model"
)

// pageSizeParam parses the pageSize query parameter as the int32 every
// Eventarc list method declares. A value the int32 binding cannot parse is
// INVALID_ARGUMENT (real GCP rejects it) instead of being folded into zero and
// treated as an absent parameter.
func pageSizeParam(nr *model.NormalizedRequest) (int, error) {
	return paging.ParseSize(nr.Params["pageSize"])
}

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
