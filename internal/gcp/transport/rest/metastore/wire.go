package metastore

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"jaiscloud/internal/model"
)

// ─── request helpers ──────────────────────────────────────────────────────────

// splitEscaped splits an escaped URL path on "/" and unescapes each segment, so
// %2F within a segment survives as part of the segment.
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

// bodyOf returns the decoded JSON request body, or an empty map when absent.
func bodyOf(nr *model.NormalizedRequest) map[string]any {
	if m, ok := nr.Params["body"].(map[string]any); ok && m != nil {
		return m
	}
	return map[string]any{}
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
