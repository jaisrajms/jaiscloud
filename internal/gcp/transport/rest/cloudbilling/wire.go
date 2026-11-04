package cloudbilling

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	"jaiscloud/internal/model"
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

// splitVerb splits a trailing custom-method suffix off a path segment
// ("0X0X:testIamPermissions" → "0X0X", "testIamPermissions").
func splitVerb(seg string) (id, verb string) {
	if i := strings.IndexByte(seg, ':'); i >= 0 {
		return seg[:i], seg[i+1:]
	}
	return seg, ""
}

// queryToParams copies single-valued query parameters into params as strings.
// The billing surface ignores them (alt, prettyPrint, fields, ...), but the
// decoder keeps them for symmetry with the other REST codecs.
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

// strParam reads a string param, returning "" when absent or not a string.
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

// mapStr reads a string field from a decoded JSON object, returning "" when
// absent or not a string.
func mapStr(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}

// stringSlice converts a decoded JSON array ([]any of strings) into a []string,
// dropping non-string elements an untyped body could carry.
func stringSlice(v any) []string {
	switch xs := v.(type) {
	case []string:
		return xs
	case []any:
		out := make([]string, 0, len(xs))
		for _, x := range xs {
			if s, ok := x.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}
