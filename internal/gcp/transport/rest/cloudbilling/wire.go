package cloudbilling

import (
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

// queryToParams copies single-valued query parameters into params as strings.
// The project billing read ignores them (alt, prettyPrint, fields, ...), but the
// decoder keeps them for symmetry with the other REST codecs and for the write
// surface that follows.
func queryToParams(r *http.Request, params map[string]any) {
	for k, vs := range r.URL.Query() {
		if len(vs) > 0 {
			params[k] = vs[0]
		}
	}
}

// strParam reads a string param, returning "" when absent or not a string.
func strParam(nr *model.NormalizedRequest, key string) string {
	s, _ := nr.Params[key].(string)
	return s
}
