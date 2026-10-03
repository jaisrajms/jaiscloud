package gateway

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// corsExtractBucket pulls the S3 bucket name from the request without full
// codec parsing (used before DetectAndDecode for CORS preflight).
func corsExtractBucket(r *http.Request) string {
	// Virtual-hosted style: "bucket.s3.region.amazonaws.com" or "bucket.<base>"
	host := r.Host
	if idx := strings.LastIndexByte(host, ':'); idx >= 0 {
		host = host[:idx]
	}
	if i := strings.Index(host, ".s3."); i >= 0 {
		return host[:i]
	}
	// Path-style: /{bucket}/{key...}
	path := strings.TrimPrefix(r.URL.Path, "/")
	if idx := strings.IndexByte(path, '/'); idx >= 0 {
		return path[:idx]
	}
	return path
}

// corsMatchRule returns the first rule that allows the given origin and method.
func corsMatchRule(rules []map[string]any, origin, method string) (map[string]any, bool) {
	for _, rule := range rules {
		if !corsMatchOrigin(origin, anySlice(rule["AllowedOrigins"])) {
			continue
		}
		if method != "" && !corsMatchMethod(method, anySlice(rule["AllowedMethods"])) {
			continue
		}
		return rule, true
	}
	return nil, false
}

func corsMatchOrigin(origin string, allowed []string) bool {
	for _, a := range allowed {
		if a == "*" || a == origin {
			return true
		}
	}
	return false
}

func corsMatchMethod(method string, allowed []string) bool {
	for _, a := range allowed {
		if strings.EqualFold(a, method) {
			return true
		}
	}
	return false
}

func corsWritePreflightHeaders(w http.ResponseWriter, rule map[string]any, origin, reqHeaders string) {
	h := w.Header()
	h.Set("Access-Control-Allow-Origin", origin)
	h.Set("Vary", "Origin")
	methods := strings.Join(anySlice(rule["AllowedMethods"]), ", ")
	if methods != "" {
		h.Set("Access-Control-Allow-Methods", methods)
	}
	if reqHeaders != "" {
		h.Set("Access-Control-Allow-Headers", reqHeaders)
	} else if headers := strings.Join(anySlice(rule["AllowedHeaders"]), ", "); headers != "" {
		h.Set("Access-Control-Allow-Headers", headers)
	}
	if age, ok := intFromAny(rule["MaxAgeSeconds"]); ok && age > 0 {
		h.Set("Access-Control-Max-Age", strconv.Itoa(age))
	}
}

// CORSAddResponseHeaders adds CORS headers to a regular (non-preflight) S3
// response when Origin is present and a matching rule exists.
func CORSAddResponseHeaders(h http.Header, rules []map[string]any, origin string) {
	rule, ok := corsMatchRule(rules, origin, "")
	if !ok {
		return
	}
	h.Set("Access-Control-Allow-Origin", origin)
	h.Set("Vary", "Origin")
	if expose := strings.Join(anySlice(rule["ExposeHeaders"]), ", "); expose != "" {
		h.Set("Access-Control-Expose-Headers", expose)
	}
}

func anySlice(v any) []string {
	switch s := v.(type) {
	case []string:
		return s
	case []any:
		out := make([]string, 0, len(s))
		for _, item := range s {
			if str, ok := item.(string); ok {
				out = append(out, str)
			}
		}
		return out
	}
	return nil
}

func intFromAny(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case float64:
		return int(n), true
	}
	return 0, false
}

// ─── GCS bucket CORS ──────────────────────────────────────────────────────────

// gcsCORSExtractBucket pulls the GCS bucket name from a request path. The JSON
// API paths are /storage/v1/b/{bucket}/..., /upload/storage/v1/b/{bucket}/...,
// and /download/storage/v1/b/{bucket}/...; raw media downloads use
// /{bucket}/{object...}. The bucket name may be percent-encoded.
func gcsCORSExtractBucket(r *http.Request) string {
	p := r.URL.EscapedPath()
	for _, prefix := range []string{"/storage/v1/b/", "/download/storage/v1/b/", "/upload/storage/v1/b/"} {
		if strings.HasPrefix(p, prefix) {
			return unescapeFirstSegment(strings.TrimPrefix(p, prefix))
		}
	}
	return unescapeFirstSegment(strings.TrimPrefix(p, "/"))
}

// unescapeFirstSegment returns the first path segment of p, percent-decoded.
func unescapeFirstSegment(p string) string {
	if i := strings.IndexByte(p, '/'); i >= 0 {
		p = p[:i]
	}
	if u, err := url.PathUnescape(p); err == nil {
		return u
	}
	return p
}

// gcsCORSMatchRule returns the first GCS CORS rule that allows the given origin
// and method. GCS rule keys are lowercase (origin/method/responseHeader/
// maxAgeSeconds); an empty method skips method matching (regular responses). A
// "*" in origins or methods matches anything.
func gcsCORSMatchRule(rules []map[string]any, origin, method string) (map[string]any, bool) {
	for _, rule := range rules {
		if !corsMatchOrigin(origin, anySlice(rule["origin"])) {
			continue
		}
		if method != "" && !gcsCORSMatchMethod(method, anySlice(rule["method"])) {
			continue
		}
		return rule, true
	}
	return nil, false
}

func gcsCORSMatchMethod(method string, allowed []string) bool {
	for _, a := range allowed {
		if a == "*" || strings.EqualFold(a, method) {
			return true
		}
	}
	return false
}

// gcsCORSPreflightHeaders writes the Access-Control-* headers for an OPTIONS
// preflight that matched a GCS CORS rule. Access-Control-Allow-Headers reflects
// the client's requested headers (GCS has no allow-list for request headers);
// Access-Control-Max-Age comes from the rule's maxAgeSeconds.
func gcsCORSPreflightHeaders(w http.ResponseWriter, rule map[string]any, origin, reqHeaders string) {
	h := w.Header()
	h.Set("Access-Control-Allow-Origin", origin)
	h.Set("Vary", "Origin")
	if methods := strings.Join(anySlice(rule["method"]), ", "); methods != "" {
		h.Set("Access-Control-Allow-Methods", methods)
	}
	if reqHeaders != "" {
		h.Set("Access-Control-Allow-Headers", reqHeaders)
	}
	if age, ok := intFromAny(rule["maxAgeSeconds"]); ok && age > 0 {
		h.Set("Access-Control-Max-Age", strconv.Itoa(age))
	}
}

// gcsCORSAddResponseHeaders adds the CORS headers for a regular (non-preflight)
// GCS response when Origin is present and a matching rule exists. The rule's
// responseHeader list becomes Access-Control-Expose-Headers.
func gcsCORSAddResponseHeaders(h http.Header, rules []map[string]any, origin string) {
	rule, ok := gcsCORSMatchRule(rules, origin, "")
	if !ok {
		return
	}
	h.Set("Access-Control-Allow-Origin", origin)
	h.Set("Vary", "Origin")
	if expose := strings.Join(anySlice(rule["responseHeader"]), ", "); expose != "" {
		h.Set("Access-Control-Expose-Headers", expose)
	}
}
