package functions

import (
	"strconv"
	"strings"

	"jaiscloud/internal/model"
)

// resourceName returns the "name" path param, or InvalidArgument when absent.
func resourceName(nr *model.NormalizedRequest) (string, error) {
	n, ok := nr.Params["name"].(string)
	if !ok || n == "" {
		return "", model.NewProviderError("InvalidArgument", "missing resource name", 400)
	}
	return n, nil
}

func strParam(nr *model.NormalizedRequest, key string) string {
	s, _ := nr.Params[key].(string)
	return s
}

// boolParam reports whether a boolean query param is truthy. It accepts the
// string forms a query string always yields ("true"/"1") and the decoded Go
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

// baseURL returns the request's absolute "scheme://host" origin, used to build
// an emulator-reachable source-upload URL (the gcloud gen2 deploy PUTs directly
// to the returned uploadUrl). It mirrors the GCS adapter's baseURLFromRequest,
// including the X-Forwarded-Proto override for proxied/port-forwarded access.
func baseURL(nr *model.NormalizedRequest) string {
	if nr == nil || nr.Raw == nil {
		return ""
	}
	r := nr.Raw
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if fwd := r.Header.Get("X-Forwarded-Proto"); fwd != "" {
		scheme = fwd
	}
	return scheme + "://" + r.Host
}

// bodyOf returns the decoded JSON request body, or an empty map when absent.
func bodyOf(nr *model.NormalizedRequest) map[string]any {
	if m, ok := nr.Params["body"].(map[string]any); ok && m != nil {
		return m
	}
	return map[string]any{}
}

func bodyString(body map[string]any, key string) string {
	if body == nil {
		return ""
	}
	s, _ := body[key].(string)
	return s
}

// intFrom coerces a decoded numeric/string param to an int.
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

// splitMask splits a comma-separated updateMask (REST query form) into paths.
func splitMask(mask string) []string {
	var out []string
	for _, p := range strings.Split(mask, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
