// Package uihelper provides shared helpers for GCP UI API handlers. It mirrors
// internal/aws/ui/uihelper but uses the GCP resource-name formatter and cloud
// identity, and never imports internal/aws.
package uihelper

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/model"
	"jaiscloud/internal/ui/middleware"
)

// PathParam returns a URL path parameter with percent-encoding fully decoded.
// chi leaves the encoding in place for characters Go treats as path-safe (e.g.
// '+', '&', ':'), so a client-encoded segment would otherwise reach handlers
// still escaped. Document/collection ids must not contain '/'; callers that
// build resource names from a param should reject a decoded slash.
//
// chi routes against r.URL.RawPath when it is non-empty and against the
// already-decoded r.URL.Path otherwise (see chi's Mux.routeHTTP). Decoding the
// latter a second time would turn a literal "%2F" in a name into "/" — e.g. a
// Firestore id of "a%2Fb" arrives as RawPath "'a%252Fb'"/Path "'a%2Fb'" and
// must stay "a%2Fb". So only unescape when chi matched the raw (still-encoded)
// path.
func PathParam(r *http.Request, key string) string {
	v := chi.URLParam(r, key)
	if r.URL.RawPath == "" {
		return v
	}
	if decoded, err := url.PathUnescape(v); err == nil {
		return decoded
	}
	return v
}

// AsSlice coerces a provider response value that may be a typed slice into
// []any so handlers can range over it uniformly. Returns nil for non-slices.
func AsSlice(v any) []any {
	if v == nil {
		return nil
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice {
		return nil
	}
	out := make([]any, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		out[i] = rv.Index(i).Interface()
	}
	return out
}

// RegionFrom reads region from the query string or request context.
func RegionFrom(r *http.Request) string {
	if v := r.URL.Query().Get("region"); v != "" {
		return v
	}
	if v := r.Context().Value(middleware.CtxKeyRegion); v != nil {
		return v.(string)
	}
	return ""
}

// AccountFrom reads the project ID from the ?account= query param, falling back
// to the request context.
func AccountFrom(r *http.Request) string {
	if v := r.URL.Query().Get("account"); v != "" {
		return v
	}
	if v := r.Context().Value(middleware.CtxKeyAccount); v != nil {
		return v.(string)
	}
	return ""
}

// PageSizeFrom parses ?pageSize clamped to [1, maxSize].
func PageSizeFrom(r *http.Request, defaultSize, maxSize int) int {
	s := r.URL.Query().Get("pageSize")
	if s == "" {
		return defaultSize
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 {
		return defaultSize
	}
	if n > maxSize {
		return maxSize
	}
	return n
}

// WriteJSON encodes v as JSON with Content-Type application/json.
func WriteJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}

// WriteJSONStatus writes v as JSON with the given HTTP status. The
// Content-Type must be set before WriteHeader, so callers that need a non-200
// status use this instead of WriteHeader followed by WriteJSON.
func WriteJSONStatus(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}

// WriteError translates a provider error to { "code": "...", "message": "..." }.
func WriteError(w http.ResponseWriter, err error) {
	var pe *model.ProviderError
	if errors.As(err, &pe) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(pe.HTTPStatus)
		json.NewEncoder(w).Encode(map[string]string{"code": pe.Code, "message": pe.Message}) //nolint:errcheck
		return
	}
	http.Error(w, `{"code":"InternalError","message":"internal server error"}`, http.StatusInternalServerError)
}

// UIError writes a UI-specific error response.
func UIError(w http.ResponseWriter, code, message string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"code": code, "message": message}) //nolint:errcheck
}

// Str returns m[key] as a string, or "" when absent or not a string.
func Str(m map[string]any, key string) string {
	s, _ := m[key].(string)
	return s
}

// MapAt returns m[key] as a map[string]any, or nil.
func MapAt(m map[string]any, key string) map[string]any {
	v, _ := m[key].(map[string]any)
	return v
}

// StringMapAt extracts a string-valued map, dropping non-string entries. The
// provider may render labels as a typed map[string]string or a generic
// map[string]any, so both are accepted.
func StringMapAt(m map[string]any, key string) map[string]string {
	switch raw := m[key].(type) {
	case map[string]string:
		if len(raw) == 0 {
			return nil
		}
		return raw
	case map[string]any:
		out := make(map[string]string, len(raw))
		for k, v := range raw {
			if s, ok := v.(string); ok {
				out[k] = s
			}
		}
		if len(out) == 0 {
			return nil
		}
		return out
	}
	return nil
}

// StringMapToAny converts a string map into the map[string]any the provider's
// body readers expect (they type-assert on map[string]any).
func StringMapToAny(in map[string]string) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// PageParams forwards the console's list query parameters (pageSize/pageToken)
// onto the provider request so paging.Apply/Page paginates correctly.
func PageParams(r *http.Request, params map[string]any) {
	for _, key := range []string{"pageSize", "pageToken"} {
		if v := r.URL.Query().Get(key); v != "" {
			params[key] = v
		}
	}
}

// Segment returns the decoded, single-segment value of a URL path parameter. A
// decoded '/' is rejected: callers use this for ids that are one segment.
func Segment(r *http.Request, key string) (string, bool) {
	v := PathParam(r, key)
	if v == "" || strings.Contains(v, "/") {
		return "", false
	}
	return v, true
}

// IamBinding is one role-to-members binding in an IAM policy. It is shared by
// the GCP UI service packages so their policy editors speak one shape.
type IamBinding struct {
	Role      string         `json:"role"`
	Members   []string       `json:"members"`
	Condition map[string]any `json:"condition,omitempty"`
}

// IamPolicy mirrors google.iam.v1.Policy for the UI policy editors.
type IamPolicy struct {
	Bindings []IamBinding `json:"bindings,omitempty"`
	Etag     string       `json:"etag,omitempty"`
	Version  int          `json:"version,omitempty"`
}

// IamPolicyBody converts a UI IamPolicy into the map[string]any shape the
// providers' policy.Set expects (bindings as []any).
func IamPolicyBody(p IamPolicy) map[string]any {
	bindings := make([]any, 0, len(p.Bindings))
	for _, b := range p.Bindings {
		entry := map[string]any{"role": b.Role, "members": StringsToAny(b.Members)}
		if b.Condition != nil {
			entry["condition"] = b.Condition
		}
		bindings = append(bindings, entry)
	}
	out := map[string]any{"bindings": bindings}
	if p.Etag != "" {
		out["etag"] = p.Etag
	}
	if p.Version > 0 {
		out["version"] = p.Version
	}
	return out
}

// StringsToAny widens a []string into the []any the provider body readers use.
func StringsToAny(in []string) []any {
	out := make([]any, 0, len(in))
	for _, s := range in {
		out = append(out, s)
	}
	return out
}

// NR constructs a NormalizedRequest for direct provider calls from UI handlers.
// Port is cfg.Port (wire port) — NOT the UI port. Clock is cfg.Clock; providers
// panic on a nil Clock.
func NR(ctx context.Context, cfg *config.Config, service, action, region, accountID string) *model.NormalizedRequest {
	return &model.NormalizedRequest{
		Service:    service,
		Action:     action,
		Params:     make(map[string]any),
		Clock:      cfg.Clock,
		Region:     region,
		AccountID:  accountID,
		Port:       cfg.Port,
		Cloud:      model.CloudGCP,
		ResourceID: resource.ResourceID(accountID),
	}
}
