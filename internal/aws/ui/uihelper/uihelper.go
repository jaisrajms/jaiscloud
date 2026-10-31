// Package uihelper provides shared helpers for UI API handlers.
// Imported by both the parent ui package and its subpackages (sqsui, lambdaui, logsui, etc.)
// to avoid circular imports.
package uihelper

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strconv"

	"jaiscloud/internal/aws/arn"
	"jaiscloud/internal/config"
	"jaiscloud/internal/model"
	"jaiscloud/internal/ui/middleware"
)

// AsSlice coerces a provider response value that may be a typed slice
// (e.g. []map[string]any, []string, []reservation) into []any so handlers can
// range over it uniformly. Returns nil for non-slice values. Providers use
// pagination.Paginate[T] which preserves the element type, so a plain
// .([]any) assertion silently fails.
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

// RegionFrom reads region from query string or request context.
func RegionFrom(r *http.Request) string {
	if v := r.URL.Query().Get("region"); v != "" {
		return v
	}
	if v := r.Context().Value(middleware.CtxKeyRegion); v != nil {
		return v.(string)
	}
	return ""
}

// AccountFrom reads accountID from ?account= query param, falling back to request context.
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

// PageTokenFrom reads ?nextToken from the query string.
func PageTokenFrom(r *http.Request) string {
	return r.URL.Query().Get("nextToken")
}

// WriteJSON encodes v as JSON with Content-Type application/json.
func WriteJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
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

// NR constructs a NormalizedRequest for direct provider calls from UI handlers.
// Port is cfg.Port (wire port, default 4566) — NOT the UI port.
// Clock is cfg.Clock — providers panic on nil Clock.
func NR(ctx context.Context, cfg *config.Config, service, action, region, accountID string) *model.NormalizedRequest {
	return &model.NormalizedRequest{
		Service:    service,
		Action:     action,
		Params:     make(map[string]any),
		Clock:      cfg.Clock,
		Region:     region,
		AccountID:  accountID,
		Port:       cfg.Port,
		Cloud:      model.CloudAWS,
		ResourceID: arn.ResourceID(region, accountID),
	}
}

// Maps returns v as a list of objects. Built-in providers return
// []map[string]any; a plugin's JSON decodes to []any of map[string]any. The
// console must show both, so handlers read lists through this helper instead
// of asserting one concrete type. Elements that are not objects are skipped.
func Maps(v any) []map[string]any {
	switch l := v.(type) {
	case []map[string]any:
		return l
	case []any:
		out := make([]map[string]any, 0, len(l))
		for _, e := range l {
			if m, ok := e.(map[string]any); ok {
				out = append(out, m)
			}
		}
		return out
	}
	return nil
}

// IntOK returns v as an int when it is any Go integer type or a JSON number.
func IntOK(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int32:
		return int(n), true
	case int64:
		return int(n), true
	case float64:
		return int(n), true
	}
	return 0, false
}

// Int is IntOK without the flag (0 when absent or not a number).
func Int(v any) int {
	n, _ := IntOK(v)
	return n
}

// Strings returns v as []string: built-ins return []string, a plugin's JSON
// decodes to []any.
func Strings(v any) []string {
	switch l := v.(type) {
	case []string:
		return l
	case []any:
		out := make([]string, 0, len(l))
		for _, e := range l {
			if s, ok := e.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

// StringMap returns v as map[string]string (a plugin's JSON decodes to
// map[string]any); non-string values are skipped.
func StringMap(v any) map[string]string {
	switch m := v.(type) {
	case map[string]string:
		return m
	case map[string]any:
		out := make(map[string]string, len(m))
		for k, e := range m {
			if s, ok := e.(string); ok {
				out[k] = s
			}
		}
		return out
	}
	return nil
}
