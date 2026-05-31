package ui

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"jaiscloud/internal/aws/ui/middleware"
	"jaiscloud/internal/model"
)

// regionFrom reads region from the request context (injected by middleware.InjectConfig).
func regionFrom(r *http.Request) string {
	if v := r.URL.Query().Get("region"); v != "" {
		return v
	}
	if v := r.Context().Value(middleware.CtxKeyRegion); v != nil {
		return v.(string)
	}
	return ""
}

// accountFrom reads accountID from the request context.
func accountFrom(r *http.Request) string {
	if v := r.Context().Value(middleware.CtxKeyAccount); v != nil {
		return v.(string)
	}
	return ""
}

// pageSizeFrom parses ?pageSize, clamped to [1, maxSize].
func pageSizeFrom(r *http.Request, defaultSize, maxSize int) int {
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

// pageTokenFrom reads ?nextToken from the query string.
func pageTokenFrom(r *http.Request) string {
	return r.URL.Query().Get("nextToken")
}

// writeJSON encodes v as JSON with Content-Type application/json.
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}

// writeError translates a provider error to { "code": "...", "message": "..." }.
func writeError(w http.ResponseWriter, err error) {
	var pe *model.ProviderError
	if errors.As(err, &pe) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(pe.HTTPStatus)
		json.NewEncoder(w).Encode(map[string]string{"code": pe.Code, "message": pe.Message}) //nolint:errcheck
		return
	}
	http.Error(w, `{"code":"InternalError","message":"internal server error"}`, http.StatusInternalServerError)
}

// uiError writes a UI-specific error (not from a provider).
func uiError(w http.ResponseWriter, code, message string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"code": code, "message": message}) //nolint:errcheck
}

// PaginatedResponse is the standard envelope for all list endpoints.
type PaginatedResponse[T any] struct {
	Items     []T    `json:"items"`
	NextToken string `json:"nextToken,omitempty"`
	Total     *int64 `json:"total,omitempty"`
}
