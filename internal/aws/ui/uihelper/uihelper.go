// Package uihelper provides shared helpers for UI API handlers.
// Imported by both the parent ui package and its subpackages (sqsui, lambdaui, logsui, etc.)
// to avoid circular imports.
package uihelper

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"jaiscloud/internal/aws/arn"
	"jaiscloud/internal/aws/ui/middleware"
	"jaiscloud/internal/config"
	"jaiscloud/internal/model"
)

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
