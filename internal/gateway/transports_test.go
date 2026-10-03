package gateway

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"jaiscloud/internal/config"
)

// TestWithCloudRoutesDisabled verifies the gRPC-only mode: the cloud catch-all
// is not registered, so a GCP REST path 404s even though the admin/control-plane
// routes remain mounted on the same listener.
func TestWithCloudRoutesDisabled(t *testing.T) {
	s := &Server{cfg: &config.Config{LogLevel: "error"}}
	WithCloudRoutesDisabled()(s)
	if !s.cloudRoutesDisabled {
		t.Fatal("WithCloudRoutesDisabled did not set the flag")
	}
	s.buildRouter()

	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/storage/v1/b", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("cloud route status = %d, want 404 when cloud routes are disabled", rec.Code)
	}
}
