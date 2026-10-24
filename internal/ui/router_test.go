package ui

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/admin"
	"jaiscloud/internal/config"
	"jaiscloud/internal/events"
	"jaiscloud/internal/model"
	"jaiscloud/internal/ui/sse"
)

// stubRegistrar is a minimal Registrar with no services, used to exercise the
// cloud-neutral core routes without a cloud provider.
type stubRegistrar struct{}

func (stubRegistrar) Cloud() model.Cloud            { return model.CloudGCP }
func (stubRegistrar) Services() []ServiceDescriptor { return nil }
func (stubRegistrar) MountRoutes(r chi.Router)      {}

func testRouter(t *testing.T, token string) chi.Router {
	t.Helper()
	cfg := &config.Config{Region: "us-central1", AccountID: "test-proj"}
	broker := sse.New(events.NewEventBus())
	t.Cleanup(broker.Shutdown)
	return BuildRouter(stubRegistrar{}, admin.NewHandler(), broker, cfg, token, "dev", "boot-test")
}

// The admin panel is mounted by the core (not a Registrar), so it must be
// reachable for every cloud once the session token checks out.
func TestBuildRouter_MountsAdminPanel(t *testing.T) {
	router := testRouter(t, "tok")
	req := httptest.NewRequest(http.MethodGet, "/api/ui/v1/admin/status", nil)
	req.Header.Set("Authorization", "Bearer tok")
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("GET /api/ui/v1/admin/status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	if got := rr.Body.String(); got == "" || got[0] != '{' {
		t.Fatalf("expected a JSON object, got: %s", got)
	}
}

// /meta is unauthenticated and carries the per-process boot ID the client uses
// to detect a restart.
func TestBuildRouter_MetaIncludesBootID(t *testing.T) {
	router := testRouter(t, "tok")
	req := httptest.NewRequest(http.MethodGet, "/api/ui/v1/meta", nil)
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("GET /api/ui/v1/meta = %d, want 200", rr.Code)
	}
	var meta MetaResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &meta); err != nil {
		t.Fatalf("decode meta: %v", err)
	}
	if meta.BootID != "boot-test" {
		t.Fatalf("bootId = %q, want %q", meta.BootID, "boot-test")
	}
}

func TestBuildRouter_AdminPanelRequiresAuth(t *testing.T) {
	router := testRouter(t, "tok")
	req := httptest.NewRequest(http.MethodGet, "/api/ui/v1/admin/status", nil)
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated admin request = %d, want 401", rr.Code)
	}
}
