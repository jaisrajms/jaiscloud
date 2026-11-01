package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"jaiscloud/internal/admin"
	"jaiscloud/internal/config"
)

type staticProjectLister struct{ rows []admin.ProjectRow }

func (s staticProjectLister) ListProjectRows(context.Context) ([]admin.ProjectRow, error) {
	return s.rows, nil
}

func newProjectsServer(t *testing.T, lister admin.ProjectLister) *Server {
	t.Helper()
	h := admin.NewHandler()
	if lister != nil {
		h.RegisterProjectLister(lister)
	}
	s := &Server{cfg: &config.Config{LogLevel: "error"}, adminHandler: h}
	s.buildRouter()
	return s
}

// The admin project-discovery route is GCP-only: surfaced on a wired binary,
// and an explicit 501 rather than an empty list on one that is not.
func TestAdminProjectsRoute(t *testing.T) {
	t.Run("unwired is 501", func(t *testing.T) {
		s := newProjectsServer(t, nil)
		rec := httptest.NewRecorder()
		s.router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/_jaiscloud/projects", nil))
		if rec.Code != http.StatusNotImplemented {
			t.Fatalf("status = %d, want 501", rec.Code)
		}
	})

	t.Run("wired serves the snapshot", func(t *testing.T) {
		s := newProjectsServer(t, staticProjectLister{rows: []admin.ProjectRow{
			{ID: "acme-e2e", Kind: "observed"},
		}})
		rec := httptest.NewRecorder()
		s.router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/_jaiscloud/projects", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
		}
		if got := rec.Body.String(); !strings.Contains(got, "acme-e2e") {
			t.Fatalf("body = %q, want the observed project", got)
		}
	})
}
