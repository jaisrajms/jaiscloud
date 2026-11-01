package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

type stubProjectLister struct {
	rows []ProjectRow
	err  error
}

func (s stubProjectLister) ListProjectRows(context.Context) ([]ProjectRow, error) {
	return s.rows, s.err
}

func TestProjectsHandler_ReturnsSnapshot(t *testing.T) {
	h := NewHandler()
	h.RegisterProjectLister(stubProjectLister{rows: []ProjectRow{
		{ID: "jaiscloud-project", Kind: "configured"},
		{ID: "acme-e2e", Kind: "observed", LastSeen: "2026-10-31T10:00:00Z", Services: []string{"storage"}},
	}})

	rec := httptest.NewRecorder()
	h.Projects(rec, httptest.NewRequest(http.MethodGet, "/_jaiscloud/projects", nil))

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	var body struct {
		Projects []ProjectRow `json:"projects"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Len(t, body.Projects, 2)
	require.Equal(t, "acme-e2e", body.Projects[1].ID)
	require.Equal(t, "observed", body.Projects[1].Kind)
	require.Equal(t, []string{"storage"}, body.Projects[1].Services)
}

func TestProjectsHandler_EmptyListIsArray(t *testing.T) {
	h := NewHandler()
	h.RegisterProjectLister(stubProjectLister{})

	rec := httptest.NewRecorder()
	h.Projects(rec, httptest.NewRequest(http.MethodGet, "/_jaiscloud/projects", nil))

	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{"projects":[]}`, rec.Body.String())
}

func TestProjectsHandler_UnwiredIsNotImplemented(t *testing.T) {
	h := NewHandler()

	rec := httptest.NewRecorder()
	h.Projects(rec, httptest.NewRequest(http.MethodGet, "/_jaiscloud/projects", nil))

	require.Equal(t, http.StatusNotImplemented, rec.Code)
}

func TestProjectsHandler_ListerError(t *testing.T) {
	h := NewHandler()
	h.RegisterProjectLister(stubProjectLister{err: errors.New("boom")})

	rec := httptest.NewRecorder()
	h.Projects(rec, httptest.NewRequest(http.MethodGet, "/_jaiscloud/projects", nil))

	require.Equal(t, http.StatusInternalServerError, rec.Code)
}
