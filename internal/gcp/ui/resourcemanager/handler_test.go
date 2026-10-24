package resourcemanagerui

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	resourcemanagercore "jaiscloud/internal/gcp/service/resourcemanager"
	"jaiscloud/internal/store"
)

// newRouter wires the real Resource Manager core over a memory store so the
// handler tests exercise the actual create/list/delete/undelete semantics.
func newRouter() (*resourcemanagercore.Service, http.Handler) {
	core := resourcemanagercore.NewService(store.NewMemoryResourceStore(),
		resourcemanagercore.WithKnownProjects("configured-proj", []string{"extra-proj"}))
	return core, BuildRouter(core)
}

func do(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var reader *strings.Reader
	if body == "" {
		reader = strings.NewReader("")
	} else {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, reader)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decodeProject(t *testing.T, rec *httptest.ResponseRecorder) Project {
	t.Helper()
	var p Project
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("decode project: %v (body %s)", err, rec.Body.String())
	}
	return p
}

func decodeList(t *testing.T, rec *httptest.ResponseRecorder) ListProjectsResponse {
	t.Helper()
	var l ListProjectsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &l); err != nil {
		t.Fatalf("decode list: %v (body %s)", err, rec.Body.String())
	}
	return l
}

func TestListProjectsIncludesConfiguredAndCreated(t *testing.T) {
	core, h := newRouter()
	if _, _, err := core.CreateProject(t.Context(), resourcemanagercore.CreateProjectInput{ProjectID: "created-proj"}); err != nil {
		t.Fatalf("CreateProject: %v", err)
	}

	rec := do(t, h, http.MethodGet, "/projects", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	list := decodeList(t, rec)
	got := map[string]bool{}
	for _, p := range list.Projects {
		got[p.ProjectID] = true
	}
	for _, want := range []string{"configured-proj", "extra-proj", "created-proj"} {
		if !got[want] {
			t.Fatalf("list missing %q: %+v", want, list.Projects)
		}
	}
	if list.Total != len(list.Projects) || list.Total != 3 {
		t.Fatalf("total = %d, projects = %d, want 3", list.Total, len(list.Projects))
	}
}

func TestCreateProject(t *testing.T) {
	_, h := newRouter()

	rec := do(t, h, http.MethodPost, "/projects",
		`{"projectId":"new-proj","displayName":"New Project","labels":{"env":"dev"}}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (body %s)", rec.Code, rec.Body.String())
	}
	p := decodeProject(t, rec)
	if p.ProjectID != "new-proj" || p.State != "ACTIVE" {
		t.Fatalf("created project = %+v", p)
	}
	if p.DisplayName != "New Project" || p.ProjectNumber == "" || p.CreateTime == "" {
		t.Fatalf("created project missing fields: %+v", p)
	}
	if p.Labels["env"] != "dev" {
		t.Fatalf("labels not persisted: %+v", p.Labels)
	}

	// A duplicate id (including a configured project) is AlreadyExists → 409.
	for _, id := range []string{"new-proj", "configured-proj"} {
		rec = do(t, h, http.MethodPost, "/projects", `{"projectId":"`+id+`"}`)
		if rec.Code != http.StatusConflict {
			t.Fatalf("duplicate %q status = %d, want 409", id, rec.Code)
		}
	}

	// An invalid id grammar is InvalidArgument → 400.
	rec = do(t, h, http.MethodPost, "/projects", `{"projectId":"Bad"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid id status = %d, want 400", rec.Code)
	}
}

func TestDeleteAndUndelete(t *testing.T) {
	_, h := newRouter()
	do(t, h, http.MethodPost, "/projects", `{"projectId":"lifecycle-proj"}`)

	rec := do(t, h, http.MethodDelete, "/projects/lifecycle-proj", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("delete status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	if p := decodeProject(t, rec); p.State != "DELETE_REQUESTED" || p.DeleteTime == "" {
		t.Fatalf("deleted project = %+v", p)
	}

	// The manager keeps DELETE_REQUESTED projects visible.
	list := decodeList(t, do(t, h, http.MethodGet, "/projects", ""))
	found := false
	for _, p := range list.Projects {
		if p.ProjectID == "lifecycle-proj" {
			found = true
			if p.State != "DELETE_REQUESTED" {
				t.Fatalf("listed deleted project state = %q", p.State)
			}
		}
	}
	if !found {
		t.Fatalf("deleted project not listed: %+v", list.Projects)
	}

	rec = do(t, h, http.MethodPost, "/projects/lifecycle-proj/undelete", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("undelete status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	if p := decodeProject(t, rec); p.State != "ACTIVE" || p.DeleteTime != "" {
		t.Fatalf("undeleted project = %+v", p)
	}

	// Deleting an unknown id is NotFound → 404; undeleting an active project is
	// FailedPrecondition → 400.
	if rec = do(t, h, http.MethodDelete, "/projects/unknown-proj", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("delete unknown status = %d, want 404", rec.Code)
	}
	if rec = do(t, h, http.MethodPost, "/projects/lifecycle-proj/undelete", ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("undelete active status = %d, want 400", rec.Code)
	}
}

func TestGetProjectSynthesizesActive(t *testing.T) {
	_, h := newRouter()
	rec := do(t, h, http.MethodGet, "/projects/never-created", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	p := decodeProject(t, rec)
	if p.ProjectID != "never-created" || p.State != "ACTIVE" {
		t.Fatalf("synthesized project = %+v", p)
	}
}
