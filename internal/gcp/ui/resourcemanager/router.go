package resourcemanagerui

import (
	"github.com/go-chi/chi/v5"
)

// BuildRouter returns the chi router for the Cloud Resource Manager UI API
// (the console project manager). The manager is project-agnostic, so unlike the
// per-project service routers it needs no config.
func BuildRouter(p ProviderInterface) chi.Router {
	h := NewHandler(p)
	r := chi.NewRouter()

	r.Get("/projects", h.ListProjects)
	r.Post("/projects", h.CreateProject)
	r.Get("/projects/{project}", h.GetProject)
	r.Delete("/projects/{project}", h.DeleteProject)
	r.Post("/projects/{project}/undelete", h.UndeleteProject)

	return r
}
