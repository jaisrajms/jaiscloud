package runui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the Cloud Run UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/services", h.ListServices)
	r.Get("/services/{region}/{service}", h.GetService)
	r.Delete("/services/{region}/{service}", h.DeleteService)
	r.Get("/services/{region}/{service}/revisions", h.ListRevisions)
	r.Get("/services/{region}/{service}/revisions/{revision}", h.GetRevision)

	return r
}
