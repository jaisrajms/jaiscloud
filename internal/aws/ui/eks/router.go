package eksui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the EKS UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/clusters", h.ListClusters)
	r.Post("/clusters", h.CreateCluster)
	r.Delete("/clusters/{name}", h.DeleteCluster)

	return r
}
