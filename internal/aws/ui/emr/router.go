package emrui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the EMR UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/clusters", h.ListClusters)
	r.Post("/clusters", h.RunJobFlow)
	r.Get("/clusters/{id}", h.DescribeCluster)
	r.Delete("/clusters/{id}", h.TerminateCluster)
	r.Get("/clusters/{id}/status", h.GetClusterStatus)
	r.Get("/clusters/{id}/steps", h.ListSteps)
	r.Post("/clusters/{id}/steps", h.AddSteps)
	r.Get("/clusters/{id}/steps/{stepId}", h.DescribeStep)
	r.Post("/clusters/{id}/steps/{stepId}/cancel", h.CancelStep)

	return r
}
