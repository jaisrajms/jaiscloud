package dataprocui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the Dataproc UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/clusters", h.ListClusters)
	r.Post("/clusters", h.CreateCluster)
	r.Get("/clusters/{region}/{cluster}", h.GetCluster)
	r.Post("/clusters/{region}/{cluster}/start", h.StartCluster)
	r.Post("/clusters/{region}/{cluster}/stop", h.StopCluster)
	r.Delete("/clusters/{region}/{cluster}", h.DeleteCluster)

	r.Get("/jobs", h.ListJobs)
	r.Get("/jobs/{region}/{job}", h.GetJob)
	r.Post("/jobs/{region}/{job}/cancel", h.CancelJob)

	r.Get("/workflow-templates", h.ListWorkflowTemplates)
	r.Get("/workflow-templates/{region}/{template}", h.GetWorkflowTemplate)

	return r
}
