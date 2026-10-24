package workflowsui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the Cloud Workflows UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/workflows", h.ListWorkflows)
	r.Post("/workflows", h.CreateWorkflow)
	r.Get("/workflows/{location}/{workflow}", h.GetWorkflow)
	r.Put("/workflows/{location}/{workflow}", h.UpdateWorkflow)
	r.Delete("/workflows/{location}/{workflow}", h.DeleteWorkflow)
	r.Get("/workflows/{location}/{workflow}/executions", h.ListExecutions)
	r.Post("/workflows/{location}/{workflow}/executions", h.RunWorkflow)
	r.Get("/workflows/{location}/{workflow}/executions/{execution}", h.GetExecution)
	r.Post("/workflows/{location}/{workflow}/executions/{execution}/cancel", h.CancelExecution)

	return r
}
