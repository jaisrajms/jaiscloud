package ecsui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the ECS UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/clusters", h.ListClusters)
	r.Post("/clusters", h.CreateCluster)
	r.Delete("/clusters/{name}", h.DeleteCluster)
	r.Get("/clusters/{name}/tasks", h.ListTasks)
	r.Post("/clusters/{name}/tasks", h.RunTask)
	r.Post("/tasks/{arn}/stop", h.StopTask)
	r.Get("/clusters/{name}/services", h.ListServices)

	return r
}
