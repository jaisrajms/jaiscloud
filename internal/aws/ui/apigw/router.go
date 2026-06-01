package apigwui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the API Gateway UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/apis", h.ListRestAPIs)
	r.Post("/apis", h.CreateRestAPI)
	r.Delete("/apis/{id}", h.DeleteRestAPI)
	r.Get("/apis/{id}/resources", h.ListResources)
	r.Get("/apis/{id}/stages", h.ListStages)
	r.Get("/apis/{id}/deployments", h.ListDeployments)
	r.Post("/apis/{id}/deployments", h.CreateDeployment)

	return r
}
