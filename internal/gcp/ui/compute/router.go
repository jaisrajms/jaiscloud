package computeui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the Compute Engine UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/instances", h.ListInstances)
	r.Get("/instances/{zone}/{instance}", h.GetInstance)
	r.Post("/instances/{zone}/{instance}/start", h.StartInstance)
	r.Post("/instances/{zone}/{instance}/stop", h.StopInstance)
	r.Delete("/instances/{zone}/{instance}", h.DeleteInstance)

	return r
}
