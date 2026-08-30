package ec2ui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the EC2 UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/instances", h.ListInstances)
	r.Delete("/instances/{id}", h.TerminateInstance)
	r.Post("/instances/{id}/start", h.StartInstance)
	r.Post("/instances/{id}/stop", h.StopInstance)

	return r
}
