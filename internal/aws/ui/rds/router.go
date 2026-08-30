package rdsui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the RDS UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/instances", h.ListDBInstances)
	r.Post("/instances", h.CreateDBInstance)
	r.Delete("/instances/{id}", h.DeleteDBInstance)
	r.Post("/instances/{id}/start", h.StartDBInstance)
	r.Post("/instances/{id}/stop", h.StopDBInstance)

	return r
}
