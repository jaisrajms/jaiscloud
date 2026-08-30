package ssmui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns a chi.Router for SSM UI API endpoints.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/parameters", h.ListParameters)
	r.Post("/parameters", h.PutParameter)
	r.Get("/parameters/value", h.GetParameter)
	r.Get("/parameters/history", h.GetParameterHistory)
	// Parameter names can contain slashes — wildcard route for delete.
	r.Delete("/parameters/*", h.DeleteParameter)

	return r
}
