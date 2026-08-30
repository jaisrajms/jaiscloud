package kmsui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns a chi.Router for KMS UI API endpoints.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/keys", h.ListKeys)
	r.Post("/keys", h.CreateKey)
	r.Get("/keys/detail", h.GetKey)
	r.Post("/keys/enable", h.EnableKey)
	r.Post("/keys/disable", h.DisableKey)
	r.Post("/keys/schedule-deletion", h.ScheduleKeyDeletion)
	r.Post("/keys/cancel-deletion", h.CancelKeyDeletion)

	r.Get("/aliases", h.ListAliases)
	r.Post("/aliases", h.CreateAlias)
	r.Delete("/aliases", h.DeleteAlias)

	return r
}
