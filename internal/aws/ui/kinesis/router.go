package kinesisui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the Kinesis UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/streams", h.ListStreams)
	r.Post("/streams", h.CreateStream)
	r.Delete("/streams/{name}", h.DeleteStream)

	return r
}
