package firehoseui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the Firehose UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/streams", h.ListDeliveryStreams)
	r.Post("/streams", h.CreateDeliveryStream)
	r.Delete("/streams/{name}", h.DeleteDeliveryStream)

	return r
}
