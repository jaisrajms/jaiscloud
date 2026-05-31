package logsui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the CloudWatch Logs UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/groups", h.ListLogGroups)
	r.Post("/groups", h.CreateLogGroup)
	r.Delete("/groups/{name}", h.DeleteLogGroup)
	r.Put("/groups/{name}/retention", h.SetRetention)
	r.Get("/groups/{name}/streams", h.ListLogStreams)
	r.Get("/groups/{name}/streams/{stream}/events", h.GetLogEvents)
	r.Post("/groups/{name}/filter", h.FilterLogEvents)

	return r
}
