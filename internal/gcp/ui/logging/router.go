package loggingui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the Cloud Logging UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	// Logs explorer.
	r.Get("/entries", h.ListEntries)
	r.Post("/entries", h.ListEntries)
	r.Get("/logs", h.ListLogs)
	r.Delete("/logs/{log}", h.DeleteLog)

	// Logs-based metrics.
	r.Get("/metrics", h.ListMetrics)
	r.Post("/metrics", h.CreateMetric)
	r.Get("/metrics/{metric}", h.GetMetric)
	r.Put("/metrics/{metric}", h.UpdateMetric)
	r.Delete("/metrics/{metric}", h.DeleteMetric)

	// Log router sinks.
	r.Get("/sinks", h.ListSinks)
	r.Post("/sinks", h.CreateSink)
	r.Get("/sinks/{id}", h.GetSink)
	r.Patch("/sinks/{id}", h.UpdateSink)
	r.Delete("/sinks/{id}", h.DeleteSink)

	// Resource-level exclusions.
	r.Get("/exclusions", h.ListExclusions)
	r.Post("/exclusions", h.CreateExclusion)
	r.Get("/exclusions/{id}", h.GetExclusion)
	r.Patch("/exclusions/{id}", h.UpdateExclusion)
	r.Delete("/exclusions/{id}", h.DeleteExclusion)

	return r
}
