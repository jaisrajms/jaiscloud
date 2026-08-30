package eventbridgeui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the EventBridge UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/rules", h.ListRules)
	r.Post("/rules", h.PutRule)
	r.Delete("/rules/{name}", h.DeleteRule)
	r.Post("/rules/{name}/enable", h.EnableRule)
	r.Post("/rules/{name}/disable", h.DisableRule)
	r.Get("/rules/{name}/targets", h.ListTargets)
	r.Post("/rules/{name}/targets", h.PutTargets)
	r.Delete("/rules/{name}/targets/{targetId}", h.RemoveTarget)
	r.Post("/events", h.PutEvents)
	r.Get("/buses", h.ListEventBuses)
	r.Post("/buses", h.CreateEventBus)
	r.Delete("/buses/{name}", h.DeleteEventBus)

	return r
}
