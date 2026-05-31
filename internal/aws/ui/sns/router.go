package snsui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the SNS UI API.
// Topic-scoped routes use ?arn=<encoded> to avoid ARN slashes in path params.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/topics", h.ListTopics)
	r.Post("/topics", h.CreateTopic)
	r.Get("/topics/detail", h.GetTopic)
	r.Delete("/topics", h.DeleteTopic)

	r.Get("/topics/subscriptions", h.ListSubscriptionsByTopic)
	r.Post("/topics/subscribe", h.Subscribe)
	r.Delete("/topics/subscriptions/{subArn}", h.Unsubscribe)

	r.Post("/topics/publish", h.Publish)

	return r
}
