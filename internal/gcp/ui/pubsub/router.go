package pubsubui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the Pub/Sub UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/topics", h.ListTopics)
	r.Post("/topics", h.CreateTopic)
	r.Get("/topics/{topic}", h.GetTopic)
	r.Delete("/topics/{topic}", h.DeleteTopic)
	r.Post("/topics/{topic}/publish", h.PublishTopic)
	r.Get("/topics/{topic}/iam", h.GetTopicIam)
	r.Put("/topics/{topic}/iam", h.PutTopicIam)

	r.Get("/subscriptions", h.ListSubscriptions)
	r.Post("/subscriptions", h.CreateSubscription)
	r.Get("/subscriptions/{subscription}", h.GetSubscription)
	r.Patch("/subscriptions/{subscription}", h.UpdateSubscription)
	r.Delete("/subscriptions/{subscription}", h.DeleteSubscription)
	r.Get("/subscriptions/{subscription}/iam", h.GetSubscriptionIam)
	r.Put("/subscriptions/{subscription}/iam", h.PutSubscriptionIam)

	return r
}
