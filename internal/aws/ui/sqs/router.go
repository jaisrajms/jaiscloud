package sqsui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the SQS UI API.
// All queue-scoped routes use ?url=<encoded> (not Chi path params)
// because SQS queue URLs contain slashes unreliable in path params.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/queues", h.ListQueues)
	r.Post("/queues", h.CreateQueue)
	r.Get("/queues/detail", h.GetQueue)
	r.Delete("/queues", h.DeleteQueue)
	r.Post("/queues/purge", h.PurgeQueue)
	r.Post("/queues/messages", h.SendMessage)
	r.Get("/queues/messages", h.ReceiveMessages)
	r.Delete("/queues/messages/receipt", h.DeleteMessage)
	r.Get("/queues/messages/peek", h.PeekMessages)
	r.Get("/queues/dlq-sources", h.ListDLQSources)
	r.Get("/queues/tags", h.GetTags)

	return r
}
