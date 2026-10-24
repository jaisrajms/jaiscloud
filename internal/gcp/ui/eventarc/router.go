package eventarcui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the Eventarc UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/triggers", h.ListTriggers)
	r.Post("/triggers", h.CreateTrigger)
	r.Get("/triggers/{location}/{trigger}", h.GetTrigger)
	r.Put("/triggers/{location}/{trigger}", h.UpdateTrigger)
	r.Delete("/triggers/{location}/{trigger}", h.DeleteTrigger)
	r.Get("/triggers/{location}/{trigger}/iam", h.GetTriggerIam)
	r.Put("/triggers/{location}/{trigger}/iam", h.SetTriggerIam)

	r.Get("/channels", h.ListChannels)
	r.Post("/channels", h.CreateChannel)
	r.Get("/channels/{location}/{channel}", h.GetChannel)
	r.Put("/channels/{location}/{channel}", h.UpdateChannel)
	r.Delete("/channels/{location}/{channel}", h.DeleteChannel)
	r.Get("/channels/{location}/{channel}/iam", h.GetChannelIam)
	r.Put("/channels/{location}/{channel}/iam", h.SetChannelIam)

	return r
}
