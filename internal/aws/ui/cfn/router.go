package cfnui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the CloudFormation UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/stacks", h.ListStacks)
	r.Post("/stacks", h.CreateStack)
	r.Delete("/stacks/{name}", h.DeleteStack)

	return r
}
