package elbv2ui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the ELBv2 UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/load-balancers", h.ListLoadBalancers)
	r.Post("/load-balancers", h.CreateLoadBalancer)
	r.Delete("/load-balancers/{arn}", h.DeleteLoadBalancer)

	return r
}
