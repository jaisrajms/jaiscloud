package route53ui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the Route53 UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/zones", h.ListHostedZones)
	r.Post("/zones", h.CreateHostedZone)
	r.Delete("/zones/{id}", h.DeleteHostedZone)
	r.Get("/zones/{id}/records", h.ListResourceRecordSets)

	return r
}
