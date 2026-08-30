package elasticacheui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the ElastiCache UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/clusters", h.ListCacheClusters)
	r.Post("/clusters", h.CreateCacheCluster)
	r.Delete("/clusters/{id}", h.DeleteCacheCluster)

	return r
}
