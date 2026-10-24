package datastoreui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the Datastore UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	// Kinds + per-kind property metadata, synthesized from the __kind__ and
	// __property__ metadata queries.
	r.Get("/kinds", h.ListKinds)
	r.Get("/kinds/{kind}/entities", h.ListEntities)
	r.Get("/kinds/{kind}/properties", h.ListProperties)

	// Entity CRUD. The key (full ancestor path + partition) travels as a JSON
	// KeyRef: in the ?key= query parameter for reads/deletes, in the body for
	// the upsert.
	r.Get("/entity", h.GetEntity)
	r.Put("/entity", h.UpsertEntity)
	r.Delete("/entity", h.DeleteEntity)

	// GQL query runner.
	r.Post("/query", h.RunQuery)

	return r
}
