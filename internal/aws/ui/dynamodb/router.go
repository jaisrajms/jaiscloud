package dynamodbui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the DynamoDB UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/tables", h.ListTables)
	r.Post("/tables", h.CreateTable)
	r.Get("/tables/{table}", h.GetTable)
	r.Delete("/tables/{table}", h.DeleteTable)

	r.Get("/tables/{table}/scan", h.ScanTable)
	r.Post("/tables/{table}/query", h.QueryTable)

	r.Post("/tables/{table}/items", h.PutItem)
	r.Post("/tables/{table}/items/get", h.GetItem)
	r.Delete("/tables/{table}/items", h.DeleteItem)

	return r
}
