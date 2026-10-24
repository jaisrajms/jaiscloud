package bigqueryui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the BigQuery UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	// Datasets.
	r.Get("/datasets", h.ListDatasets)
	r.Post("/datasets", h.CreateDataset)
	r.Get("/datasets/{dataset}", h.GetDataset)
	r.Delete("/datasets/{dataset}", h.DeleteDataset)

	// Tables.
	r.Get("/datasets/{dataset}/tables", h.ListTables)
	r.Post("/datasets/{dataset}/tables", h.CreateTable)
	r.Get("/datasets/{dataset}/tables/{table}", h.GetTable)
	r.Delete("/datasets/{dataset}/tables/{table}", h.DeleteTable)
	r.Get("/datasets/{dataset}/tables/{table}/rows", h.ListRows)
	r.Post("/datasets/{dataset}/tables/{table}/rows", h.InsertRows)

	// Jobs.
	r.Get("/jobs", h.ListJobs)
	r.Get("/jobs/{job}", h.GetJob)
	r.Delete("/jobs/{job}", h.DeleteJob)
	r.Post("/jobs/{job}/cancel", h.CancelJob)

	// Query (jobs.query).
	r.Post("/query", h.RunQuery)

	return r
}
