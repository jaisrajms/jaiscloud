package glueui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the Glue UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/databases", h.ListDatabases)
	r.Post("/databases", h.CreateDatabase)
	r.Delete("/databases/{name}", h.DeleteDatabase)
	r.Get("/databases/{db}/tables", h.ListTables)
	r.Post("/databases/{db}/tables", h.CreateTable)
	r.Delete("/databases/{db}/tables/{name}", h.DeleteTable)
	r.Get("/jobs", h.ListJobs)
	r.Post("/jobs", h.CreateJob)
	r.Delete("/jobs/{name}", h.DeleteJob)
	r.Post("/jobs/{name}/runs", h.StartJobRun)
	r.Get("/jobs/{name}/runs", h.ListJobRuns)
	r.Get("/crawlers", h.ListCrawlers)
	r.Post("/crawlers", h.CreateCrawler)
	r.Delete("/crawlers/{name}", h.DeleteCrawler)
	r.Post("/crawlers/{name}/start", h.StartCrawler)

	return r
}
