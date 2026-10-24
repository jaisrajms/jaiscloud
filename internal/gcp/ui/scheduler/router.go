package schedulerui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the Cloud Scheduler UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/jobs", h.ListJobs)
	r.Post("/jobs", h.CreateJob)
	r.Get("/jobs/{location}/{job}", h.GetJob)
	r.Put("/jobs/{location}/{job}", h.UpdateJob)
	r.Delete("/jobs/{location}/{job}", h.DeleteJob)
	r.Post("/jobs/{location}/{job}/pause", h.PauseJob)
	r.Post("/jobs/{location}/{job}/resume", h.ResumeJob)
	r.Post("/jobs/{location}/{job}/run", h.RunJob)

	return r
}
