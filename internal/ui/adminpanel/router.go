package adminpanel

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/admin"
)

// BuildRouter returns the chi router for the cloud-neutral Admin Panel UI API.
func BuildRouter(a *admin.Handler) chi.Router {
	h := NewHandler(a)
	r := chi.NewRouter()

	r.Get("/status", h.Status)
	r.Post("/reset", h.Reset)
	r.Get("/export-info", h.ExportInfo)

	r.Get("/clock", h.GetClock)
	r.Post("/clock", h.SetClock)

	r.Get("/snapshots", h.ListSnapshots)
	r.Post("/snapshots", h.CreateSnapshot)
	r.Post("/snapshots/{name}/revert", h.RevertSnapshot)
	r.Delete("/snapshots/{name}", h.DeleteSnapshot)

	return r
}
