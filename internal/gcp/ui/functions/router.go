package functionsui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the Cloud Functions UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/functions", h.ListFunctions)
	r.Post("/functions", h.CreateFunction)
	r.Post("/functions/upload", h.GenerateUploadURL)
	r.Put("/functions/upload", h.UploadSource)
	r.Get("/functions/{location}/{function}", h.GetFunction)
	r.Delete("/functions/{location}/{function}", h.DeleteFunction)
	r.Post("/functions/{location}/{function}/call", h.CallFunction)
	r.Get("/functions/{location}/{function}/iam", h.GetIam)
	r.Put("/functions/{location}/{function}/iam", h.SetIam)
	r.Get("/functions/{location}/{function}/deliveries", h.ListDeliveries)

	return r
}
