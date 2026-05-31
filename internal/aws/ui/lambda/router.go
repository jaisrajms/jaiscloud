package lambdaui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the Lambda UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/functions", h.ListFunctions)
	r.Get("/functions/{name}", h.GetFunction)
	r.Delete("/functions/{name}", h.DeleteFunction)
	r.Post("/functions/{name}/invoke", h.InvokeFunction)
	r.Patch("/functions/{name}/config", h.UpdateConfig)
	r.Get("/functions/{name}/status", h.GetFunctionStatus)

	return r
}
