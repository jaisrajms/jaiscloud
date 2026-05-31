package secretsmanagerui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns a chi.Router for SecretsManager UI API endpoints.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/secrets", h.ListSecrets)
	r.Post("/secrets", h.CreateSecret)
	r.Get("/secrets/{name}", h.GetSecret)
	r.Delete("/secrets/{name}", h.DeleteSecret)
	r.Get("/secrets/{name}/value", h.GetSecretValue)
	r.Post("/secrets/{name}/value", h.PutSecretValue)
	r.Get("/secrets/{name}/versions", h.ListSecretVersions)

	return r
}
