package secretmanagerui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the Secret Manager UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	// Secrets.
	r.Get("/secrets", h.ListSecrets)
	r.Post("/secrets", h.CreateSecret)
	r.Get("/secrets/{secret}", h.GetSecret)
	r.Patch("/secrets/{secret}", h.UpdateSecret)
	r.Delete("/secrets/{secret}", h.DeleteSecret)

	// IAM policy.
	r.Get("/secrets/{secret}/iam", h.GetIamPolicy)
	r.Put("/secrets/{secret}/iam", h.SetIamPolicy)
	r.Post("/secrets/{secret}/iam/testIamPermissions", h.TestIamPermissions)

	// Versions.
	r.Get("/secrets/{secret}/versions", h.ListVersions)
	r.Post("/secrets/{secret}/versions", h.AddVersion)
	r.Get("/secrets/{secret}/versions/{version}", h.GetVersion)
	r.Get("/secrets/{secret}/versions/{version}/access", h.AccessVersion)
	r.Post("/secrets/{secret}/versions/{version}/destroy", h.DestroyVersion)
	r.Post("/secrets/{secret}/versions/{version}/disable", h.DisableVersion)
	r.Post("/secrets/{secret}/versions/{version}/enable", h.EnableVersion)

	return r
}
