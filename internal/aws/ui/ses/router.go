package sesui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the SES UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	r.Get("/identities", h.ListIdentities)
	r.Post("/identities", h.VerifyEmailIdentity)
	r.Delete("/identities/{identity}", h.DeleteIdentity)

	return r
}
