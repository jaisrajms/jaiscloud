package iamui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns the chi router for the IAM UI API.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	// Service accounts.
	r.Get("/serviceAccounts", h.ListServiceAccounts)
	r.Post("/serviceAccounts", h.CreateServiceAccount)
	r.Get("/serviceAccounts/{email}", h.GetServiceAccount)
	r.Patch("/serviceAccounts/{email}", h.UpdateServiceAccount)
	r.Delete("/serviceAccounts/{email}", h.DeleteServiceAccount)
	r.Post("/serviceAccounts/{email}/disable", h.DisableServiceAccount)
	r.Post("/serviceAccounts/{email}/enable", h.EnableServiceAccount)

	// IAM policy.
	r.Get("/serviceAccounts/{email}/iam", h.GetIamPolicy)
	r.Put("/serviceAccounts/{email}/iam", h.SetIamPolicy)
	r.Post("/serviceAccounts/{email}/iam/testIamPermissions", h.TestIamPermissions)

	// Keys.
	r.Get("/serviceAccounts/{email}/keys", h.ListKeys)
	r.Post("/serviceAccounts/{email}/keys", h.CreateKey)
	r.Get("/serviceAccounts/{email}/keys/{key}", h.GetKey)
	r.Delete("/serviceAccounts/{email}/keys/{key}", h.DeleteKey)
	r.Post("/serviceAccounts/{email}/keys/{key}/disable", h.DisableKey)
	r.Post("/serviceAccounts/{email}/keys/{key}/enable", h.EnableKey)

	// Signing.
	r.Post("/serviceAccounts/{email}/signBlob", h.SignBlob)
	r.Post("/serviceAccounts/{email}/signJwt", h.SignJwt)

	return r
}
