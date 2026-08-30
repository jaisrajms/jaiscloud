package iamui

import (
	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/config"
)

// BuildRouter returns a chi.Router for IAM UI API endpoints.
func BuildRouter(p ProviderInterface, cfg *config.Config) chi.Router {
	h := NewHandler(p, cfg)
	r := chi.NewRouter()

	// Roles
	r.Get("/roles", h.ListRoles)
	r.Post("/roles", h.CreateRole)
	r.Get("/roles/{roleName}", h.GetRole)
	r.Delete("/roles/{roleName}", h.DeleteRole)
	r.Get("/roles/{roleName}/policies", h.ListAttachedRolePolicies)
	r.Post("/roles/{roleName}/policies", h.AttachRolePolicy)
	r.Delete("/roles/{roleName}/policies", h.DetachRolePolicy)

	// Users
	r.Get("/users", h.ListUsers)
	r.Post("/users", h.CreateUser)
	r.Delete("/users/{userName}", h.DeleteUser)
	r.Get("/users/{userName}/access-keys", h.ListAccessKeys)
	r.Post("/users/{userName}/access-keys", h.CreateAccessKey)
	r.Delete("/users/{userName}/access-keys", h.DeleteAccessKey)

	// Policies
	r.Get("/policies", h.ListPolicies)
	r.Post("/policies", h.CreatePolicy)
	r.Delete("/policies", h.DeletePolicy)

	return r
}
