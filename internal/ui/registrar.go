package ui

import (
	"context"

	"github.com/go-chi/chi/v5"

	"jaiscloud/internal/model"
)

// Registrar contributes one cloud's UI surface to the shared UI core.
// The AWS build supplies an AWS registrar, the GCP build a GCP registrar.
type Registrar interface {
	// Cloud is the emulated cloud identity reported by /api/ui/v1/meta.
	Cloud() model.Cloud
	// Services lists the service catalog entries to render in the navigation.
	// A service is listed only when its provider is wired.
	Services() []ServiceDescriptor
	// MountRoutes registers the cloud's service API routes. It is invoked
	// inside the authenticated route group, so handlers assume a valid
	// session token.
	MountRoutes(r chi.Router)
}

// AccountsProvider is an optional Registrar capability for a cloud whose
// tenancy unit is enumerable rather than fixed config. When a Registrar
// implements it, GET /api/ui/v1/meta/accounts returns the configured default +
// extra accounts unioned with the ids it contributes (e.g. GCP projects
// created at runtime), deduplicated and sorted. A cloud whose accounts are a
// fixed config echo simply does not implement it.
type AccountsProvider interface {
	Accounts(ctx context.Context) []string
}
