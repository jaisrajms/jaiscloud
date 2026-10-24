package ui

import (
	"jaiscloud/internal/admin"
	"jaiscloud/internal/config"
	"jaiscloud/internal/events"
	coreui "jaiscloud/internal/ui"
)

// UIServer is the shared UI server type, re-exported so existing AWS callers
// keep the `ui.UIServer` spelling.
type UIServer = coreui.UIServer

// OpenBrowser opens url in the platform browser (shared implementation).
var OpenBrowser = coreui.OpenBrowser

// New creates the AWS UI server backed by the shared UI core.
// Returns (nil, nil) when the binary was built without -tags ui.
func New(providers *AWSProviders, adminHandler *admin.Handler, cfg *config.Config, bus *events.EventBus, version string) (*coreui.UIServer, error) {
	return coreui.New(NewRegistrar(providers, cfg), adminHandler, cfg, bus, version)
}
