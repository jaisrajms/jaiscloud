package ui

import (
	"net/http"

	"jaiscloud/internal/admin"
	"jaiscloud/internal/config"
)

func buildMetaHandler(adminHandler *admin.Handler, cfg *config.Config, version string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		meta := adminHandler.Meta()

		mode := "memory"
		if cfg.Ephemeral {
			mode = "ephemeral"
		} else if cfg.DSN != "" {
			mode = "postgres"
		}

		writeJSON(w, MetaResponse{
			Cloud:      string(cfg.Cloud),
			Region:     cfg.Region,
			AccountId:  cfg.AccountID,
			Mode:       mode,
			Version:    version,
			UIVersion:  "dev",
			InstanceId: meta.InstanceID,
		})
	}
}

// AccountsResponse is the payload for GET /api/ui/v1/meta/accounts.
type AccountsResponse struct {
	Accounts []string `json:"accounts"`
}

func buildAccountsHandler(cfg *config.Config) http.HandlerFunc {
	accounts := append([]string{cfg.AccountID}, cfg.ExtraAccounts...)
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, AccountsResponse{Accounts: accounts})
	}
}
