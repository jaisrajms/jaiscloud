package ui

import (
	"encoding/json"
	"net/http"
	"sort"

	"jaiscloud/internal/admin"
	"jaiscloud/internal/config"
)

func buildMetaHandler(adminHandler *admin.Handler, cfg *config.Config, version string, cloud string, bootID string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		meta := adminHandler.Meta()

		mode := "memory"
		if cfg.Ephemeral {
			mode = "ephemeral"
		} else if cfg.DSN != "" {
			mode = "postgres"
		}

		writeJSON(w, MetaResponse{
			Cloud:      cloud,
			Region:     cfg.Region,
			AccountId:  cfg.AccountID,
			Mode:       mode,
			Version:    version,
			UIVersion:  "dev",
			InstanceId: meta.InstanceID,
			BootID:     bootID,
		})
	}
}

// buildAccountsHandler serves GET /api/ui/v1/meta/accounts. The configured
// default + extra accounts are always listed; a Registrar that implements
// AccountsProvider contributes additional ids (GCP projects created at
// runtime), so the console's project picker reflects the real registry rather
// than only the startup config.
func buildAccountsHandler(cfg *config.Config, reg Registrar) http.HandlerFunc {
	defaults := append([]string{cfg.AccountID}, cfg.ExtraAccounts...)
	provider, _ := reg.(AccountsProvider)
	return func(w http.ResponseWriter, r *http.Request) {
		accounts := defaults
		if provider != nil {
			accounts = mergeAccounts(defaults, provider.Accounts(r.Context()))
		}
		writeJSON(w, AccountsResponse{Accounts: accounts})
	}
}

// mergeAccounts unions the configured default accounts with the ids a
// Registrar contributes, dropping empty ids and duplicates and sorting the
// result so the response is deterministic.
func mergeAccounts(defaults, extra []string) []string {
	seen := make(map[string]bool, len(defaults)+len(extra))
	out := make([]string, 0, len(defaults)+len(extra))
	for _, list := range [][]string{defaults, extra} {
		for _, a := range list {
			if a == "" || seen[a] {
				continue
			}
			seen[a] = true
			out = append(out, a)
		}
	}
	sort.Strings(out)
	return out
}

// buildServicesHandler reports the services the active registrar supports.
func buildServicesHandler(reg Registrar) http.HandlerFunc {
	services := reg.Services()
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, ServicesResponse{Services: services})
	}
}

// writeJSON encodes v as JSON with Content-Type application/json.
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}
