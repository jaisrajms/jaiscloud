package serviceaccount

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// AccountResourceType is the ResourceStore type of a service-account record
// written by the IAM provider (projects.serviceAccounts.create). The signing
// surfaces resolve the account they sign for through this same registry: real
// GCP signs only for a service account that exists, so an identifier with no
// record is NOT_FOUND rather than a lazily created key.
const AccountResourceType = "gcp_service_account"

// Account is the minimal projection of a stored service-account record that the
// signing surfaces need to resolve an identifier.
type Account struct {
	Email    string `json:"email"`
	UniqueID string `json:"uniqueId,omitempty"`
}

// AccountNotFound is the 404 NOT_FOUND provider error every signing surface
// reports for a service account that does not exist. Real IAM Credentials
// resolves the account before signing and answers NOT_FOUND for a miss; the
// emulator no longer materialises a key for one.
func AccountNotFound() error {
	return model.NewProviderError("NotFound", "service account not found", 404)
}

// LookupAccount resolves the service account named by identifier — its email or
// its numeric unique id — and returns the store account that owns it and its
// canonical email. account may be the "projects/-" wildcard, which resolves from
// the email's project (AccountForSA). A unique id names no project of its own,
// so the wildcard cannot scope it: the ResourceStore is per-account and has no
// global index, the same limitation the IAM management surface records (DC29).
//
// A miss is AccountNotFound.
func LookupAccount(ctx context.Context, resources store.ResourceStore, account, identifier string) (string, string, error) {
	if identifier == "" {
		return "", "", AccountNotFound()
	}
	if strings.Contains(identifier, "@") {
		acc := AccountForSA(account, identifier)
		if acc == "" || acc == "-" {
			// The path project is the wildcard and the identifier embeds no
			// project of its own, so the account cannot be scoped.
			return "", "", AccountNotFound()
		}
		if _, err := resources.Get(ctx, acc, store.GlobalRegion, AccountResourceType, identifier); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return "", "", AccountNotFound()
			}
			return "", "", err
		}
		return acc, identifier, nil
	}
	if account == "" || account == "-" {
		return "", "", AccountNotFound()
	}
	entries, err := resources.List(ctx, account, store.GlobalRegion, AccountResourceType, "")
	if err != nil {
		return "", "", err
	}
	for _, e := range entries {
		var m Account
		if json.Unmarshal(e.Data, &m) != nil {
			continue
		}
		if m.UniqueID != "" && m.UniqueID == identifier {
			return account, m.Email, nil
		}
	}
	return "", "", AccountNotFound()
}
