package serviceaccount

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
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

// Exister adapts LookupAccount to the existence-check interfaces other services
// hold (for example Eventarc's Trigger.serviceAccount validation), so a core
// resolves an account without importing the IAM provider.
type Exister struct{ Resources store.ResourceStore }

// ServiceAccountExists reports whether identifier names a live service account
// in project. A malformed or unknown account is (false, nil); a store failure
// is returned.
func (e Exister) ServiceAccountExists(ctx context.Context, project, identifier string) (bool, error) {
	_, _, err := LookupAccount(ctx, e.Resources, project, identifier)
	if err == nil {
		return true, nil
	}
	var pe *model.ProviderError
	if errors.As(err, &pe) && pe.HTTPStatus == 404 {
		return false, nil
	}
	return false, err
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

// delegatePrefix is the documented prefix of a delegation-chain entry
// (IAM Credentials GenerateAccessToken.delegates and its siblings). The project
// segment must be the "-" wildcard: the Discovery document states that replacing
// it with a project id is invalid.
const delegatePrefix = "projects/-/serviceAccounts/" //gcplint:ignore resource-prefix documented delegate prefix, parsed rather than built

// ValidateDelegates checks the documented delegation chain of an IAM Credentials
// request against the service-account registry.
//
// The documented chain runs caller → delegates[0] → … → target, and the caller
// and the target are explicitly not part of it. The emulator has no IAM
// authorization plane — role bindings are stored but never enforced — so the
// Token Creator grants between consecutive accounts cannot be checked; what is
// checked is everything the wire contract states structurally: every entry is a
// "projects/-/serviceAccounts/{ACCOUNT_EMAIL_OR_UNIQUEID}" name, every delegate
// is a service account that exists, no delegate repeats, and the chain never
// names the account the credential is for. Real GCP publishes no maximum chain
// length, so none is imposed.
//
// targetEmail is the account the credential is minted for.
func ValidateDelegates(ctx context.Context, resources store.ResourceStore, targetEmail string, delegates []string) error {
	if len(delegates) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(delegates))
	for _, d := range delegates {
		identifier := strings.TrimPrefix(d, delegatePrefix)
		if identifier == d || identifier == "" || strings.Contains(identifier, "/") {
			return Invalid("delegate " + strconv.Quote(d) + " must have the form " + delegatePrefix + "{ACCOUNT_EMAIL_OR_UNIQUEID}")
		}
		_, email, err := LookupAccount(ctx, resources, "-", identifier)
		if err != nil {
			return err
		}
		if email == targetEmail {
			return Invalid("delegates must not include the service account the credential is for")
		}
		if _, dup := seen[email]; dup {
			return Invalid("delegates must not repeat the service account " + strconv.Quote(email))
		}
		seen[email] = struct{}{}
	}
	return nil
}
