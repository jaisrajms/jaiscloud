package serviceaccount

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"

	"jaiscloud/internal/gcp/resource"
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

// DefaultAccount is one Google-managed service account a project has without an
// explicit projects.serviceAccounts.create. Real IAM renders it through the
// same serviceAccounts.get/list surface as a created account; the emulator
// materializes no store record, so the IAM provider synthesizes this shape.
//
// The captured shape (2026-11-09, live project with Compute enabled):
//
//	email       {project}@appspot.gserviceaccount.com or
//	            {number}-compute@developer.gserviceaccount.com
//	displayName "App Engine default service account" /
//	            "Compute Engine default service account"
//	uniqueId    21-digit numeric; oauth2ClientId is the same value
//	etag        the fixed DefaultAccountEtag
type DefaultAccount struct {
	Email       string
	DisplayName string
	// UniqueID is the account's numeric unique id; real IAM reports the same
	// value as oauth2ClientId for a Google-managed default.
	UniqueID string
}

// DefaultAccountEtag is the etag real IAM reports for a Google-managed default
// account. It is a fixed value rather than a hash of the account (the App
// Engine and Compute Engine defaults answer the same etag), captured from a
// live project on 2026-11-09.
const DefaultAccountEtag = "MDEwMjE5MjA="

// Defaults returns the Google-managed service accounts project has without an
// explicit create: the App Engine default ({project}@appspot...) and the
// Compute Engine default ({number}-compute@developer...). Real GCP materializes
// each when its owning API is enabled; the emulator serves appspot and compute
// for every project because its served APIs are not enablement-gated.
//
// The Google APIs service agent ({number}@cloudservices.gserviceaccount.com) is
// deliberately not included: it is not a default every project has (a captured
// project with dozens of enabled APIs answers 404 for it), so treating it as
// live would make the emulator serve an account real GCP does not.
func Defaults(project string) []DefaultAccount {
	if project == "" || project == "-" {
		return nil
	}
	number := resource.ProjectNumberFor(project)
	appspot := project + "@appspot.gserviceaccount.com"
	compute := number + "-compute@developer.gserviceaccount.com"
	return []DefaultAccount{
		{Email: appspot, DisplayName: "App Engine default service account", UniqueID: ManagedUniqueID(appspot)},
		{Email: compute, DisplayName: "Compute Engine default service account", UniqueID: ManagedUniqueID(compute)},
	}
}

// DefaultByEmail returns the default account of project named by email.
func DefaultByEmail(project, email string) (DefaultAccount, bool) {
	for _, d := range Defaults(project) {
		if d.Email == email {
			return d, true
		}
	}
	return DefaultAccount{}, false
}

// DefaultByUniqueID returns the default account of project carrying uniqueID.
func DefaultByUniqueID(project, uniqueID string) (DefaultAccount, bool) {
	if uniqueID == "" {
		return DefaultAccount{}, false
	}
	for _, d := range Defaults(project) {
		if d.UniqueID == uniqueID {
			return d, true
		}
	}
	return DefaultAccount{}, false
}

// DefaultEmailProject returns the project id named by an App Engine default
// account email ({project}@appspot.gserviceaccount.com), or "" for any other
// email. Such an email names its project directly, so it resolves even through
// the "projects/-" wildcard.
func DefaultEmailProject(email string) string {
	const suffix = "@appspot.gserviceaccount.com"
	project, ok := strings.CutSuffix(email, suffix)
	if !ok || project == "" || strings.Contains(project, "@") {
		return ""
	}
	return project
}

// DefaultEmailNumber returns the project number named by a number-based
// default-account email ({number}-compute@developer.gserviceaccount.com), or ""
// for any other email. A number is not invertible to a project id, so a caller
// must resolve it through the project registry.
func DefaultEmailNumber(email string) string {
	const suffix = "-compute@developer.gserviceaccount.com"
	number, ok := strings.CutSuffix(email, suffix)
	if !ok || number == "" {
		return ""
	}
	for _, c := range number {
		if c < '0' || c > '9' {
			return ""
		}
	}
	return number
}

// ManagedUniqueID returns the deterministic 21-digit numeric uniqueId of a
// Google-managed service account derived from its email. It is deterministic (a
// read must not churn) and in the system-assigned range: real IAM's managed
// accounts carry 21-digit ids beginning with "1" (captured:
// 111361368305025563191), so the hash supplies the trailing digits. It backs
// both the App Engine/Compute default accounts and the service agents
// synthesized by Service Usage's generateServiceIdentity.
func ManagedUniqueID(email string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(email))
	return fmt.Sprintf("1%020d", h.Sum64())
}

// ImplicitDefault reports whether email names a Google-managed default account
// every project has without an explicit create (DefaultByEmail). Real GCP
// resolves them, so a reference check must not report one missing just because
// the emulator never materialized the record.
func ImplicitDefault(project, email string) bool {
	_, ok := DefaultByEmail(project, email)
	return ok
}

// ServiceAccountExists reports whether identifier names a live service account
// in project. The Google-managed defaults count as live without a store
// record; a malformed or unknown account is (false, nil) and a store failure is
// returned.
func (e Exister) ServiceAccountExists(ctx context.Context, project, identifier string) (bool, error) {
	if ImplicitDefault(project, identifier) {
		return true, nil
	}
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
