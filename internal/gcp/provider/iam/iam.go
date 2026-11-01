// Package iam implements the Google Cloud IAM provider (service accounts).
package iam

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/gcp/policy"
	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/gcp/serviceaccount"
	"jaiscloud/internal/model"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

const (
	rtServiceAccount        = "gcp_service_account"
	rtServiceAccountPolicy  = "gcp_service_account_policy"
	rtServiceAccountDeleted = "gcp_service_account_deleted"
)

// deletedServiceAccount is the tombstone persisted by Delete so a later
// projects.serviceAccounts.undelete can restore the account. Real GCP keeps a
// deleted service account for 30 days; the emulator keeps it until cleared by a
// restore (there is no background GC of tombstones).
type deletedServiceAccount struct {
	Account     string             `json:"account"`
	Email       string             `json:"email"`
	DeletedAt   string             `json:"deletedAt"`
	ServiceAcct serviceAccountMeta `json:"serviceAccount"`
}

// Provider handles IAM service accounts and their IAM policies.
type Provider struct {
	resources store.ResourceStore
}

func New(resources store.ResourceStore) *Provider {
	return &Provider{resources: resources}
}

func (p *Provider) Routes() map[string]provider.HandlerFunc {
	return map[string]provider.HandlerFunc{
		"IAM.ServiceAccountCreate":             p.Create,
		"IAM.ServiceAccountList":               p.List,
		"IAM.ServiceAccountGet":                p.Get,
		"IAM.ServiceAccountPatch":              p.Update,
		"IAM.ServiceAccountUpdate":             p.Update,
		"IAM.ServiceAccountDelete":             p.Delete,
		"IAM.ServiceAccountDisable":            p.Disable,
		"IAM.ServiceAccountEnable":             p.Enable,
		"IAM.ServiceAccountUndelete":           p.Undelete,
		"IAM.ServiceAccountGetIamPolicy":       p.GetIamPolicy,
		"IAM.ServiceAccountSetIamPolicy":       p.SetIamPolicy,
		"IAM.ServiceAccountTestIamPermissions": p.TestIamPermissions,
		"IAM.ServiceAccountKeyCreate":          p.ServiceAccountKeyCreate,
		"IAM.ServiceAccountKeyUpload":          p.ServiceAccountKeyUpload,
		"IAM.ServiceAccountKeyList":            p.ServiceAccountKeyList,
		"IAM.ServiceAccountKeyGet":             p.ServiceAccountKeyGet,
		"IAM.ServiceAccountKeyDelete":          p.ServiceAccountKeyDelete,
		"IAM.ServiceAccountKeyDisable":         p.ServiceAccountKeyDisable,
		"IAM.ServiceAccountKeyEnable":          p.ServiceAccountKeyEnable,
		"IAM.ServiceAccountSignBlob":           p.ServiceAccountSignBlob,
		"IAM.ServiceAccountSignJwt":            p.ServiceAccountSignJwt,
	}
}

type serviceAccountMeta struct {
	Name           string `json:"name"`
	Email          string `json:"email"`
	DisplayName    string `json:"displayName"`
	ProjectID      string `json:"projectId"`
	Description    string `json:"description"`
	Disabled       bool   `json:"disabled"`
	Oauth2ClientID string `json:"oauth2ClientId"`
	Etag           string `json:"etag"`
	// UniqueID is the account's numeric unique id, the second identifier real
	// IAM accepts in a serviceAccounts resource name. Records written before
	// the field was modelled leave it empty and keep reporting the email there
	// (see saToMap); new accounts carry a generated 21-digit id.
	UniqueID string `json:"uniqueId,omitempty"`
}

// resourceName returns the "name" path param, or a 400 when absent.
func resourceName(nr *model.NormalizedRequest) (string, error) {
	n, ok := nr.Params["name"].(string)
	if !ok || n == "" {
		return "", model.NewProviderError("InvalidRequest", "missing resource name", 400)
	}
	return n, nil
}

// validAccountID enforces the service-account accountId pattern real GCP
// applies, [a-z]([-a-z0-9]*[a-z0-9])?: the id begins with a lowercase letter,
// contains only lowercase letters, digits and hyphens, and (when longer than
// one character) ends in a letter or digit. The 6-30 length rule is not
// enforced here because the emulator's own fixtures use shorter ids.
func validAccountID(id string) bool {
	if id == "" || id[0] < 'a' || id[0] > 'z' {
		return false
	}
	if len(id) > 1 {
		last := id[len(id)-1]
		if !((last >= 'a' && last <= 'z') || (last >= '0' && last <= '9')) {
			return false
		}
	}
	for i := 1; i < len(id)-1; i++ {
		c := id[i]
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' {
			continue
		}
		return false
	}
	return true
}

// emailFromName extracts the service-account email from a full or relative
// resource name (last path segment).
func emailFromName(name string) string {
	if i := strings.LastIndex(name, "/"); i >= 0 {
		return name[i+1:]
	}
	return name
}

// saRef is a service account addressed by a method's resource name: the store
// account (project) that owns it and the canonical email used as its store key.
type saRef struct {
	Account string
	Email   string
}

// saLookupError is the failure every service-account method reports for a
// resource it could not find. Real IAM documents that a miss through the
// "projects/-" wildcard answers 403 PERMISSION_DENIED instead of 404
// NOT_FOUND, and `gcloud iam service-accounts …` addresses every command with
// that wildcard (command_lib/iam/iam_util.py EmailToAccountResourceName), so
// the distinction is client-visible.
//
// The message names the wildcard resource but no permission: the emulator never
// evaluates IAM permissions, so claiming a specific permission was denied (the
// shape policy.PermissionDenied produces for the services that do check them)
// would be a fiction.
func saLookupError(nr *model.NormalizedRequest, name, notFoundMessage string) error {
	if nr.AccountID == "-" {
		return &model.ProviderError{
			Code:       "PermissionDenied",
			Message:    "Permission denied on resource 'projects/-/" + name + "' (or it may not exist).",
			HTTPStatus: 403,
			Status:     "PERMISSION_DENIED",
		}
	}
	return model.NewProviderError("NotFound", notFoundMessage, 404)
}

// resolveSA maps the service-account identifier in a resource name (the segment
// after "serviceAccounts/") to the store account that owns it and its canonical
// email. Real IAM accepts two identifier forms — the email
// ("{id}@{project}.iam.gserviceaccount.com") and the account's unique id — and
// accepts "projects/-" in place of the project on every service-account
// resource method.
//
// The project embedded in the email is authoritative for the wildcard
// (serviceaccount.AccountForSA, the same resolver signBlob/signJwt and IAM
// Credentials use); a unique id carries no project of its own, so it is
// resolved by scanning the addressed account. types selects which records are
// scanned (the live accounts by default, or the tombstones for undelete).
func (p *Provider) resolveSA(ctx context.Context, nr *model.NormalizedRequest, identifier string, types ...string) (saRef, error) {
	notFound := func() error {
		return saLookupError(nr, "serviceAccounts/"+identifier, "service account not found")
	}
	if identifier == "" {
		// An empty name segment must not match a record whose uniqueId is
		// unset (every account written before the field existed).
		return saRef{}, notFound()
	}
	if strings.Contains(identifier, "@") {
		account := serviceaccount.AccountForSA(nr.AccountID, identifier)
		if account == "" || account == "-" {
			// The path project is the wildcard and the identifier embeds no
			// project of its own, so the account cannot be scoped.
			return saRef{}, notFound()
		}
		return saRef{Account: account, Email: identifier}, nil
	}
	if nr.AccountID == "" || nr.AccountID == "-" {
		// A unique id under the wildcard would need a global account index; the
		// emulator's ResourceStore is per-account, so it cannot be resolved.
		return saRef{}, notFound()
	}
	if len(types) == 0 {
		types = []string{rtServiceAccount}
	}
	for _, rt := range types {
		entries, err := p.resources.List(ctx, nr.AccountID, store.GlobalRegion, rt, "")
		if err != nil {
			return saRef{}, err
		}
		for _, e := range entries {
			var m serviceAccountMeta
			if rt == rtServiceAccountDeleted {
				// Tombstones wrap the account so undelete can restore it.
				var tomb deletedServiceAccount
				if json.Unmarshal(e.Data, &tomb) != nil {
					continue
				}
				m = tomb.ServiceAcct
			} else if json.Unmarshal(e.Data, &m) != nil {
				continue
			}
			if m.UniqueID == identifier || m.Email == identifier {
				return saRef{Account: nr.AccountID, Email: m.Email}, nil
			}
		}
	}
	return saRef{}, notFound()
}

// saNameFormat returns the resource-name formatter for a resolved account: the
// request's injected formatter when it already addresses that account, and the
// shared GCP formatter when a wildcard request resolved its project from the
// service-account email. Response names must never carry "projects/-".
func saNameFormat(nr *model.NormalizedRequest, account string) func(resourceType, name string) string {
	if nr.ResourceID != nil && account == nr.AccountID {
		return nr.ResourceID
	}
	return resource.ResourceID(account)
}

// newNumericID returns a 21-digit decimal identifier in the shape real IAM
// assigns to a service account's uniqueId and oauth2ClientId. The two are
// distinct values on real GCP (the Discovery sample pairs
// 123456789012345678901 with 987654321098765432109), so Create draws each
// separately; the leading digit is non-zero to keep the value 21 characters
// long.
//
// The id is random rather than derived from the account because IAM documents
// that a deleted service account keeps its id while an account re-created under
// the same name gets a different one, so a stable hash of the email would
// violate that contract.
func newNumericID() string {
	const digits = 21
	buf := make([]byte, digits)
	_, _ = rand.Read(buf)
	out := make([]byte, digits)
	out[0] = '1' + buf[0]%9
	for i := 1; i < digits; i++ {
		out[i] = '0' + buf[i]%10
	}
	return string(out)
}

func (p *Provider) Create(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	accountID, _ := nr.Params["accountId"].(string)
	if accountID == "" {
		if body, ok := nr.Params["body"].(map[string]any); ok {
			accountID, _ = body["accountId"].(string)
		}
	}
	if accountID == "" {
		return nil, model.NewProviderError("InvalidRequest", "missing accountId", 400)
	}
	if !validAccountID(accountID) {
		return nil, model.NewProviderError("InvalidRequest",
			accountID+" does not match [a-z]([-a-z0-9]*[a-z0-9])?", 400)
	}
	displayName := accountID
	description := ""
	if body, ok := nr.Params["body"].(map[string]any); ok {
		if sa, ok := body["serviceAccount"].(map[string]any); ok {
			if dn, _ := sa["displayName"].(string); dn != "" {
				displayName = dn
			}
			description, _ = sa["description"].(string)
		}
	}
	email := accountID + "@" + nr.AccountID + ".iam.gserviceaccount.com"
	// uniqueId, oauth2ClientId and disabled are output-only in the Discovery
	// schema: real IAM ignores them on input and answers the server-generated
	// values (a disabled service account is created enabled, then disabled
	// through projects.serviceAccounts.disable).
	m := serviceAccountMeta{
		Name:           nr.ResourceID("service-account", email),
		Email:          email,
		DisplayName:    displayName,
		ProjectID:      nr.AccountID,
		Description:    description,
		Disabled:       false,
		Oauth2ClientID: newNumericID(),
		Etag:           policy.Etag(displayName),
		UniqueID:       newNumericID(),
	}
	data, _ := json.Marshal(m)
	if err := p.resources.Create(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{Type: rtServiceAccount, ID: email, Data: data}); err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			return nil, model.NewProviderError("AlreadyExists", "service account already exists", 409)
		}
		return nil, err
	}
	return provider.OK(saToMap(m)), nil
}

func (p *Provider) List(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	entries, err := p.resources.List(ctx, nr.AccountID, store.GlobalRegion, rtServiceAccount, "")
	if err != nil {
		return nil, err
	}
	page, nextToken, err := paging.Apply(entries, nr.Params)
	if err != nil {
		return nil, err
	}
	items := make([]any, 0, len(page))
	for _, e := range page {
		var m serviceAccountMeta
		if json.Unmarshal(e.Data, &m) == nil {
			items = append(items, saToMap(m))
		}
	}
	resp := map[string]any{"accounts": items}
	if nextToken != "" {
		resp["nextPageToken"] = nextToken
	}
	return provider.OK(resp), nil
}

func (p *Provider) Get(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	ref, err := p.resolveSA(ctx, nr, emailFromName(name))
	if err != nil {
		return nil, err
	}
	m, err := p.loadServiceAccountMeta(ctx, nr, ref)
	if err != nil {
		return nil, err
	}
	return provider.OK(saToMap(m)), nil
}

// Update applies a serviceAccounts.patch (or update) request. Real IAM permits
// patching only display_name and description, and requires update_mask to name
// the fields to overlay; an empty mask overlays every field present in the
// request. The read-modify-write runs inside UpsertAtomic so a concurrent
// update can't slip past the etag precondition.
func (p *Provider) Update(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	ref, err := p.resolveSA(ctx, nr, emailFromName(name))
	if err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	// PatchServiceAccountRequest wraps the account under "serviceAccount";
	// tolerate a bare ServiceAccount body for the PUT/update verb.
	sa, _ := body["serviceAccount"].(map[string]any)
	if sa == nil {
		sa = body
	}
	mask := strParam(nr, "updateMask")
	if mask == "" {
		mask, _ = body["updateMask"].(string)
	}

	updated, err := p.resources.UpsertAtomic(ctx, ref.Account, store.GlobalRegion, rtServiceAccount, ref.Email,
		func(current store.ResourceEntry, exists bool) (store.ResourceEntry, error) {
			if !exists {
				return store.ResourceEntry{}, store.ErrNotFound
			}
			var m serviceAccountMeta
			if err := json.Unmarshal(current.Data, &m); err != nil {
				return store.ResourceEntry{}, err
			}
			if err := applyServiceAccountUpdate(&m, sa, mask); err != nil {
				return store.ResourceEntry{}, err
			}
			data, err := json.Marshal(m)
			if err != nil {
				return store.ResourceEntry{}, err
			}
			current.Data = data
			return current, nil
		})
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, saLookupError(nr, "serviceAccounts/"+ref.Email, "service account not found")
		}
		return nil, err
	}
	var m serviceAccountMeta
	if err := json.Unmarshal(updated.Data, &m); err != nil {
		return nil, err
	}
	return provider.OK(saToMap(m)), nil
}

// serviceAccountUpdateFields are the only fields IAM permits a patch to change.
var serviceAccountUpdateFields = map[string]bool{"displayName": true, "description": true}

// normalizeSAField maps the proto snake_case spelling some clients send onto
// the Discovery camelCase field name.
func normalizeSAField(field string) string {
	if field == "display_name" {
		return "displayName"
	}
	return field
}

// applyServiceAccountUpdate overlays the masked fields onto the stored account.
// Unsupported mask paths fail loud (400) rather than silently no-op; a supplied
// etag is enforced for optimistic concurrency (ABORTED/409).
func applyServiceAccountUpdate(m *serviceAccountMeta, sa map[string]any, mask string) error {
	masked := map[string]bool{}
	hasMask := false
	for _, field := range splitMask(mask) {
		hasMask = true
		f := normalizeSAField(field)
		if !serviceAccountUpdateFields[f] {
			return model.NewProviderError("InvalidArgument", "unsupported updateMask field "+field, 400)
		}
		masked[f] = true
	}
	if etag, _ := sa["etag"].(string); etag != "" && etag != m.Etag {
		return model.NewProviderError("Aborted", "etag mismatch: optimistic concurrency control failed", 409)
	}
	apply := func(field string) bool { return !hasMask || masked[field] }
	if v, ok := sa["displayName"].(string); ok && apply("displayName") {
		m.DisplayName = v
	}
	if v, ok := sa["description"].(string); ok && apply("description") {
		m.Description = v
	}
	m.Etag = policy.Etag(m.DisplayName)
	return nil
}

// strParam returns a string param, tolerating a non-string value.
func strParam(nr *model.NormalizedRequest, key string) string {
	s, _ := nr.Params[key].(string)
	return s
}

// splitMask splits a comma-separated updateMask, trimming whitespace and
// dropping empty entries.
func splitMask(mask string) []string {
	var out []string
	for _, p := range strings.Split(mask, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func (p *Provider) Delete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	ref, err := p.resolveSA(ctx, nr, emailFromName(name))
	if err != nil {
		return nil, err
	}
	e, err := p.resources.Get(ctx, ref.Account, store.GlobalRegion, rtServiceAccount, ref.Email)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, saLookupError(nr, "serviceAccounts/"+ref.Email, "service account not found")
		}
		return nil, err
	}
	// Persist a tombstone so projects.serviceAccounts.undelete can restore the
	// account (real GCP keeps a deleted service account for 30 days).
	var m serviceAccountMeta
	if json.Unmarshal(e.Data, &m) == nil {
		tomb, _ := json.Marshal(deletedServiceAccount{
			Account:     ref.Account,
			Email:       ref.Email,
			DeletedAt:   clock.Now().UTC().Format(time.RFC3339Nano),
			ServiceAcct: m,
		})
		_ = p.resources.Upsert(ctx, ref.Account, store.GlobalRegion, store.ResourceEntry{
			Type: rtServiceAccountDeleted, ID: ref.Email, Data: tomb,
		})
	}
	if err := p.resources.Delete(ctx, ref.Account, store.GlobalRegion, rtServiceAccount, ref.Email); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, saLookupError(nr, "serviceAccounts/"+ref.Email, "service account not found")
		}
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

// loadServiceAccountMeta loads a service account's stored metadata.
func (p *Provider) loadServiceAccountMeta(ctx context.Context, nr *model.NormalizedRequest, ref saRef) (serviceAccountMeta, error) {
	e, err := p.resources.Get(ctx, ref.Account, store.GlobalRegion, rtServiceAccount, ref.Email)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return serviceAccountMeta{}, saLookupError(nr, "serviceAccounts/"+ref.Email, "service account not found")
		}
		return serviceAccountMeta{}, err
	}
	var m serviceAccountMeta
	if err := json.Unmarshal(e.Data, &m); err != nil {
		return serviceAccountMeta{}, err
	}
	return m, nil
}

// saveServiceAccountMeta persists an updated service account.
func (p *Provider) saveServiceAccountMeta(ctx context.Context, account string, m serviceAccountMeta) error {
	data, _ := json.Marshal(m)
	return p.resources.Update(ctx, account, store.GlobalRegion, store.ResourceEntry{
		Type: rtServiceAccount, ID: m.Email, Data: data,
	})
}

// setDisabled flips a service account's disabled flag (projects.serviceAccounts
// .disable/.enable). Both methods declare an Empty response, so the new state is
// observable only through a follow-up get — real GCP never returns the account
// here, and a client decoding strictly against Discovery must not receive one.
func (p *Provider) setDisabled(ctx context.Context, nr *model.NormalizedRequest, disabled bool) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	ref, err := p.resolveSA(ctx, nr, emailFromName(name))
	if err != nil {
		return nil, err
	}
	m, err := p.loadServiceAccountMeta(ctx, nr, ref)
	if err != nil {
		return nil, err
	}
	m.Disabled = disabled
	if err := p.saveServiceAccountMeta(ctx, ref.Account, m); err != nil {
		return nil, err
	}
	return provider.OK(map[string]any{}), nil
}

func (p *Provider) Disable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.setDisabled(ctx, nr, true)
}

func (p *Provider) Enable(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	return p.setDisabled(ctx, nr, false)
}

// Undelete restores a previously deleted service account from its tombstone
// (projects.serviceAccounts.undelete). A live account is returned unchanged.
func (p *Provider) Undelete(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	identifier := emailFromName(name)
	// Resolve against the live accounts first: undelete of a live account is a
	// no-op that returns it unchanged. A unique-id identifier may exist only as
	// a tombstone, so fall back to scanning those. Only a lookup miss falls
	// through — a store failure must not be masked as "not found".
	ref, rerr := p.resolveSA(ctx, nr, identifier)
	if rerr != nil {
		if _, ok := rerr.(*model.ProviderError); !ok {
			return nil, rerr
		}
		ref, rerr = p.resolveSA(ctx, nr, identifier, rtServiceAccountDeleted)
		if rerr != nil {
			return nil, rerr
		}
	} else if m, lerr := p.loadServiceAccountMeta(ctx, nr, ref); lerr == nil {
		return provider.OK(undeleteMap(m)), nil
	} else if _, ok := lerr.(*model.ProviderError); !ok {
		return nil, lerr
	}
	e, err := p.resources.Get(ctx, ref.Account, store.GlobalRegion, rtServiceAccountDeleted, ref.Email)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, saLookupError(nr, "serviceAccounts/"+ref.Email, "service account not found")
		}
		return nil, err
	}
	var tomb deletedServiceAccount
	if err := json.Unmarshal(e.Data, &tomb); err != nil {
		return nil, err
	}
	m := tomb.ServiceAcct
	m.Disabled = false
	if err := p.resources.Upsert(ctx, ref.Account, store.GlobalRegion, store.ResourceEntry{
		Type: rtServiceAccount, ID: m.Email, Data: mustJSON(m),
	}); err != nil {
		return nil, err
	}
	_ = p.resources.Delete(ctx, ref.Account, store.GlobalRegion, rtServiceAccountDeleted, ref.Email)
	return provider.OK(undeleteMap(m)), nil
}

// undeleteMap renders the projects.serviceAccounts.undelete response. The
// Discovery schema (UndeleteServiceAccountResponse) and the generated clients
// expect the restored account nested under restoredAccount, not returned bare.
func undeleteMap(m serviceAccountMeta) map[string]any {
	return map[string]any{"restoredAccount": saToMap(m)}
}

// mustJSON marshals a service account metadata value for the resource store.
func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func (p *Provider) GetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	ref, err := p.resolveSA(ctx, nr, emailFromName(name))
	if err != nil {
		return nil, err
	}
	if err := p.requireServiceAccount(ctx, nr, ref); err != nil {
		return nil, err
	}
	pol := policy.Load(ctx, p.resources, ref.Account, rtServiceAccountPolicy, ref.Email)
	// Honor options.requestedPolicyVersion: report at least the requested
	// version (IAM supports versions 1 and 3; the emulator stores bindings
	// without conditions, so 1 is authoritative but the version is surfaced).
	if rpv, ok := nr.Params["options.requestedPolicyVersion"].(string); ok {
		if v, err := strconv.Atoi(rpv); err == nil && v > pol.Version {
			pol.Version = v
		}
	}
	return provider.OK(policy.ToMap(pol)), nil
}

func (p *Provider) SetIamPolicy(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	ref, err := p.resolveSA(ctx, nr, emailFromName(name))
	if err != nil {
		return nil, err
	}
	if err := p.requireServiceAccount(ctx, nr, ref); err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	pol, err := policy.Set(ctx, p.resources, ref.Account, rtServiceAccountPolicy, ref.Email, body)
	if err != nil {
		return nil, err
	}
	return provider.OK(policy.ToMap(pol)), nil
}

func (p *Provider) TestIamPermissions(ctx context.Context, nr *model.NormalizedRequest) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	ref, err := p.resolveSA(ctx, nr, emailFromName(name))
	if err != nil {
		return nil, err
	}
	if err := p.requireServiceAccount(ctx, nr, ref); err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	return provider.OK(map[string]any{"permissions": policy.TestPermissions(policy.Permissions(body))}), nil
}

func (p *Provider) requireServiceAccount(ctx context.Context, nr *model.NormalizedRequest, ref saRef) error {
	if _, err := p.resources.Get(ctx, ref.Account, store.GlobalRegion, rtServiceAccount, ref.Email); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return saLookupError(nr, "serviceAccounts/"+ref.Email, "service account not found")
		}
		return err
	}
	return nil
}

func saToMap(m serviceAccountMeta) map[string]any {
	// Records written before the unique id was stored report the email there,
	// which is what the API returned for them; they also keep their empty
	// oauth2ClientId. Everything created since carries numeric ids.
	uniqueID := m.UniqueID
	if uniqueID == "" {
		uniqueID = m.Email
	}
	return map[string]any{
		"name":           m.Name,
		"email":          m.Email,
		"displayName":    m.DisplayName,
		"projectId":      m.ProjectID,
		"uniqueId":       uniqueID,
		"description":    m.Description,
		"disabled":       m.Disabled,
		"oauth2ClientId": m.Oauth2ClientID,
		"etag":           m.Etag,
	}
}
