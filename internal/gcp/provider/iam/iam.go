// Package iam implements the Google Cloud IAM provider (service accounts).
package iam

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/paging"
	"jaiscloud/internal/gcp/policy"
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
}

// resourceName returns the "name" path param, or a 400 when absent.
func resourceName(nr *model.NormalizedRequest) (string, error) {
	n, ok := nr.Params["name"].(string)
	if !ok || n == "" {
		return "", model.NewProviderError("InvalidRequest", "missing resource name", 400)
	}
	return n, nil
}

// emailFromName extracts the service-account email from a full or relative
// resource name (last path segment).
func emailFromName(name string) string {
	if i := strings.LastIndex(name, "/"); i >= 0 {
		return name[i+1:]
	}
	return name
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
	displayName := accountID
	description := ""
	disabled := false
	oauthClientID := ""
	if body, ok := nr.Params["body"].(map[string]any); ok {
		if sa, ok := body["serviceAccount"].(map[string]any); ok {
			if dn, _ := sa["displayName"].(string); dn != "" {
				displayName = dn
			}
			description, _ = sa["description"].(string)
			disabled, _ = sa["disabled"].(bool)
			oauthClientID, _ = sa["oauth2ClientId"].(string)
		}
	}
	email := accountID + "@" + nr.AccountID + ".iam.gserviceaccount.com"
	m := serviceAccountMeta{
		Name:           nr.ResourceID("service-account", email),
		Email:          email,
		DisplayName:    displayName,
		ProjectID:      nr.AccountID,
		Description:    description,
		Disabled:       disabled,
		Oauth2ClientID: oauthClientID,
		Etag:           policy.Etag(displayName),
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
	page, nextToken := paging.Apply(entries, nr.Params)
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
	email := emailFromName(name)
	e, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtServiceAccount, email)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "service account not found", 404)
		}
		return nil, err
	}
	var m serviceAccountMeta
	json.Unmarshal(e.Data, &m)
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
	email := emailFromName(name)
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

	updated, err := p.resources.UpsertAtomic(ctx, nr.AccountID, store.GlobalRegion, rtServiceAccount, email,
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
			return nil, model.NewProviderError("NotFound", "service account not found", 404)
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
	email := emailFromName(name)
	e, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtServiceAccount, email)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "service account not found", 404)
		}
		return nil, err
	}
	// Persist a tombstone so projects.serviceAccounts.undelete can restore the
	// account (real GCP keeps a deleted service account for 30 days).
	var m serviceAccountMeta
	if json.Unmarshal(e.Data, &m) == nil {
		tomb, _ := json.Marshal(deletedServiceAccount{
			Account:     nr.AccountID,
			Email:       email,
			DeletedAt:   clock.Now().UTC().Format(time.RFC3339Nano),
			ServiceAcct: m,
		})
		_ = p.resources.Upsert(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{
			Type: rtServiceAccountDeleted, ID: email, Data: tomb,
		})
	}
	if err := p.resources.Delete(ctx, nr.AccountID, store.GlobalRegion, rtServiceAccount, email); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "service account not found", 404)
		}
		return nil, err
	}
	return &model.ProviderResponse{HTTPStatus: 200, Data: map[string]any{}}, nil
}

// loadServiceAccountMeta loads a service account's stored metadata.
func (p *Provider) loadServiceAccountMeta(ctx context.Context, account, email string) (serviceAccountMeta, error) {
	e, err := p.resources.Get(ctx, account, store.GlobalRegion, rtServiceAccount, email)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return serviceAccountMeta{}, model.NewProviderError("NotFound", "service account not found", 404)
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
// .disable/.enable).
func (p *Provider) setDisabled(ctx context.Context, nr *model.NormalizedRequest, disabled bool) (*model.ProviderResponse, error) {
	name, err := resourceName(nr)
	if err != nil {
		return nil, err
	}
	email := emailFromName(name)
	m, err := p.loadServiceAccountMeta(ctx, nr.AccountID, email)
	if err != nil {
		return nil, err
	}
	m.Disabled = disabled
	if err := p.saveServiceAccountMeta(ctx, nr.AccountID, m); err != nil {
		return nil, err
	}
	return provider.OK(saToMap(m)), nil
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
	email := emailFromName(name)
	if m, err := p.loadServiceAccountMeta(ctx, nr.AccountID, email); err == nil {
		return provider.OK(saToMap(m)), nil
	}
	e, err := p.resources.Get(ctx, nr.AccountID, store.GlobalRegion, rtServiceAccountDeleted, email)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, model.NewProviderError("NotFound", "service account not found", 404)
		}
		return nil, err
	}
	var tomb deletedServiceAccount
	if err := json.Unmarshal(e.Data, &tomb); err != nil {
		return nil, err
	}
	m := tomb.ServiceAcct
	m.Disabled = false
	if err := p.resources.Upsert(ctx, nr.AccountID, store.GlobalRegion, store.ResourceEntry{
		Type: rtServiceAccount, ID: m.Email, Data: mustJSON(m),
	}); err != nil {
		return nil, err
	}
	_ = p.resources.Delete(ctx, nr.AccountID, store.GlobalRegion, rtServiceAccountDeleted, email)
	return provider.OK(saToMap(m)), nil
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
	email := emailFromName(name)
	if err := p.requireServiceAccount(ctx, nr.AccountID, email); err != nil {
		return nil, err
	}
	pol := policy.Load(ctx, p.resources, nr.AccountID, rtServiceAccountPolicy, email)
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
	email := emailFromName(name)
	if err := p.requireServiceAccount(ctx, nr.AccountID, email); err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	pol, err := policy.Set(ctx, p.resources, nr.AccountID, rtServiceAccountPolicy, email, body)
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
	email := emailFromName(name)
	if err := p.requireServiceAccount(ctx, nr.AccountID, email); err != nil {
		return nil, err
	}
	body, _ := nr.Params["body"].(map[string]any)
	return provider.OK(map[string]any{"permissions": policy.TestPermissions(policy.Permissions(body))}), nil
}

func (p *Provider) requireServiceAccount(ctx context.Context, account, email string) error {
	if _, err := p.resources.Get(ctx, account, store.GlobalRegion, rtServiceAccount, email); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return model.NewProviderError("NotFound", "service account not found", 404)
		}
		return err
	}
	return nil
}

func saToMap(m serviceAccountMeta) map[string]any {
	return map[string]any{
		"name":           m.Name,
		"email":          m.Email,
		"displayName":    m.DisplayName,
		"projectId":      m.ProjectID,
		"uniqueId":       m.Email,
		"description":    m.Description,
		"disabled":       m.Disabled,
		"oauth2ClientId": m.Oauth2ClientID,
		"etag":           m.Etag,
	}
}
