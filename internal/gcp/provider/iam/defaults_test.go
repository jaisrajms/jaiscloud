package iam

import (
	"context"
	"errors"
	"testing"

	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/gcp/serviceaccount"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// stubProjectNumbers is a ProjectNumberResolver backed by a fixed map.
type stubProjectNumbers map[string]string

func (s stubProjectNumbers) ProjectByNumber(_ context.Context, number string) (string, bool, error) {
	project, ok := s[number]
	return project, ok, nil
}

// errProjectNumbers fails every resolution.
type errProjectNumbers struct{}

func (errProjectNumbers) ProjectByNumber(context.Context, string) (string, bool, error) {
	return "", false, errors.New("registry unavailable")
}

// managedDefaults returns the project's two synthesized defaults.
func managedDefaults(project string) (appspot, compute serviceaccount.DefaultAccount) {
	d := serviceaccount.Defaults(project)
	return d[0], d[1]
}

// TestServiceAccountGetRendersManagedDefaults covers the read path TF2-1-1
// unblocked: the Google-managed defaults a project has without a create are
// readable through the same surface as a created account, with the captured
// real-GCP shape. The Google APIs service agent is not one of them (a live
// project answers 404 for it), so it stays a miss.
func TestServiceAccountGetRendersManagedDefaults(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())
	appspot, compute := managedDefaults("proj")

	for _, want := range []serviceaccount.DefaultAccount{appspot, compute} {
		resp, err := p.Get(ctx, newNR(map[string]any{"name": "serviceAccounts/" + want.Email}))
		if err != nil {
			t.Fatalf("get %s: %v", want.Email, err)
		}
		if got := resp.Data["email"]; got != want.Email {
			t.Errorf("email = %v, want %s", got, want.Email)
		}
		if got := resp.Data["displayName"]; got != want.DisplayName {
			t.Errorf("displayName = %v, want %q", got, want.DisplayName)
		}
		if got := resp.Data["name"]; got != "projects/proj/serviceAccounts/"+want.Email {
			t.Errorf("name = %v, want the concrete project name", got)
		}
		if got := resp.Data["projectId"]; got != "proj" {
			t.Errorf("projectId = %v, want proj", got)
		}
		if got := resp.Data["uniqueId"]; got != want.UniqueID {
			t.Errorf("uniqueId = %v, want the synthesized %q", got, want.UniqueID)
		}
		if got := resp.Data["oauth2ClientId"]; got != want.UniqueID {
			t.Errorf("oauth2ClientId = %v, want the same value as uniqueId", got)
		}
		if got := resp.Data["etag"]; got != serviceaccount.DefaultAccountEtag {
			t.Errorf("etag = %v, want the captured %q", got, serviceaccount.DefaultAccountEtag)
		}
		if got := resp.Data["disabled"]; got != false {
			t.Errorf("disabled = %v, want false", got)
		}
	}

	// A second read is stable: no generated value churns (Terraform would see
	// drift).
	first, err := p.Get(ctx, newNR(map[string]any{"name": "serviceAccounts/" + compute.Email}))
	if err != nil {
		t.Fatalf("get compute: %v", err)
	}
	again, err := p.Get(ctx, newNR(map[string]any{"name": "serviceAccounts/" + compute.Email}))
	if err != nil {
		t.Fatalf("get compute (again): %v", err)
	}
	for _, field := range []string{"uniqueId", "oauth2ClientId", "etag"} {
		if first.Data[field] != again.Data[field] {
			t.Errorf("%s churned between reads: %v then %v", field, first.Data[field], again.Data[field])
		}
	}

	// The Google APIs service agent is not a default: real IAM answers 404.
	agent := resource.ProjectNumber("proj") + "@cloudservices.gserviceaccount.com"
	if _, err := p.Get(ctx, newNR(map[string]any{"name": "serviceAccounts/" + agent})); errStatus(err) != 404 {
		t.Errorf("get %s = %v, want 404", agent, err)
	}
}

// TestServiceAccountGetManagedDefaultThroughWildcard covers the form the
// hashicorp/google data source sends: projects/-/serviceAccounts/{email}. The
// App Engine default embeds its project id; the Compute Engine default embeds
// only the project number, which the wired project registry resolves.
func TestServiceAccountGetManagedDefaultThroughWildcard(t *testing.T) {
	ctx := context.Background()
	number := resource.ProjectNumber("proj")
	p := New(store.NewMemoryResourceStore())
	p.SetProjectNumberResolver(stubProjectNumbers{number: "proj"})
	appspot, compute := managedDefaults("proj")

	for _, email := range []string{appspot.Email, compute.Email} {
		nr := wildNR(map[string]any{"name": "serviceAccounts/" + email})
		resp, err := p.Get(ctx, nr)
		if err != nil {
			t.Fatalf("wildcard get %s: %v", email, err)
		}
		if got, want := resp.Data["name"], "projects/proj/serviceAccounts/"+email; got != want {
			t.Errorf("wildcard get %s name = %v, want %s (the response must not carry projects/-)", email, got, want)
		}
		if got := resp.Data["email"]; got != email {
			t.Errorf("wildcard get email = %v, want %s", got, email)
		}
	}

	// An unknown number resolves no project and keeps the documented wildcard
	// miss (403 PERMISSION_DENIED, not 404).
	unknown := "999999999999-compute@developer.gserviceaccount.com"
	_, err := p.Get(ctx, wildNR(map[string]any{"name": "serviceAccounts/" + unknown}))
	pe, status := wildcardErrStatus(t, err)
	if status != 403 || pe.Code != "PermissionDenied" {
		t.Errorf("unknown number through projects/- = %v (status %d), want 403 PermissionDenied", pe, status)
	}

	// A registry failure is not a lookup miss and must surface.
	p.SetProjectNumberResolver(errProjectNumbers{})
	_, err = p.Get(ctx, wildNR(map[string]any{"name": "serviceAccounts/" + compute.Email}))
	if err == nil || errStatus(err) == 404 || errStatus(err) == 403 {
		t.Errorf("registry failure = %v, want a surfaced error", err)
	}

	// Without a wired registry the number cannot be scoped: the wildcard stays
	// a miss rather than inventing a project.
	p.SetProjectNumberResolver(nil)
	_, err = p.Get(ctx, wildNR(map[string]any{"name": "serviceAccounts/" + compute.Email}))
	if pe, status := wildcardErrStatus(t, err); status != 403 || pe.Code != "PermissionDenied" {
		t.Errorf("unresolvable number through projects/- = %v (status %d), want 403 PermissionDenied", pe, status)
	}
}

// TestServiceAccountGetManagedDefaultByUniqueID covers the second documented
// identifier form on the concrete project path: the synthesized uniqueId names
// the default account.
func TestServiceAccountGetManagedDefaultByUniqueID(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())
	_, compute := managedDefaults("proj")

	resp, err := p.Get(ctx, newNR(map[string]any{"name": "serviceAccounts/" + compute.UniqueID}))
	if err != nil {
		t.Fatalf("get by unique id: %v", err)
	}
	if got := resp.Data["email"]; got != compute.Email {
		t.Errorf("email = %v, want %s", got, compute.Email)
	}
	if got := resp.Data["name"]; got != "projects/proj/serviceAccounts/"+compute.Email {
		t.Errorf("name = %v, want the canonical name", got)
	}

	// The wildcard cannot scope a unique id (no global index), consistent with
	// the created-account behaviour.
	_, err = p.Get(ctx, wildNR(map[string]any{"name": "serviceAccounts/" + compute.UniqueID}))
	if pe, status := wildcardErrStatus(t, err); status != 403 || pe.Code != "PermissionDenied" {
		t.Errorf("unique id through projects/- = %v (status %d), want 403 PermissionDenied", pe, status)
	}
}

// TestServiceAccountListIncludesManagedDefaults covers the list half: the
// defaults are listed alongside created accounts, pagination sees them, and a
// store record for the same email is not listed twice.
func TestServiceAccountListIncludesManagedDefaults(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())
	if _, err := p.Create(ctx, newNR(map[string]any{"body": map[string]any{"accountId": "svc"}})); err != nil {
		t.Fatalf("create: %v", err)
	}
	appspot, compute := managedDefaults("proj")

	resp, err := p.List(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	accounts, _ := resp.Data["accounts"].([]any)
	byEmail := map[string]map[string]any{}
	for _, a := range accounts {
		m, _ := a.(map[string]any)
		email, _ := m["email"].(string)
		byEmail[email] = m
	}
	for _, want := range []serviceaccount.DefaultAccount{appspot, compute} {
		m := byEmail[want.Email]
		if m == nil {
			t.Fatalf("list did not include the default %s (got %v)", want.Email, byEmail)
		}
		if m["uniqueId"] != want.UniqueID || m["oauth2ClientId"] != want.UniqueID {
			t.Errorf("listed %s identifiers = %v, want the synthesized id", want.Email, m)
		}
	}
	if byEmail["svc@proj.iam.gserviceaccount.com"] == nil {
		t.Errorf("list dropped the created account (got %v)", byEmail)
	}

	// Pagination sees the defaults: a pageSize=1 walk visits every account once
	// (created + the two defaults), so appending them before paging did not
	// break the cursor.
	seen := map[string]int{}
	token := ""
	for i := 0; i < 10; i++ {
		params := map[string]any{"pageSize": "1"}
		if token != "" {
			params["pageToken"] = token
		}
		page, err := p.List(ctx, newNR(params))
		if err != nil {
			t.Fatalf("paged list (page %d): %v", i, err)
		}
		accounts, _ := page.Data["accounts"].([]any)
		if len(accounts) != 1 {
			t.Fatalf("page %d returned %d accounts, want 1", i, len(accounts))
		}
		m, _ := accounts[0].(map[string]any)
		email, _ := m["email"].(string)
		seen[email]++
		token, _ = page.Data["nextPageToken"].(string)
		if token == "" {
			break
		}
	}
	if len(seen) != 3 {
		t.Errorf("paged walk saw %d accounts (%v), want 3", len(seen), seen)
	}
	for email, n := range seen {
		if n != 1 {
			t.Errorf("paged walk saw %q %d times, want once", email, n)
		}
	}

	// A materialized record for a default email wins and is listed once.
	if err := p.resources.Create(ctx, "proj", store.GlobalRegion, store.ResourceEntry{
		Type: rtServiceAccount, ID: appspot.Email,
		Data: mustJSON(serviceAccountMeta{Email: appspot.Email, DisplayName: "materialized", ProjectID: "proj", UniqueID: appspot.UniqueID}),
	}); err != nil {
		t.Fatalf("seed materialized default: %v", err)
	}
	resp, err = p.List(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("list again: %v", err)
	}
	accounts, _ = resp.Data["accounts"].([]any)
	count := 0
	for _, a := range accounts {
		if m, _ := a.(map[string]any); m["email"] == appspot.Email {
			count++
		}
	}
	if count != 1 {
		t.Errorf("materialized default listed %d times, want 1", count)
	}

	// The - wildcard has no default set: a list scoped there must not invent
	// accounts for the literal "-" project.
	accounts = nil
	lresp, err := p.List(ctx, wildNR(nil))
	if err != nil {
		t.Fatalf("wildcard list: %v", err)
	}
	accounts, _ = lresp.Data["accounts"].([]any)
	if len(accounts) != 0 {
		t.Errorf("projects/- list returned %d accounts, want 0", len(accounts))
	}
}

// TestServiceAccountDefaultMutationsStayLookupMiss pins the scope boundary: the
// defaults are served read-only, so a mutation (including a key operation)
// keeps the lookup miss it had before they were rendered — no account record and
// no key material is silently materialized.
func TestServiceAccountDefaultMutationsStayLookupMiss(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())
	_, compute := managedDefaults("proj")
	name := "serviceAccounts/" + compute.Email

	for _, tc := range []struct {
		name string
		call func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error)
	}{
		{"disable", p.Disable},
		{"enable", p.Enable},
		{"patch", p.Update},
		{"delete", p.Delete},
		{"undelete", p.Undelete},
		{"keys.create", p.ServiceAccountKeyCreate},
		{"keys.upload", p.ServiceAccountKeyUpload},
	} {
		if _, err := tc.call(ctx, newNR(map[string]any{
			"name": name,
			"body": map[string]any{
				"serviceAccount": map[string]any{"displayName": "x"},
				"publicKeyData":  "bm90LWEta2V5",
			},
		})); errStatus(err) != 404 {
			t.Errorf("%s on a managed default = %v, want 404", tc.name, err)
		}
	}
	// No account or key record was materialized by the failed mutations.
	if _, err := p.resources.Get(ctx, "proj", store.GlobalRegion, rtServiceAccount, compute.Email); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("a failed mutation materialized the default account: %v", err)
	}
	keys, err := p.resources.List(ctx, "proj", store.GlobalRegion, rtServiceAccountKey, "")
	if err != nil {
		t.Fatalf("list keys: %v", err)
	}
	if len(keys) != 0 {
		t.Errorf("a failed mutation wrote %d key records, want none", len(keys))
	}
}

// TestServiceAccountDefaultIamPolicy covers the policy surface on a managed
// default: real IAM serves the account's IAM policy, and the policy store is
// keyed by email, so no account record is needed. The managed key surface stays
// a miss (tested above).
func TestServiceAccountDefaultIamPolicy(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())
	_, compute := managedDefaults("proj")
	name := "serviceAccounts/" + compute.Email

	pol, err := p.GetIamPolicy(ctx, newNR(map[string]any{"name": name}))
	if err != nil {
		t.Fatalf("getIamPolicy: %v", err)
	}
	if pol.Data["etag"] == "" || pol.Data["etag"] == nil {
		t.Errorf("default policy carries no etag: %v", pol.Data)
	}

	binding := map[string]any{"role": "roles/iam.serviceAccountUser", "members": []any{"serviceAccount:user@proj.iam.gserviceaccount.com"}}
	set, err := p.SetIamPolicy(ctx, newNR(map[string]any{
		"name": name,
		"body": map[string]any{"policy": map[string]any{"bindings": []any{binding}}},
	}))
	if err != nil {
		t.Fatalf("setIamPolicy: %v", err)
	}
	got, _ := set.Data["bindings"].([]any)
	if len(got) != 1 {
		t.Fatalf("bindings = %v, want the granted binding", set.Data["bindings"])
	}
	// The read-back converges (Terraform would plan no changes).
	again, err := p.GetIamPolicy(ctx, newNR(map[string]any{"name": name}))
	if err != nil {
		t.Fatalf("getIamPolicy after set: %v", err)
	}
	if len(again.Data["bindings"].([]any)) != 1 || again.Data["etag"] != set.Data["etag"] {
		t.Errorf("policy did not converge: %v then %v", set.Data, again.Data)
	}
}
