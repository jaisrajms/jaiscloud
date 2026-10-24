package iam

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// wildNR builds a request addressed through the "projects/-" wildcard — the
// form gcloud sends for every `iam service-accounts` command
// (command_lib/iam/iam_util.py EmailToAccountResourceName). The gateway binds
// the injected name formatter to the request's project, so a wildcard request
// carries that literal project through ResourceID: exactly the state the
// provider must not leak into response names.
func wildNR(params map[string]any) *model.NormalizedRequest {
	return &model.NormalizedRequest{
		AccountID:  "-",
		Params:     params,
		ResourceID: resource.ResourceID("-"),
	}
}

// wildcardErrStatus returns the ProviderError's HTTP status (0 when the error is
// not a ProviderError).
func wildcardErrStatus(t *testing.T, err error) (*model.ProviderError, int) {
	t.Helper()
	pe, ok := err.(*model.ProviderError)
	if !ok {
		t.Fatalf("error %v is not a *model.ProviderError", err)
	}
	return pe, pe.HTTPStatus
}

// TestServiceAccountWildcardProject covers DC24: every service-account
// management method must address the account the same way the official clients
// do, i.e. through "projects/-" with the project embedded in the email.
func TestServiceAccountWildcardProject(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())
	if _, err := p.Create(ctx, newNR(map[string]any{"body": map[string]any{"accountId": "wild"}})); err != nil {
		t.Fatalf("create: %v", err)
	}
	email := "wild@proj.iam.gserviceaccount.com"
	canonical := "projects/proj/serviceAccounts/" + email
	wild := func(params map[string]any) *model.NormalizedRequest {
		params["name"] = "serviceAccounts/" + email
		return wildNR(params)
	}
	concrete := func(params map[string]any) *model.NormalizedRequest {
		params["name"] = "serviceAccounts/" + email
		return newNR(params)
	}

	// get: the wildcard resolves to the account's project and the response
	// carries the canonical name, never "projects/-".
	resp, err := p.Get(ctx, wild(map[string]any{}))
	if err != nil {
		t.Fatalf("wildcard get: %v", err)
	}
	if got := resp.Data["name"]; got != canonical {
		t.Errorf("wildcard get name = %v, want %v", got, canonical)
	}
	if got := resp.Data["projectId"]; got != "proj" {
		t.Errorf("wildcard get projectId = %v, want proj", got)
	}

	// patch.
	resp, err = p.Update(ctx, wild(map[string]any{
		"body": map[string]any{
			"serviceAccount": map[string]any{"displayName": "Wild"},
			"updateMask":     "displayName",
		},
	}))
	if err != nil {
		t.Fatalf("wildcard patch: %v", err)
	}
	if got := resp.Data["displayName"]; got != "Wild" {
		t.Errorf("wildcard patch displayName = %v, want Wild", got)
	}
	if resp, err = p.Get(ctx, concrete(map[string]any{})); err != nil {
		t.Fatalf("concrete get after wildcard patch: %v", err)
	}
	if got := resp.Data["displayName"]; got != "Wild" {
		t.Errorf("concrete get displayName = %v, want the wildcard patch to have applied", got)
	}

	// disable / enable. Both answer Empty (Discovery), so the state is read back.
	if resp, err = p.Disable(ctx, wild(map[string]any{})); err != nil {
		t.Fatalf("wildcard disable: %v", err)
	}
	assertEmptyResponse(t, "wildcard disable", resp)
	if resp, err = p.Get(ctx, wild(map[string]any{})); err != nil {
		t.Fatalf("wildcard get after disable: %v", err)
	}
	if got := resp.Data["disabled"]; got != true {
		t.Errorf("wildcard disable disabled = %v, want true", got)
	}
	if resp, err = p.Enable(ctx, wild(map[string]any{})); err != nil {
		t.Fatalf("wildcard enable: %v", err)
	}
	assertEmptyResponse(t, "wildcard enable", resp)
	if resp, err = p.Get(ctx, wild(map[string]any{})); err != nil {
		t.Fatalf("wildcard get after enable: %v", err)
	}
	if got := resp.Data["disabled"]; got != false {
		t.Errorf("wildcard enable disabled = %v, want false", got)
	}

	// getIamPolicy / setIamPolicy / testIamPermissions.
	if _, err = p.GetIamPolicy(ctx, wild(map[string]any{})); err != nil {
		t.Fatalf("wildcard getIamPolicy: %v", err)
	}
	bindings := []any{map[string]any{"role": "roles/iam.serviceAccountUser", "members": []any{"allUsers"}}}
	set, err := p.SetIamPolicy(ctx, wild(map[string]any{"body": map[string]any{"bindings": bindings}}))
	if err != nil {
		t.Fatalf("wildcard setIamPolicy: %v", err)
	}
	if etag, _ := set.Data["etag"].(string); etag == "" {
		t.Error("wildcard setIamPolicy returned no etag")
	}
	// The policy landed on the concrete account, not on a "projects/-" record.
	pol, err := p.GetIamPolicy(ctx, concrete(map[string]any{}))
	if err != nil {
		t.Fatalf("concrete getIamPolicy after wildcard set: %v", err)
	}
	if got, _ := pol.Data["bindings"].([]any); len(got) != 1 {
		t.Errorf("concrete getIamPolicy bindings = %v, want the wildcard set to have applied", got)
	}
	perms, err := p.TestIamPermissions(ctx, wild(map[string]any{
		"body": map[string]any{"permissions": []any{"iam.serviceAccounts.get"}},
	}))
	if err != nil {
		t.Fatalf("wildcard testIamPermissions: %v", err)
	}
	if got, _ := perms.Data["permissions"].([]string); len(got) != 1 {
		t.Errorf("wildcard testIamPermissions permissions = %v, want 1", perms.Data["permissions"])
	}

	// delete (writes the tombstone under the resolved account) then undelete.
	if _, err = p.Delete(ctx, wild(map[string]any{})); err != nil {
		t.Fatalf("wildcard delete: %v", err)
	}
	if _, err = p.Get(ctx, concrete(map[string]any{})); err == nil || errStatus(err) != 404 {
		t.Fatalf("concrete get after wildcard delete = %v, want 404", err)
	}
	resp, err = p.Undelete(ctx, wild(map[string]any{}))
	if err != nil {
		t.Fatalf("wildcard undelete: %v", err)
	}
	restored, _ := resp.Data["restoredAccount"].(map[string]any)
	if restored["email"] != email || restored["name"] != canonical {
		t.Errorf("wildcard undelete restoredAccount = %v, want email %q name %q", restored, email, canonical)
	}
	if _, err = p.Get(ctx, concrete(map[string]any{})); err != nil {
		t.Fatalf("concrete get after wildcard undelete: %v", err)
	}
}

// TestServiceAccountKeysWildcardProject covers DC24 for the keys surface: the
// key methods inherit the wildcard resolution and render canonical key names.
func TestServiceAccountKeysWildcardProject(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())
	if _, err := p.Create(ctx, newNR(map[string]any{"body": map[string]any{"accountId": "wildkey"}})); err != nil {
		t.Fatalf("create: %v", err)
	}
	email := "wildkey@proj.iam.gserviceaccount.com"
	parent := "serviceAccounts/" + email
	canonicalParent := "projects/proj/serviceAccounts/" + email
	canonical := canonicalParent + "/keys/"

	created, err := p.ServiceAccountKeyCreate(ctx, wildNR(map[string]any{"name": parent + "/keys"}))
	if err != nil {
		t.Fatalf("wildcard keys.create: %v", err)
	}
	name, _ := created.Data["name"].(string)
	if !strings.HasPrefix(name, canonical) {
		t.Fatalf("wildcard keys.create name = %q, want the %q prefix", name, canonical)
	}
	keyID := name[strings.LastIndex(name, "/")+1:]

	// The credentials file carries the resolved project, not "-".
	rawCreds, err := base64.StdEncoding.DecodeString(created.Data["privateKeyData"].(string))
	if err != nil {
		t.Fatalf("decode privateKeyData: %v", err)
	}
	var creds map[string]any
	if err := json.Unmarshal(rawCreds, &creds); err != nil {
		t.Fatalf("credentials JSON: %v", err)
	}
	if creds["project_id"] != "proj" {
		t.Errorf("credentials project_id = %v, want proj", creds["project_id"])
	}
	if creds["client_email"] != email {
		t.Errorf("credentials client_email = %v, want %s", creds["client_email"], email)
	}

	// list.
	listed, err := p.ServiceAccountKeyList(ctx, wildNR(map[string]any{"name": parent + "/keys"}))
	if err != nil {
		t.Fatalf("wildcard keys.list: %v", err)
	}
	items, _ := listed.Data["keys"].([]any)
	if len(items) != 1 {
		t.Fatalf("wildcard keys.list returned %d keys, want 1", len(items))
	}
	for _, item := range items {
		key, _ := item.(map[string]any)
		if got, _ := key["name"].(string); !strings.HasPrefix(got, canonical) {
			t.Errorf("wildcard keys.list name = %q, want the %q prefix", got, canonical)
		}
	}

	// get / disable / enable / delete.
	got, err := p.ServiceAccountKeyGet(ctx, wildNR(map[string]any{"name": parent + "/keys/" + keyID}))
	if err != nil {
		t.Fatalf("wildcard keys.get: %v", err)
	}
	if got.Data["name"] != canonical+keyID {
		t.Errorf("wildcard keys.get name = %v, want %v", got.Data["name"], canonical+keyID)
	}
	dis, err := p.ServiceAccountKeyDisable(ctx, wildNR(map[string]any{"name": parent + "/keys/" + keyID}))
	if err != nil {
		t.Fatalf("wildcard keys.disable: %v", err)
	}
	assertEmptyResponse(t, "wildcard keys.disable", dis)
	if got, err = p.ServiceAccountKeyGet(ctx, wildNR(map[string]any{"name": parent + "/keys/" + keyID})); err != nil {
		t.Fatalf("wildcard keys.get after disable: %v", err)
	}
	if got.Data["disabled"] != true {
		t.Errorf("wildcard keys.disable disabled = %v, want true", got.Data["disabled"])
	}
	en, err := p.ServiceAccountKeyEnable(ctx, wildNR(map[string]any{"name": parent + "/keys/" + keyID}))
	if err != nil {
		t.Fatalf("wildcard keys.enable: %v", err)
	}
	assertEmptyResponse(t, "wildcard keys.enable", en)
	if got, err = p.ServiceAccountKeyGet(ctx, wildNR(map[string]any{"name": parent + "/keys/" + keyID})); err != nil {
		t.Fatalf("wildcard keys.get after enable: %v", err)
	}
	if got.Data["disabled"] != false {
		t.Errorf("wildcard keys.enable disabled = %v, want false", got.Data["disabled"])
	}
	if _, err := p.ServiceAccountKeyDelete(ctx, wildNR(map[string]any{"name": parent + "/keys/" + keyID})); err != nil {
		t.Fatalf("wildcard keys.delete: %v", err)
	}
	if _, err := p.ServiceAccountKeyGet(ctx, wildNR(map[string]any{"name": parent + "/keys/" + keyID})); err == nil || errStatus(err) != 404 {
		t.Fatalf("wildcard keys.get after delete = %v, want 404", err)
	}
}

// TestServiceAccountWildcardMissIsPermissionDenied pins the documented negative:
// a miss through "projects/-" answers 403 PERMISSION_DENIED, not 404 (the
// Discovery document spells this out for every serviceAccounts method), while
// the concrete project path keeps 404.
func TestServiceAccountWildcardMissIsPermissionDenied(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())
	missing := "serviceAccounts/ghost@proj.iam.gserviceaccount.com"

	cases := []struct {
		name string
		call func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error)
	}{
		{"get", p.Get},
		{"patch", p.Update},
		{"delete", p.Delete},
		{"disable", p.Disable},
		{"enable", p.Enable},
		{"undelete", p.Undelete},
		{"getIamPolicy", p.GetIamPolicy},
		{"setIamPolicy", p.SetIamPolicy},
		{"testIamPermissions", p.TestIamPermissions},
	}
	for _, tc := range cases {
		_, err := tc.call(ctx, wildNR(map[string]any{"name": missing}))
		pe, status := wildcardErrStatus(t, err)
		if status != 403 || pe.Code != "PermissionDenied" || pe.Status != "PERMISSION_DENIED" {
			t.Errorf("%s through projects/- = %v (status %d), want 403 PermissionDenied/PERMISSION_DENIED", tc.name, pe, status)
		}
		_, err = tc.call(ctx, newNR(map[string]any{"name": missing}))
		if status := errStatus(err); status != 404 {
			t.Errorf("%s through the concrete project = %v, want 404", tc.name, err)
		}
	}

	keyCases := []struct {
		name string
		call func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error)
	}{
		{"keys.create", p.ServiceAccountKeyCreate},
		{"keys.list", p.ServiceAccountKeyList},
		{"keys.get", p.ServiceAccountKeyGet},
		{"keys.delete", p.ServiceAccountKeyDelete},
		{"keys.disable", p.ServiceAccountKeyDisable},
		{"keys.enable", p.ServiceAccountKeyEnable},
	}
	for _, tc := range keyCases {
		_, err := tc.call(ctx, wildNR(map[string]any{"name": "serviceAccounts/ghost@proj.iam.gserviceaccount.com/keys/deadbeef"}))
		pe, status := wildcardErrStatus(t, err)
		if status != 403 || pe.Code != "PermissionDenied" {
			t.Errorf("%s through projects/- = %v (status %d), want 403 PermissionDenied", tc.name, pe, status)
		}
	}
	// The key verbs address a key name here; the signing verbs refuse an
	// unresolvable wildcard address separately (TestServiceAccountSignUniqueID),
	// because a wildcard *email* still signs lazily by design.
	//
	// No signature request may leave a key under the literal "-" account.
	entries, err := p.resources.List(ctx, "-", store.GlobalRegion, rtServiceAccountKey, "")
	if err != nil {
		t.Fatalf("list the wildcard account: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("%d keys were written under the projects/- account, want none", len(entries))
	}

	// An identifier that is neither an email nor resolvable in a concrete
	// project cannot scope the request either.
	_, err = p.Get(ctx, wildNR(map[string]any{"name": "serviceAccounts/107876543210987654321"}))
	if pe, status := wildcardErrStatus(t, err); status != 403 || pe.Code != "PermissionDenied" {
		t.Errorf("unique id through projects/- = %v (status %d), want 403 PermissionDenied", pe, status)
	}
}

// TestServiceAccountUniqueIDName covers the second documented identifier form:
// projects.serviceAccounts.get/patch/... and the key methods accept the
// account's unique id in place of the email. The fixture overwrites the
// generated id with a fixed one so the assertions do not depend on the random
// generator (TestServiceAccountCreateNumericIDs covers the generated values).
func TestServiceAccountUniqueIDName(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())
	const uniqueID = "107876543210987654321"
	email := "uniq@proj.iam.gserviceaccount.com"

	if _, err := p.Create(ctx, newNR(map[string]any{"body": map[string]any{"accountId": "uniq"}})); err != nil {
		t.Fatalf("create: %v", err)
	}
	// Give the account the numeric unique id real GCP assigns.
	e, err := p.resources.Get(ctx, "proj", store.GlobalRegion, rtServiceAccount, email)
	if err != nil {
		t.Fatalf("load stored account: %v", err)
	}
	var m serviceAccountMeta
	if err := json.Unmarshal(e.Data, &m); err != nil {
		t.Fatalf("decode stored account: %v", err)
	}
	m.UniqueID = uniqueID
	e.Data = mustJSON(m)
	if err := p.resources.Upsert(ctx, "proj", store.GlobalRegion, e); err != nil {
		t.Fatalf("store uniqueId: %v", err)
	}

	// get and keys.list by unique id under the concrete project path.
	resp, err := p.Get(ctx, newNR(map[string]any{"name": "serviceAccounts/" + uniqueID}))
	if err != nil {
		t.Fatalf("get by unique id: %v", err)
	}
	if resp.Data["email"] != email || resp.Data["name"] != "projects/proj/serviceAccounts/"+email {
		t.Errorf("get by unique id = %v, want email %q and the canonical name", resp.Data, email)
	}
	if _, err := p.ServiceAccountKeyList(ctx, newNR(map[string]any{"name": "serviceAccounts/" + uniqueID + "/keys"})); err != nil {
		t.Fatalf("keys.list by unique id: %v", err)
	}

	// undelete resolves a deleted account by its unique id too.
	if _, err := p.Delete(ctx, newNR(map[string]any{"name": "serviceAccounts/" + email})); err != nil {
		t.Fatalf("delete: %v", err)
	}
	resp, err = p.Undelete(ctx, newNR(map[string]any{"name": "serviceAccounts/" + uniqueID}))
	if err != nil {
		t.Fatalf("undelete by unique id: %v", err)
	}
	restored, _ := resp.Data["restoredAccount"].(map[string]any)
	if restored["email"] != email {
		t.Errorf("undelete by unique id restored %v, want %q", restored["email"], email)
	}

	// Records written before uniqueId was stored keep reporting the email there.
	legacyEmail := "legacy@proj.iam.gserviceaccount.com"
	if err := p.resources.Upsert(ctx, "proj", store.GlobalRegion, store.ResourceEntry{
		Type: rtServiceAccount,
		ID:   legacyEmail,
		Data: mustJSON(serviceAccountMeta{
			Name:      "projects/proj/serviceAccounts/" + legacyEmail,
			Email:     legacyEmail,
			ProjectID: "proj",
		}),
	}); err != nil {
		t.Fatalf("store legacy account: %v", err)
	}
	resp, err = p.Get(ctx, newNR(map[string]any{"name": "serviceAccounts/" + legacyEmail}))
	if err != nil {
		t.Fatalf("get legacy account: %v", err)
	}
	if resp.Data["uniqueId"] != legacyEmail {
		t.Errorf("legacy uniqueId = %v, want %q", resp.Data["uniqueId"], legacyEmail)
	}
	if resp.Data["oauth2ClientId"] != "" {
		t.Errorf("legacy oauth2ClientId = %v, want the empty value such a record was written with", resp.Data["oauth2ClientId"])
	}
}

// TestServiceAccountSignUniqueID covers the unique-id form on the signing verbs:
// it must resolve to the real account, and it must not sign for a phantom
// account under the literal "projects/-" project.
func TestServiceAccountSignUniqueID(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())
	const uniqueID = "107876543210987654321"
	email := "signer@proj.iam.gserviceaccount.com"
	if _, err := p.Create(ctx, newNR(map[string]any{"body": map[string]any{"accountId": "signer"}})); err != nil {
		t.Fatalf("create: %v", err)
	}
	e, err := p.resources.Get(ctx, "proj", store.GlobalRegion, rtServiceAccount, email)
	if err != nil {
		t.Fatalf("load stored account: %v", err)
	}
	var m serviceAccountMeta
	if err := json.Unmarshal(e.Data, &m); err != nil {
		t.Fatalf("decode stored account: %v", err)
	}
	m.UniqueID = uniqueID
	e.Data = mustJSON(m)
	if err := p.resources.Upsert(ctx, "proj", store.GlobalRegion, e); err != nil {
		t.Fatalf("store uniqueId: %v", err)
	}

	payload := base64.StdEncoding.EncodeToString([]byte("hello"))
	signed, err := p.ServiceAccountSignBlob(ctx, newNR(map[string]any{
		"name": "serviceAccounts/" + uniqueID,
		"body": map[string]any{"payload": payload},
	}))
	if err != nil {
		t.Fatalf("signBlob by unique id: %v", err)
	}
	if sig, _ := signed.Data["signature"].(string); sig == "" {
		t.Error("signBlob by unique id returned no signature")
	}
	// The key belongs to the resolved account, not to the unique-id string.
	entries, err := p.resources.List(ctx, "proj", store.GlobalRegion, rtServiceAccountKey, "")
	if err != nil {
		t.Fatalf("list keys: %v", err)
	}
	for _, entry := range entries {
		var k serviceAccountKeyMeta
		if json.Unmarshal(entry.Data, &k) == nil && k.Email != email {
			t.Errorf("key signed for %q, want %q", k.Email, email)
		}
	}

	// The wildcard cannot scope a unique id (DC29), and a refused request must
	// not materialise a signing key under the "-" account.
	_, err = p.ServiceAccountSignJwt(ctx, wildNR(map[string]any{
		"name": "serviceAccounts/" + uniqueID,
		"body": map[string]any{"payload": `{"iss":"proj"}`},
	}))
	if pe, status := wildcardErrStatus(t, err); status != 403 || pe.Code != "PermissionDenied" {
		t.Errorf("signJwt by unique id through projects/- = %v (status %d), want 403 PermissionDenied", pe, status)
	}
	wildEntries, err := p.resources.List(ctx, "-", store.GlobalRegion, rtServiceAccountKey, "")
	if err != nil {
		t.Fatalf("list the wildcard account: %v", err)
	}
	if len(wildEntries) != 0 {
		t.Errorf("%d keys were written under the projects/- account, want none", len(wildEntries))
	}
}
