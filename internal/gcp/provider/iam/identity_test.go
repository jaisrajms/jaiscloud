// identity_test.go covers DC25: the two output-only service-account identifiers
// real IAM assigns. Discovery documents uniqueId as "the unique, stable numeric
// ID ... Each service account retains its unique ID even if you delete the
// service account ... create a new service account with the same name, the new
// service account has a different unique ID", and oauth2ClientId as a distinct
// numeric client id (the Discovery sample pairs 123456789012345678901 with
// 987654321098765432109). The emulator used to return the email as uniqueId and
// an empty oauth2ClientId, so both are now generated, stored and asserted here.
package iam

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"

	"jaiscloud/internal/store"
)

// numericID asserts that field is a 21-digit decimal string (the shape real IAM
// assigns: not an int64, which would overflow) and returns it.
func numericID(t *testing.T, field string, v any) string {
	t.Helper()
	s, ok := v.(string)
	if !ok {
		t.Fatalf("%s = %v (%T), want a 21-digit string", field, v, v)
	}
	if len(s) != 21 {
		t.Fatalf("%s = %q, want 21 digits", field, s)
	}
	if s[0] == '0' {
		t.Errorf("%s = %q, want a non-zero leading digit", field, s)
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			t.Fatalf("%s = %q, want only decimal digits", field, s)
		}
	}
	return s
}

func TestServiceAccountCreateNumericIDs(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())

	created, err := p.Create(ctx, newNR(map[string]any{"body": map[string]any{"accountId": "identity"}}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	email := "identity@proj.iam.gserviceaccount.com"
	uniqueID := numericID(t, "uniqueId", created.Data["uniqueId"])
	clientID := numericID(t, "oauth2ClientId", created.Data["oauth2ClientId"])
	if uniqueID == clientID {
		t.Errorf("uniqueId and oauth2ClientId are the same value %q, want distinct ids", uniqueID)
	}
	if uniqueID == email || clientID == email {
		t.Errorf("identifiers = %q / %q, want numeric ids rather than the email", uniqueID, clientID)
	}

	// get by the generated unique id resolves the account and reports the
	// canonical resource name, and the key methods resolve it too.
	got, err := p.Get(ctx, newNR(map[string]any{"name": "serviceAccounts/" + uniqueID}))
	if err != nil {
		t.Fatalf("get by unique id: %v", err)
	}
	if got.Data["email"] != email {
		t.Errorf("get by unique id email = %v, want %q", got.Data["email"], email)
	}
	if got.Data["name"] != "projects/proj/serviceAccounts/"+email {
		t.Errorf("get by unique id name = %v, want the canonical name", got.Data["name"])
	}
	if got.Data["uniqueId"] != uniqueID || got.Data["oauth2ClientId"] != clientID {
		t.Errorf("get by unique id identifiers = %v, want the stored %q / %q", got.Data, uniqueID, clientID)
	}
	if _, err := p.ServiceAccountKeyList(ctx, newNR(map[string]any{"name": "serviceAccounts/" + uniqueID + "/keys"})); err != nil {
		t.Fatalf("keys.list by unique id: %v", err)
	}

	// list reports the same stored values.
	listed, err := p.List(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	accounts, _ := listed.Data["accounts"].([]any)
	if len(accounts) != 1 {
		t.Fatalf("list returned %d accounts, want 1", len(accounts))
	}
	item, _ := accounts[0].(map[string]any)
	if item["uniqueId"] != uniqueID || item["oauth2ClientId"] != clientID {
		t.Errorf("list identifiers = %v, want the stored %q / %q", item, uniqueID, clientID)
	}

	// A second account gets its own identifiers.
	other, err := p.Create(ctx, newNR(map[string]any{"body": map[string]any{"accountId": "identity2"}}))
	if err != nil {
		t.Fatalf("create second: %v", err)
	}
	otherUnique := numericID(t, "uniqueId", other.Data["uniqueId"])
	otherClient := numericID(t, "oauth2ClientId", other.Data["oauth2ClientId"])
	if otherUnique == uniqueID || otherClient == clientID {
		t.Errorf("second account reused an identifier (%q, %q)", otherUnique, otherClient)
	}

	// The documented stability contract: deleting the account keeps its id, and
	// undelete restores that same id.
	if _, err := p.Delete(ctx, newNR(map[string]any{"name": "serviceAccounts/" + email})); err != nil {
		t.Fatalf("delete: %v", err)
	}
	restored, err := p.Undelete(ctx, newNR(map[string]any{"name": "serviceAccounts/" + uniqueID}))
	if err != nil {
		t.Fatalf("undelete by unique id: %v", err)
	}
	account, _ := restored.Data["restoredAccount"].(map[string]any)
	if account["uniqueId"] != uniqueID || account["oauth2ClientId"] != clientID {
		t.Errorf("undelete identifiers = %v, want the original %q / %q", account, uniqueID, clientID)
	}
}

// TestServiceAccountRecreateGetsNewNumericID covers the other half of the
// documented uniqueId contract: a service account re-created under the same name
// gets a different id, so the old id must no longer resolve.
func TestServiceAccountRecreateGetsNewNumericID(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())
	email := "reborn@proj.iam.gserviceaccount.com"

	first, err := p.Create(ctx, newNR(map[string]any{"body": map[string]any{"accountId": "reborn"}}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	firstID := numericID(t, "uniqueId", first.Data["uniqueId"])
	if _, err := p.Delete(ctx, newNR(map[string]any{"name": "serviceAccounts/" + email})); err != nil {
		t.Fatalf("delete: %v", err)
	}

	second, err := p.Create(ctx, newNR(map[string]any{"body": map[string]any{"accountId": "reborn"}}))
	if err != nil {
		t.Fatalf("re-create: %v", err)
	}
	secondID := numericID(t, "uniqueId", second.Data["uniqueId"])
	if secondID == firstID {
		t.Errorf("re-created account uniqueId = %q, want a different id than the deleted %q", secondID, firstID)
	}

	if _, err := p.Get(ctx, newNR(map[string]any{"name": "serviceAccounts/" + firstID})); err == nil || errStatus(err) != 404 {
		t.Errorf("get by the deleted account's unique id = %v, want 404", err)
	}
}

// TestServiceAccountCreateIgnoresOutputOnlyFields covers the other half of the
// identifier contract: uniqueId, oauth2ClientId and disabled are output-only in
// the Discovery schema, so a create that supplies them must answer the
// server-generated values (an account is created enabled).
func TestServiceAccountCreateIgnoresOutputOnlyFields(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())

	const (
		suppliedUnique = "123456789012345678901"
		suppliedClient = "987654321098765432109"
	)
	resp, err := p.Create(ctx, newNR(map[string]any{"body": map[string]any{
		"accountId": "outputonly",
		"serviceAccount": map[string]any{
			"displayName":    "Output Only",
			"uniqueId":       suppliedUnique,
			"oauth2ClientId": suppliedClient,
			"disabled":       true,
		},
	}}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	uniqueID := numericID(t, "uniqueId", resp.Data["uniqueId"])
	clientID := numericID(t, "oauth2ClientId", resp.Data["oauth2ClientId"])
	if uniqueID == suppliedUnique || clientID == suppliedClient {
		t.Errorf("create echoed an output-only identifier: %v", resp.Data)
	}
	if disabled, _ := resp.Data["disabled"].(bool); disabled {
		t.Error("create honored the output-only disabled field, want an enabled account")
	}
	if resp.Data["displayName"] != "Output Only" {
		t.Errorf("displayName = %v, want the caller-supplied value", resp.Data["displayName"])
	}
}

// TestServiceAccountKeyCredentialsClientID asserts the credentials file carries
// the account's oauth2ClientId: the official client libraries read client_id out
// of the file, so the emulator's old "0" placeholder made it unusable.
func TestServiceAccountKeyCredentialsClientID(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())
	email := "keyed@proj.iam.gserviceaccount.com"

	created, err := p.Create(ctx, newNR(map[string]any{"body": map[string]any{"accountId": "keyed"}}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	clientID := numericID(t, "oauth2ClientId", created.Data["oauth2ClientId"])

	key, err := p.ServiceAccountKeyCreate(ctx, newNR(map[string]any{"name": "serviceAccounts/" + email + "/keys"}))
	if err != nil {
		t.Fatalf("keys.create: %v", err)
	}
	raw, err := base64.StdEncoding.DecodeString(key.Data["privateKeyData"].(string))
	if err != nil {
		t.Fatalf("decode privateKeyData: %v", err)
	}
	var creds map[string]any
	if err := json.Unmarshal(raw, &creds); err != nil {
		t.Fatalf("credentials JSON: %v", err)
	}
	if creds["client_id"] != clientID {
		t.Errorf("credentials client_id = %v, want the account's oauth2ClientId %q", creds["client_id"], clientID)
	}
	if keyName, _ := key.Data["name"].(string); keyName == "" {
		t.Error("keys.create returned no key name")
	}
}
