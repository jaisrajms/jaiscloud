package iam

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"jaiscloud/internal/gcp/serviceaccount"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// systemKeyWindow is the validity window real IAM reports for a Google-managed
// key: validBeforeTime is 730 days after validAfterTime (probe 2026-11-02, a
// key created at 2026-11-03T03:23:54Z expires 2028-11-02T03:23:54Z).
const systemKeyWindow = 730 * 24 * time.Hour

// createManagedSA creates a service account through the provider and returns its
// email. Create is the path NF15 changed, so every test here starts from it.
func createManagedSA(t *testing.T, p *Provider, accountID string) string {
	t.Helper()
	if _, err := p.Create(context.Background(), newNR(map[string]any{
		"body": map[string]any{"accountId": accountID},
	})); err != nil {
		t.Fatalf("create SA %q: %v", accountID, err)
	}
	return accountID + "@proj.iam.gserviceaccount.com"
}

// listManagedKeys returns a service account's keys as rendered maps.
func listManagedKeys(t *testing.T, p *Provider, email string) []map[string]any {
	t.Helper()
	resp, err := p.ServiceAccountKeyList(context.Background(),
		newNR(map[string]any{"name": "serviceAccounts/" + email + "/keys"}))
	if err != nil {
		t.Fatalf("keys.list %s: %v", email, err)
	}
	raw, _ := resp.Data["keys"].([]any)
	out := make([]map[string]any, 0, len(raw))
	for _, item := range raw {
		m, _ := item.(map[string]any)
		out = append(out, m)
	}
	return out
}

// requireManagedKey asserts the NF15 shape of a listed Google-managed key and
// returns it: SYSTEM_MANAGED/GOOGLE_PROVIDED, RSA-2048, and a two-year window
// anchored on validAfterTime (never the user-managed 9999-12-31 sentinel).
func requireManagedKey(t *testing.T, key map[string]any) {
	t.Helper()
	if key["keyType"] != "SYSTEM_MANAGED" || key["keyOrigin"] != "GOOGLE_PROVIDED" {
		t.Fatalf("listed key is keyType=%v keyOrigin=%v, want SYSTEM_MANAGED/GOOGLE_PROVIDED: %v",
			key["keyType"], key["keyOrigin"], key)
	}
	if key["keyAlgorithm"] != "KEY_ALG_RSA_2048" {
		t.Errorf("managed key algorithm = %v, want KEY_ALG_RSA_2048", key["keyAlgorithm"])
	}
	if key["disabled"] != false {
		t.Errorf("managed key disabled = %v, want false", key["disabled"])
	}
	afterStr, _ := key["validAfterTime"].(string)
	beforeStr, _ := key["validBeforeTime"].(string)
	after, err := time.Parse(time.RFC3339Nano, afterStr)
	if err != nil {
		t.Fatalf("validAfterTime %q: %v", afterStr, err)
	}
	before, err := time.Parse(time.RFC3339Nano, beforeStr)
	if err != nil {
		t.Fatalf("validBeforeTime %q: %v", beforeStr, err)
	}
	if beforeStr == nonExpiringKeyTime {
		t.Errorf("managed key validBeforeTime = %s, want validAfterTime + two years", beforeStr)
	}
	if got := before.Sub(after); got != systemKeyWindow {
		t.Errorf("managed key window = %v (validAfter %s, validBefore %s), want %v",
			got, afterStr, beforeStr, systemKeyWindow)
	}
}

// assertManagedKeyContract is the NF15 contract, run against any ResourceStore
// so the memory store and --dsn (Postgres) cannot drift.
func assertManagedKeyContract(t *testing.T, p *Provider, accountID string) {
	t.Helper()
	ctx := context.Background()
	email := createManagedSA(t, p, accountID)

	// A fresh account already lists exactly one Google-managed key — nothing has
	// signed yet.
	keys := listManagedKeys(t, p, email)
	if len(keys) != 1 {
		t.Fatalf("fresh account lists %d keys, want exactly 1 (the Google-managed key): %v", len(keys), keys)
	}
	requireManagedKey(t, keys[0])
	keyName, _ := keys[0]["name"].(string)
	if !strings.Contains(keyName, "/keys/") {
		t.Fatalf("managed key name %q is not a key resource name", keyName)
	}
	keyID := keyName[strings.LastIndex(keyName, "/")+1:]

	// keyTypes filters see the same key.
	if got := listKeyTypes(t, p, queryNR(email, "keyTypes=SYSTEM_MANAGED")); got["SYSTEM_MANAGED"] != 1 || got["USER_MANAGED"] != 0 {
		t.Errorf("keyTypes=SYSTEM_MANAGED = %v, want only the managed key", got)
	}
	if got := listKeyTypes(t, p, queryNR(email, "keyTypes=USER_MANAGED")); got["USER_MANAGED"] != 0 {
		t.Errorf("keyTypes=USER_MANAGED = %v, want none on a fresh account", got)
	}

	// keys.get answers 200 with the metadata and no publicKeyData: a
	// Google-managed key is not downloadable (probe 2026-11-02).
	got, err := p.ServiceAccountKeyGet(ctx, newNR(map[string]any{"name": "serviceAccounts/" + email + "/keys/" + keyID}))
	if err != nil {
		t.Fatalf("keys.get on the managed key: %v", err)
	}
	if _, present := got.Data["publicKeyData"]; present {
		t.Error("keys.get on the Google-managed key must not emit publicKeyData")
	}
	for field, want := range map[string]any{
		"name":         keyName,
		"keyType":      "SYSTEM_MANAGED",
		"keyOrigin":    "GOOGLE_PROVIDED",
		"keyAlgorithm": "KEY_ALG_RSA_2048",
	} {
		if got.Data[field] != want {
			t.Errorf("keys.get %s = %v, want %v", field, got.Data[field], want)
		}
	}

	// delete/disable/enable are 404 NOT_FOUND: real IAM addresses a
	// Google-managed key from keys.list only.
	keyPath := newNR(map[string]any{"name": "serviceAccounts/" + email + "/keys/" + keyID})
	mutations := []struct {
		name string
		call func() (*model.ProviderResponse, error)
	}{
		{"delete", func() (*model.ProviderResponse, error) { return p.ServiceAccountKeyDelete(ctx, keyPath) }},
		{"disable", func() (*model.ProviderResponse, error) { return p.ServiceAccountKeyDisable(ctx, keyPath) }},
		{"enable", func() (*model.ProviderResponse, error) { return p.ServiceAccountKeyEnable(ctx, keyPath) }},
	}
	for _, m := range mutations {
		if _, err := m.call(); err == nil {
			t.Errorf("keys.%s on the Google-managed key succeeded, want 404", m.name)
		} else if perr, ok := err.(*model.ProviderError); !ok || perr.HTTPStatus != 404 {
			t.Errorf("keys.%s on the Google-managed key = %v, want 404", m.name, err)
		}
	}

	// signing uses that same key and never mints a second one.
	sign := func() string {
		t.Helper()
		resp, err := p.ServiceAccountSignBlob(ctx, newNR(map[string]any{
			"name": "serviceAccounts/" + email,
			"body": map[string]any{"payload": "aGVsbG8="},
		}))
		if err != nil {
			t.Fatalf("signBlob: %v", err)
		}
		id, _ := resp.Data["keyId"].(string)
		return id
	}
	if signed := sign(); signed != keyID {
		t.Errorf("signBlob signed with key %q, want the managed key %q created with the account", signed, keyID)
	}
	if signed := sign(); signed != keyID {
		t.Errorf("second signBlob signed with key %q, want the same key %q", signed, keyID)
	}
	if keys := listManagedKeys(t, p, email); len(keys) != 1 {
		t.Errorf("signing materialised another key: %d keys listed, want 1", len(keys))
	}

	// A user-managed key is unaffected: created, gettable with publicKeyData,
	// deletable, and it never replaces the managed key for signing.
	created, err := p.ServiceAccountKeyCreate(ctx, newNR(map[string]any{"name": "serviceAccounts/" + email + "/keys"}))
	if err != nil {
		t.Fatalf("keys.create: %v", err)
	}
	if created.Data["keyType"] != "USER_MANAGED" || created.Data["keyOrigin"] != "USER_PROVIDED" {
		t.Errorf("keys.create = keyType %v keyOrigin %v, want USER_MANAGED/USER_PROVIDED",
			created.Data["keyType"], created.Data["keyOrigin"])
	}
	if created.Data["validBeforeTime"] != nonExpiringKeyTime {
		t.Errorf("user-managed key validBeforeTime = %v, want %s", created.Data["validBeforeTime"], nonExpiringKeyTime)
	}
	userName, _ := created.Data["name"].(string)
	userID := userName[strings.LastIndex(userName, "/")+1:]
	userGot, err := p.ServiceAccountKeyGet(ctx, newNR(map[string]any{"name": "serviceAccounts/" + email + "/keys/" + userID}))
	if err != nil {
		t.Fatalf("keys.get on the user-managed key: %v", err)
	}
	if _, present := userGot.Data["publicKeyData"]; !present {
		t.Error("keys.get on a user-managed key must carry publicKeyData")
	}
	if signed := sign(); signed != keyID {
		t.Errorf("signBlob signed with %q after a user-managed key existed, want the managed key %q", signed, keyID)
	}
	if _, err := p.ServiceAccountKeyDelete(ctx, newNR(map[string]any{"name": "serviceAccounts/" + email + "/keys/" + userID})); err != nil {
		t.Fatalf("keys.delete on the user-managed key: %v", err)
	}
	if _, err := p.ServiceAccountKeyGet(ctx, newNR(map[string]any{"name": "serviceAccounts/" + email + "/keys/" + userID})); err == nil {
		t.Error("deleted user-managed key still resolves")
	}
}

// TestServiceAccountCreateMaterialisesManagedKey covers NF15/DC26 on the memory
// store: real IAM creates the Google-managed key pair with the account.
func TestServiceAccountCreateMaterialisesManagedKey(t *testing.T) {
	p := New(store.NewMemoryResourceStore())
	assertManagedKeyContract(t, p, "fresh")
}

// TestServiceAccountSignMaterialisesManagedKeyForLegacyAccount covers accounts
// persisted before NF15: the record exists without a pair, and the lazy path
// still materialises one on the first sign.
func TestServiceAccountSignMaterialisesManagedKeyForLegacyAccount(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())

	// A legacy snapshot holds the account record and no key at all.
	email := "legacy@proj.iam.gserviceaccount.com"
	data, _ := json.Marshal(serviceAccountMeta{
		Name:           "projects/proj/serviceAccounts/" + email,
		Email:          email,
		ProjectID:      "proj",
		Oauth2ClientID: "123456789012345678901",
		UniqueID:       "123456789012345678902",
	})
	if err := p.resources.Upsert(ctx, "proj", store.GlobalRegion, store.ResourceEntry{
		Type: rtServiceAccount, ID: email, Data: data,
	}); err != nil {
		t.Fatalf("seed legacy account: %v", err)
	}
	if keys := listManagedKeys(t, p, email); len(keys) != 0 {
		t.Fatalf("legacy account already lists %d keys, want none before signing", len(keys))
	}

	resp, err := p.ServiceAccountSignBlob(ctx, newNR(map[string]any{
		"name": "serviceAccounts/" + email,
		"body": map[string]any{"payload": "aGVsbG8="},
	}))
	if err != nil {
		t.Fatalf("signBlob on a legacy account: %v", err)
	}
	keyID, _ := resp.Data["keyId"].(string)
	keys := listManagedKeys(t, p, email)
	if len(keys) != 1 {
		t.Fatalf("first sign materialised %d keys, want exactly 1", len(keys))
	}
	requireManagedKey(t, keys[0])
	if name, _ := keys[0]["name"].(string); !strings.HasSuffix(name, "/keys/"+keyID) {
		t.Errorf("listed key %q does not match the signing key %q", name, keyID)
	}
}

// TestServiceAccountManagedKeySnapshotRoundTrip covers the soft-restart path:
// the pair is part of the account's persisted state, so a restore neither loses
// it nor mints a second one.
func TestServiceAccountManagedKeySnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	mem := store.NewMemoryResourceStore()
	p := New(mem)
	email := createManagedSA(t, p, "snap")

	before := listManagedKeys(t, p, email)
	if len(before) != 1 {
		t.Fatalf("pre-snapshot keys = %d, want 1", len(before))
	}

	var buf bytes.Buffer
	if err := mem.Snapshot(ctx, &buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	restored := store.NewMemoryResourceStore()
	if err := restored.Restore(ctx, &buf); err != nil {
		t.Fatalf("restore: %v", err)
	}
	p2 := New(restored)

	after := listManagedKeys(t, p2, email)
	if len(after) != 1 {
		t.Fatalf("post-restore keys = %d, want 1", len(after))
	}
	requireManagedKey(t, after[0])
	for _, field := range []string{"name", "validAfterTime", "validBeforeTime", "keyAlgorithm", "keyType", "keyOrigin"} {
		if before[0][field] != after[0][field] {
			t.Errorf("restored key %s = %v, want %v", field, after[0][field], before[0][field])
		}
	}

	// The restored pair still signs, and signing does not replace it.
	resp, err := p2.ServiceAccountSignBlob(ctx, newNR(map[string]any{
		"name": "serviceAccounts/" + email,
		"body": map[string]any{"payload": "aGVsbG8="},
	}))
	if err != nil {
		t.Fatalf("signBlob after restore: %v", err)
	}
	listed := before[0]["name"].(string)
	wantID := listed[strings.LastIndex(listed, "/")+1:]
	if id, _ := resp.Data["keyId"].(string); id != wantID {
		t.Errorf("post-restore signBlob signed with %q, want the restored managed key %q", id, wantID)
	}
	if keys := listManagedKeys(t, p2, email); len(keys) != 1 {
		t.Errorf("post-restore signing materialised another key: %d listed", len(keys))
	}
}

// TestServiceAccountManagedKeyTypeOfExistingKeys pins the classification the
// create-time pair must share with the lazily created one, so an account that
// predates NF15 and one created after it list the same key type.
func TestServiceAccountManagedKeyTypeOfExistingKeys(t *testing.T) {
	p := New(store.NewMemoryResourceStore())
	email := createManagedSA(t, p, "classify")

	keys := listManagedKeys(t, p, email)
	if len(keys) != 1 {
		t.Fatalf("keys = %d, want 1", len(keys))
	}
	if keys[0]["keyType"] != serviceaccount.KeyTypeSystemManaged {
		t.Errorf("keyType = %v, want %s", keys[0]["keyType"], serviceaccount.KeyTypeSystemManaged)
	}
}
