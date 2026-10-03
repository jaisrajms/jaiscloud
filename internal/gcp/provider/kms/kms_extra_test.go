package kms

import (
	"context"
	"encoding/base64"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	kmsstore "jaiscloud/internal/gcp/store/kms"
	"jaiscloud/internal/model"
)

func errStatus(err error) int {
	if pe, ok := err.(*model.ProviderError); ok {
		return pe.HTTPStatus
	}
	return 0
}

func TestKMSNegativesAndPagination(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	for _, kr := range []string{"kr-a", "kr-b", "kr-c"} {
		nr := newNR(map[string]any{"location": "global", "keyRingId": kr})
		if _, err := p.KeyRingCreate(ctx, nr); err != nil {
			t.Fatalf("create keyring %s: %v", kr, err)
		}
	}

	// KeyRing pagination.
	nr := newNR(map[string]any{"location": "global", "pageSize": "2"})
	resp, err := p.KeyRingList(ctx, nr)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	rings, _ := resp.Data["keyRings"].([]any)
	if len(rings) != 2 {
		t.Fatalf("page 1 expected 2 keyrings, got %d", len(rings))
	}
	token, _ := resp.Data["nextPageToken"].(string)
	if token == "" {
		t.Fatal("expected nextPageToken")
	}

	// 409 duplicate keyring.
	nr = newNR(map[string]any{"location": "global", "keyRingId": "kr-a"})
	if _, err := p.KeyRingCreate(ctx, nr); err == nil || errStatus(err) != 409 {
		t.Fatalf("expected 409 on duplicate keyring, got %v", err)
	}

	// Encrypt/decrypt response fields.
	nr = newNR(map[string]any{"name": "locations/global/keyRings/kr-a", "cryptoKeyId": "key1"})
	if _, err := p.CryptoKeyCreate(ctx, nr); err != nil {
		t.Fatalf("cryptokey create: %v", err)
	}
	nr = newNR(map[string]any{"name": "locations/global/keyRings/kr-a/cryptoKeys/key1", "body": map[string]any{"plaintext": "aGVsbG8="}})
	enc, err := p.CryptoKeyEncrypt(ctx, nr)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	for _, field := range []string{"ciphertextCrc32c", "protectionLevel", "verifiedPlaintextCrc32c"} {
		if _, ok := enc.Data[field]; !ok {
			t.Errorf("encrypt response missing %s", field)
		}
	}
	if enc.Data["protectionLevel"] != "SOFTWARE" {
		t.Errorf("expected protectionLevel SOFTWARE, got %v", enc.Data["protectionLevel"])
	}

	ciphertext, _ := enc.Data["ciphertext"].(string)
	nr = newNR(map[string]any{"name": "locations/global/keyRings/kr-a/cryptoKeys/key1", "body": map[string]any{"ciphertext": ciphertext}})
	dec, err := p.CryptoKeyDecrypt(ctx, nr)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	for _, field := range []string{"plaintextCrc32c", "protectionLevel", "usedPrimary"} {
		if _, ok := dec.Data[field]; !ok {
			t.Errorf("decrypt response missing %s", field)
		}
	}
	if dec.Data["plaintext"] != "aGVsbG8=" {
		t.Errorf("expected plaintext aGVsbG8=, got %v", dec.Data["plaintext"])
	}

	// 404 missing crypto key.
	nr = newNR(map[string]any{"name": "locations/global/keyRings/kr-a/cryptoKeys/nope"})
	if _, err := p.CryptoKeyGet(ctx, nr); err == nil || errStatus(err) != 404 {
		t.Fatalf("expected 404 on missing crypto key, got %v", err)
	}
}

// TestKMSCryptoKeyNotFound verifies encrypt/decrypt reject a non-existent key
// with 404 instead of silently operating on it.
func TestKMSCryptoKeyNotFound(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := newNR(map[string]any{"name": "locations/global/keyRings/kr-a/cryptoKeys/missing", "body": map[string]any{"plaintext": "aGk="}})
	if _, err := p.CryptoKeyEncrypt(ctx, nr); err == nil || errStatus(err) != 404 {
		t.Fatalf("expected 404 on encrypt missing key, got %v", err)
	}
	nr = newNR(map[string]any{"name": "locations/global/keyRings/kr-a/cryptoKeys/missing", "body": map[string]any{"ciphertext": "amFpc2Nsb3VkOmhp"}})
	if _, err := p.CryptoKeyDecrypt(ctx, nr); err == nil || errStatus(err) != 404 {
		t.Fatalf("expected 404 on decrypt missing key, got %v", err)
	}
}

// TestKMSEncryptDecryptRealCrypto verifies AES-GCM semantics: nondeterministic
// ciphertext and decryption failure on a wrong AAD.
func TestKMSEncryptDecryptRealCrypto(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "kr"})); err != nil {
		t.Fatalf("keyring: %v", err)
	}
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{"name": "locations/global/keyRings/kr", "cryptoKeyId": "k"})); err != nil {
		t.Fatalf("cryptokey: %v", err)
	}

	enc := func(aad string) string {
		body := map[string]any{"plaintext": "aGVsbG8="}
		if aad != "" {
			body["additionalAuthenticatedData"] = aad
		}
		resp, err := p.CryptoKeyEncrypt(ctx, newNR(map[string]any{"name": "locations/global/keyRings/kr/cryptoKeys/k", "body": body}))
		if err != nil {
			t.Fatalf("encrypt: %v", err)
		}
		return resp.Data["ciphertext"].(string)
	}

	c1 := enc("")
	c2 := enc("")
	if c1 == c2 {
		t.Error("expected nondeterministic ciphertext")
	}

	// Decrypt with wrong AAD must fail.
	aad := base64.StdEncoding.EncodeToString([]byte("ctx"))
	_ = enc(aad) // encrypt with AAD
	resp, err := p.CryptoKeyEncrypt(ctx, newNR(map[string]any{"name": "locations/global/keyRings/kr/cryptoKeys/k", "body": map[string]any{"plaintext": "aGVsbG8=", "additionalAuthenticatedData": aad}}))
	if err != nil {
		t.Fatalf("encrypt with aad: %v", err)
	}
	if _, err := p.CryptoKeyDecrypt(ctx, newNR(map[string]any{"name": "locations/global/keyRings/kr/cryptoKeys/k", "body": map[string]any{"ciphertext": resp.Data["ciphertext"]}})); err == nil {
		t.Error("expected decrypt to fail without AAD")
	}
}

// TestKMSRotation verifies that after rotating the primary version, ciphertext
// encrypted under the old version still decrypts (version is embedded in the
// ciphertext blob).
func TestKMSRotation(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "kr"})); err != nil {
		t.Fatalf("keyring: %v", err)
	}
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{"name": "locations/global/keyRings/kr", "cryptoKeyId": "k"})); err != nil {
		t.Fatalf("cryptokey: %v", err)
	}

	enc := func() string {
		resp, err := p.CryptoKeyEncrypt(ctx, newNR(map[string]any{"name": "locations/global/keyRings/kr/cryptoKeys/k", "body": map[string]any{"plaintext": "aGVsbG8="}}))
		if err != nil {
			t.Fatalf("encrypt: %v", err)
		}
		return resp.Data["ciphertext"].(string)
	}
	dec := func(ct string) {
		resp, err := p.CryptoKeyDecrypt(ctx, newNR(map[string]any{"name": "locations/global/keyRings/kr/cryptoKeys/k", "body": map[string]any{"ciphertext": ct}}))
		if err != nil {
			t.Fatalf("decrypt: %v", err)
		}
		if resp.Data["plaintext"] != "aGVsbG8=" {
			t.Errorf("unexpected plaintext: %v", resp.Data["plaintext"])
		}
	}

	old := enc()

	// Create version 2 and rotate primary to it.
	if _, err := p.CryptoKeyVersionCreate(ctx, newNR(map[string]any{"name": "locations/global/keyRings/kr/cryptoKeys/k/cryptoKeyVersions"})); err != nil {
		t.Fatalf("create version: %v", err)
	}
	if _, err := p.CryptoKeyUpdatePrimaryVersion(ctx, newNR(map[string]any{"name": "locations/global/keyRings/kr/cryptoKeys/k", "body": map[string]any{"cryptoKeyVersionId": "2"}})); err != nil {
		t.Fatalf("update primary: %v", err)
	}

	// Both old (v1) and new (v2) ciphertext decrypt correctly.
	dec(old)
	dec(enc())

	// Version list shows two versions.
	lr, err := p.CryptoKeyVersionList(ctx, newNR(map[string]any{"name": "locations/global/keyRings/kr/cryptoKeys/k/cryptoKeyVersions"}))
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	if vs, _ := lr.Data["cryptoKeyVersions"].([]any); len(vs) != 2 {
		t.Fatalf("expected 2 versions, got %v", lr.Data["cryptoKeyVersions"])
	}

	// Get + destroy version 1.
	if _, err := p.CryptoKeyVersionGet(ctx, newNR(map[string]any{"name": "locations/global/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/1"})); err != nil {
		t.Fatalf("get version: %v", err)
	}
	dr, err := p.CryptoKeyVersionDestroy(ctx, newNR(map[string]any{"name": "locations/global/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/1"}))
	if err != nil {
		t.Fatalf("destroy version: %v", err)
	}
	if dr.Data["state"] != "DESTROY_SCHEDULED" {
		t.Errorf("expected DESTROY_SCHEDULED, got %v", dr.Data["state"])
	}
	if _, ok := dr.Data["destroyTime"].(string); !ok {
		t.Errorf("expected destroyTime on scheduled version, got %v", dr.Data)
	}
}

// TestKMSDestroyedPrimaryUnusable verifies a destroyed (scheduled then
// promoted) primary version cannot be used for encryption and that GetCryptoKey
// reports its real state.
func TestKMSDestroyedPrimaryUnusable(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	defer clock.SetGlobalClock(clock.RealClock{})

	ctx := context.Background()
	p := newTestProvider()

	if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "kr"})); err != nil {
		t.Fatalf("keyring: %v", err)
	}
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{"name": "locations/global/keyRings/kr", "cryptoKeyId": "k"})); err != nil {
		t.Fatalf("cryptokey: %v", err)
	}
	keyName := "locations/global/keyRings/kr/cryptoKeys/k"

	// Encrypt works while the primary is ENABLED.
	if _, err := p.CryptoKeyEncrypt(ctx, newNR(map[string]any{"name": keyName, "body": map[string]any{"plaintext": "aGVsbG8="}})); err != nil {
		t.Fatalf("encrypt before destroy: %v", err)
	}

	// Destroy the primary version: it moves to DESTROY_SCHEDULED.
	dr, err := p.CryptoKeyVersionDestroy(ctx, newNR(map[string]any{"name": keyName + "/cryptoKeyVersions/1"}))
	if err != nil {
		t.Fatalf("destroy: %v", err)
	}
	if dr.Data["state"] != "DESTROY_SCHEDULED" {
		t.Fatalf("destroy state = %v, want DESTROY_SCHEDULED", dr.Data["state"])
	}

	// Encrypt must now fail with FailedPrecondition.
	_, err = p.CryptoKeyEncrypt(ctx, newNR(map[string]any{"name": keyName, "body": map[string]any{"plaintext": "aGVsbG8="}}))
	if err == nil {
		t.Fatal("expected encrypt against scheduled primary to fail")
	}
	if pe, ok := err.(*model.ProviderError); !ok || pe.Code != "FailedPrecondition" {
		t.Fatalf("expected FailedPrecondition, got %v", err)
	}

	// GetCryptoKey reports the scheduled state.
	resp, err := p.CryptoKeyGet(ctx, newNR(map[string]any{"name": keyName}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	primary, _ := resp.Data["primary"].(map[string]any)
	if primary["state"] != "DESTROY_SCHEDULED" {
		t.Fatalf("primary.state = %v, want DESTROY_SCHEDULED", primary["state"])
	}

	// Once destroy_time passes, a read promotes the primary to DESTROYED and
	// encryption still fails.
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(kmsstore.DefaultDestroyScheduledDuration + time.Second)})
	resp, err = p.CryptoKeyGet(ctx, newNR(map[string]any{"name": keyName}))
	if err != nil {
		t.Fatalf("get after window: %v", err)
	}
	primary, _ = resp.Data["primary"].(map[string]any)
	if primary["state"] != "DESTROYED" {
		t.Fatalf("primary.state after window = %v, want DESTROYED", primary["state"])
	}
	if _, err := p.CryptoKeyEncrypt(ctx, newNR(map[string]any{"name": keyName, "body": map[string]any{"plaintext": "aGVsbG8="}})); err == nil {
		t.Fatal("expected encrypt against promoted DESTROYED primary to fail")
	}
}

// TestKeyRingIamPolicy covers the REST keyring IAM surface: getIamPolicy
// returns a Policy with a non-empty etag (the real GCP divergence this fixes),
// setIamPolicy persists bindings, a missing keyring 404s, and
// testIamPermissions grants the requested permissions.
func TestKeyRingIamPolicy(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	nr := newNR(map[string]any{"location": "global", "keyRingId": "iam-kr"})
	if _, err := p.KeyRingCreate(ctx, nr); err != nil {
		t.Fatalf("keyring create: %v", err)
	}
	krName := "locations/global/keyRings/iam-kr"

	get, err := p.GetIamPolicy(ctx, newNR(map[string]any{"name": krName}))
	if err != nil {
		t.Fatalf("getIamPolicy: %v", err)
	}
	if etag, _ := get.Data["etag"].(string); etag == "" {
		t.Fatalf("getIamPolicy returned no etag: %#v", get.Data)
	}
	// An empty policy renders as just {"etag": ...} (no version/bindings),
	// matching the real Cloud KMS REST response.
	if _, ok := get.Data["bindings"]; ok {
		t.Fatalf("empty getIamPolicy must omit bindings: %#v", get.Data)
	}
	if _, ok := get.Data["version"]; ok {
		t.Fatalf("empty getIamPolicy must omit version: %#v", get.Data)
	}

	set, err := p.SetIamPolicy(ctx, newNR(map[string]any{
		"name": krName,
		"body": map[string]any{"policy": map[string]any{
			"bindings": []any{map[string]any{
				"role":    "roles/cloudkms.cryptoKeyEncrypter",
				"members": []any{"user:a@example.com"},
			}},
		}},
	}))
	if err != nil {
		t.Fatalf("setIamPolicy: %v", err)
	}
	if bindings, _ := set.Data["bindings"].([]any); len(bindings) != 1 {
		t.Fatalf("setIamPolicy bindings = %#v, want 1", set.Data["bindings"])
	}

	// A stale etag must be rejected (ABORTED / HTTP 409).
	if _, err := p.SetIamPolicy(ctx, newNR(map[string]any{
		"name": krName,
		"body": map[string]any{"policy": map[string]any{"etag": "stale", "bindings": []any{}}},
	})); errStatus(err) != 409 {
		t.Fatalf("stale etag: got %v, want 409", err)
	}

	if _, err := p.GetIamPolicy(ctx, newNR(map[string]any{"name": "locations/global/keyRings/missing"})); errStatus(err) != 404 {
		t.Fatalf("missing keyring getIamPolicy: got %v, want 404", err)
	}

	tp, err := p.TestIamPermissions(ctx, newNR(map[string]any{
		"name": krName,
		"body": map[string]any{"permissions": []any{"cloudkms.cryptoKeys.get", "cloudkms.cryptoKeys.list"}},
	}))
	if err != nil {
		t.Fatalf("testIamPermissions: %v", err)
	}
	if perms, _ := tp.Data["permissions"].([]string); len(perms) != 2 {
		t.Fatalf("testIamPermissions = %#v, want 2", tp.Data["permissions"])
	}
}

// TestPrimaryVersionTimes verifies a crypto key's primary reports createTime and
// generateTime on create, get and list.
func TestPrimaryVersionTimes(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "times-kr"})); err != nil {
		t.Fatalf("keyring create: %v", err)
	}
	created, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
		"name":        "locations/global/keyRings/times-kr",
		"cryptoKeyId": "times-key",
		"body":        map[string]any{"purpose": "ENCRYPT_DECRYPT"},
	}))
	if err != nil {
		t.Fatalf("cryptokey create: %v", err)
	}

	assertTimes := func(where string, data map[string]any) {
		t.Helper()
		primary, _ := data["primary"].(map[string]any)
		ct, _ := primary["createTime"].(string)
		gt, _ := primary["generateTime"].(string)
		if ct == "" || gt == "" {
			t.Fatalf("%s: primary = %#v, want createTime/generateTime", where, data["primary"])
		}
	}
	assertTimes("create", created.Data)

	got, err := p.CryptoKeyGet(ctx, newNR(map[string]any{"name": "locations/global/keyRings/times-kr/cryptoKeys/times-key"}))
	if err != nil {
		t.Fatalf("cryptokey get: %v", err)
	}
	assertTimes("get", got.Data)

	list, err := p.CryptoKeyList(ctx, newNR(map[string]any{"name": "locations/global/keyRings/times-kr/cryptoKeys"}))
	if err != nil {
		t.Fatalf("cryptokey list: %v", err)
	}
	items, _ := list.Data["cryptoKeys"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected 1 crypto key, got %d", len(items))
	}
	first, _ := items[0].(map[string]any)
	assertTimes("list", first)
}

// TestCryptoKeyVersionUpdate pins cryptoKeyVersions.patch as the real KMS way
// to change a version's state (there is no :disable/:enable custom method), and
// that version-level IAM is rejected (real KMS scopes IAM to key rings/keys).
func TestCryptoKeyVersionUpdate(t *testing.T) {
	ctx := context.Background()
	p := newTestProvider()

	if _, err := p.KeyRingCreate(ctx, newNR(map[string]any{"location": "global", "keyRingId": "kr"})); err != nil {
		t.Fatalf("keyring create: %v", err)
	}
	if _, err := p.CryptoKeyCreate(ctx, newNR(map[string]any{
		"name": "locations/global/keyRings/kr", "cryptoKeyId": "k",
		"body": map[string]any{"purpose": "ENCRYPT_DECRYPT"},
	})); err != nil {
		t.Fatalf("cryptokey create: %v", err)
	}
	verName := "locations/global/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/1"

	// cryptoKeyVersions.patch to DISABLED (update mask names state).
	resp, err := p.CryptoKeyVersionUpdate(ctx, newNR(map[string]any{
		"name": verName, "updateMask": "state",
		"body": map[string]any{"state": "DISABLED"},
	}))
	if err != nil {
		t.Fatalf("update to DISABLED: %v", err)
	}
	if got, _ := resp.Data["state"].(string); got != "DISABLED" {
		t.Fatalf("state = %q, want DISABLED", got)
	}
	if got, _ := resp.Data["name"].(string); got != "projects/proj/locations/global/keyRings/kr/cryptoKeys/k/cryptoKeyVersions/1" {
		t.Fatalf("name = %q, want full version resource name", got)
	}

	// Back to ENABLED with no update mask.
	resp, err = p.CryptoKeyVersionUpdate(ctx, newNR(map[string]any{
		"name": verName, "body": map[string]any{"state": "ENABLED"},
	}))
	if err != nil {
		t.Fatalf("update to ENABLED: %v", err)
	}
	if got, _ := resp.Data["state"].(string); got != "ENABLED" {
		t.Fatalf("state = %q, want ENABLED", got)
	}

	// Unsupported mask, unsupported state, and a missing state are all 400.
	if _, err := p.CryptoKeyVersionUpdate(ctx, newNR(map[string]any{
		"name": verName, "updateMask": "algorithm", "body": map[string]any{"state": "DISABLED"},
	})); errStatus(err) != 400 {
		t.Fatalf("bad mask: status = %d (%v), want 400", errStatus(err), err)
	}
	if _, err := p.CryptoKeyVersionUpdate(ctx, newNR(map[string]any{
		"name": verName, "body": map[string]any{"state": "DESTROYED"},
	})); errStatus(err) != 400 {
		t.Fatalf("bad state: status = %d (%v), want 400", errStatus(err), err)
	}
	if _, err := p.CryptoKeyVersionUpdate(ctx, newNR(map[string]any{
		"name": verName, "body": map[string]any{},
	})); errStatus(err) != 400 {
		t.Fatalf("missing state: status = %d (%v), want 400", errStatus(err), err)
	}

	// Version-level IAM is not a real KMS resource.
	if _, err := p.GetIamPolicy(ctx, newNR(map[string]any{"name": verName})); errStatus(err) != 400 {
		t.Fatalf("version getIamPolicy: status = %d (%v), want 400", errStatus(err), err)
	}
}
