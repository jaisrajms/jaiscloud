package secretmanager

import (
	"context"
	"encoding/base64"
	"strconv"
	"sync"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/crypto"
	"jaiscloud/internal/gcp/store/kms"
	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func errStatus(err error) int {
	if pe, ok := err.(*model.ProviderError); ok {
		return pe.HTTPStatus
	}
	return 0
}

func TestSecretNegativesAndPagination(t *testing.T) {
	ctx := context.Background()
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))

	for _, id := range []string{"a", "b", "c"} {
		nr := newNR(map[string]any{"secretId": id})
		if _, err := p.Create(ctx, nr); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}

	// Pagination.
	nr := newNR(map[string]any{"pageSize": "2"})
	resp, err := p.List(ctx, nr)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	secrets, _ := resp.Data["secrets"].([]any)
	if len(secrets) != 2 {
		t.Fatalf("page 1 expected 2 secrets, got %d", len(secrets))
	}
	if resp.Data["totalSize"] != 3 {
		t.Errorf("expected totalSize 3 (total across pages), got %v", resp.Data["totalSize"])
	}
	token, _ := resp.Data["nextPageToken"].(string)
	if token == "" {
		t.Fatal("expected nextPageToken")
	}
	nr = newNR(map[string]any{"pageSize": "2", "pageToken": token})
	resp, err = p.List(ctx, nr)
	if err != nil {
		t.Fatalf("list page 2: %v", err)
	}
	secrets, _ = resp.Data["secrets"].([]any)
	if len(secrets) != 1 {
		t.Fatalf("page 2 expected 1 secret, got %d", len(secrets))
	}

	// 409 duplicate.
	nr = newNR(map[string]any{"secretId": "a"})
	if _, err := p.Create(ctx, nr); err == nil || errStatus(err) != 409 {
		t.Fatalf("expected 409 on duplicate secret, got %v", err)
	}

	// 404 get missing.
	nr = newNR(map[string]any{"name": "secrets/missing"})
	if _, err := p.Get(ctx, nr); err == nil || errStatus(err) != 404 {
		t.Fatalf("expected 404 on missing secret, got %v", err)
	}

	// Access response includes dataCrc32c.
	nr = newNR(map[string]any{"name": "secrets/a", "body": map[string]any{"payload": map[string]any{"data": "aGVsbG8="}}})
	if _, err := p.AddVersion(ctx, nr); err != nil {
		t.Fatalf("addVersion: %v", err)
	}
	nr = newNR(map[string]any{"name": "secrets/a/versions/1"})
	resp, err = p.Access(ctx, nr)
	if err != nil {
		t.Fatalf("access: %v", err)
	}
	payload, _ := resp.Data["payload"].(map[string]any)
	if _, ok := payload["dataCrc32c"]; !ok {
		t.Error("expected dataCrc32c in access payload")
	}

	// 400 on missing name.
	nr = newNR(nil)
	if _, err := p.Get(ctx, nr); err == nil || errStatus(err) != 400 {
		t.Fatalf("expected 400 on missing name, got %v", err)
	}
}

// TestSecretVersionLifecycle verifies disable/enable/destroy transitions apply
// and are reflected in the returned version state.
func TestSecretVersionLifecycle(t *testing.T) {
	ctx := context.Background()
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))

	nr := newNR(map[string]any{"secretId": "s"})
	if _, err := p.Create(ctx, nr); err != nil {
		t.Fatalf("create: %v", err)
	}
	nr = newNR(map[string]any{"name": "secrets/s", "body": map[string]any{"payload": map[string]any{"data": "aGk="}}})
	if _, err := p.AddVersion(ctx, nr); err != nil {
		t.Fatalf("addVersion: %v", err)
	}

	// Deterministic lifecycle (DESTROYED is terminal, so order matters).
	nr = newNR(map[string]any{"name": "secrets/s/versions/1"})
	steps := []struct {
		state string
		fn    func(context.Context, *model.NormalizedRequest) (*model.ProviderResponse, error)
	}{
		{"DISABLED", p.DisableVersion},
		{"ENABLED", p.EnableVersion},
		{"DESTROYED", p.DestroyVersion},
	}
	for _, step := range steps {
		resp, err := step.fn(ctx, nr)
		if err != nil {
			t.Fatalf("%s: %v", step.state, err)
		}
		if resp.Data["state"] != step.state {
			t.Errorf("expected state %s, got %v", step.state, resp.Data["state"])
		}
	}
	// A destroyed version cannot be re-enabled.
	if _, err := p.EnableVersion(ctx, nr); err == nil {
		t.Error("expected error re-enabling a destroyed version")
	}

	// 404 on a missing version.
	nr = newNR(map[string]any{"name": "secrets/s/versions/99"})
	if _, err := p.DisableVersion(ctx, nr); err == nil || errStatus(err) != 404 {
		t.Fatalf("expected 404 on missing version, got %v", err)
	}
}

// TestSecretAddVersionConcurrent verifies concurrent AddVersion calls allocate
// distinct version numbers (no lost or colliding versions).
func TestSecretAddVersionConcurrent(t *testing.T) {
	ctx := context.Background()
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))

	if _, err := p.Create(ctx, newNR(map[string]any{"secretId": "s"})); err != nil {
		t.Fatalf("create: %v", err)
	}

	const n = 50
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			nr := newNR(map[string]any{"name": "secrets/s", "body": map[string]any{"payload": map[string]any{"data": "aGk="}}})
			if _, err := p.AddVersion(ctx, nr); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("addVersion: %v", err)
	}

	versions, err := p.secrets.ListVersions(ctx, "proj", "s")
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	if len(versions) != n {
		t.Fatalf("expected %d versions, got %d", n, len(versions))
	}
}

// TestSecretUpdateConcurrentWithAddVersion is the regression test for the
// version-counter corruption bug: Update used to Get the secret, merge
// request fields (including the NextVer it just read) into a full copy, then
// write that whole copy back with a separate UpdateSecret call. If a
// concurrent AddVersion's NextVersion() advanced the counter in the window
// between Update's read and its write, Update's stale write silently rolled
// the counter back — a subsequent AddVersion would then reuse an
// already-issued version ID and CreateVersion would silently overwrite that
// version's stored payload. Fixed by routing Update through
// UpdateSecretAtomic, which holds the store's lock for the whole
// read-merge-write sequence so NextVersion() can't land in the middle of it.
//
// This asserts the property that actually matters: with N concurrent
// AddVersion calls interleaved with N concurrent Update calls (patching an
// unrelated field, labels), every AddVersion must produce a distinct version
// ID — no two ever collide on the same ID, which is exactly what a rolled-
// back counter would cause.
func TestSecretUpdateConcurrentWithAddVersion(t *testing.T) {
	ctx := context.Background()
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))

	if _, err := p.Create(ctx, newNR(map[string]any{"secretId": "s"})); err != nil {
		t.Fatalf("create: %v", err)
	}

	const n = 50
	var wg sync.WaitGroup
	errs := make(chan error, 2*n)
	for i := 0; i < n; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			nr := newNR(map[string]any{"name": "secrets/s", "body": map[string]any{"payload": map[string]any{"data": "aGk="}}})
			if _, err := p.AddVersion(ctx, nr); err != nil {
				errs <- err
			}
		}(i)
		go func(i int) {
			defer wg.Done()
			nr := newNR(map[string]any{
				"name": "secrets/s",
				"body": map[string]any{"labels": map[string]any{"iteration": strconv.Itoa(i)}},
			})
			if _, err := p.Update(ctx, nr); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent AddVersion/Update: %v", err)
	}

	versions, err := p.secrets.ListVersions(ctx, "proj", "s")
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	if len(versions) != n {
		t.Fatalf("expected %d distinct versions (a rolled-back counter would cause fewer, via silent ID collision), got %d", n, len(versions))
	}
	seen := make(map[string]bool, n)
	for _, v := range versions {
		if seen[v.VersionID] {
			t.Fatalf("duplicate version ID %q — the counter was rolled back and two AddVersion calls collided", v.VersionID)
		}
		seen[v.VersionID] = true
	}
}

// TestSecretIamPolicy verifies secret getIamPolicy/setIamPolicy/testIamPermissions.
func TestSecretIamPolicy(t *testing.T) {
	ctx := context.Background()
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))

	if _, err := p.Create(ctx, newNR(map[string]any{"secretId": "s"})); err != nil {
		t.Fatalf("create: %v", err)
	}

	resp, err := p.GetIamPolicy(ctx, newNR(map[string]any{"name": "secrets/s"}))
	if err != nil {
		t.Fatalf("getIamPolicy: %v", err)
	}
	if resp.Data["version"] != 1 {
		t.Errorf("expected version 1, got %v", resp.Data["version"])
	}

	bindings := []any{map[string]any{"role": "roles/secretmanager.secretAccessor", "members": []any{"allUsers"}}}
	if _, err := p.SetIamPolicy(ctx, newNR(map[string]any{"name": "secrets/s", "body": map[string]any{"bindings": bindings, "etag": "BOGUS="}})); err == nil || errStatus(err) != 409 {
		t.Fatalf("expected 409 on stale etag, got %v", err)
	}
	if _, err := p.SetIamPolicy(ctx, newNR(map[string]any{"name": "secrets/s", "body": map[string]any{"bindings": bindings}})); err != nil {
		t.Fatalf("setIamPolicy: %v", err)
	}

	tr, err := p.TestIamPermissions(ctx, newNR(map[string]any{"name": "secrets/s", "body": map[string]any{"permissions": []any{"secretmanager.secrets.access"}}}))
	if err != nil {
		t.Fatalf("testIamPermissions: %v", err)
	}
	if perms, _ := tr.Data["permissions"].([]string); len(perms) != 1 {
		t.Errorf("expected 1 granted permission, got %v", tr.Data["permissions"])
	}

	if _, err := p.GetIamPolicy(ctx, newNR(map[string]any{"name": "secrets/missing"})); err == nil || errStatus(err) != 404 {
		t.Fatalf("expected 404 on missing secret, got %v", err)
	}
}

// TestSecretRotationAndAliases verifies GCP-native rotation + version aliases
// (no AWS version stages): they persist round-trip, and a due rotation advances
// nextRotationTime without creating a version. Real Secret Manager's rotation
// schedule only publishes a SECRET_ROTATE notification; adding the version is
// the subscriber's job (DC56), so an automatic reschedule must never fabricate
// one — the emulator used to add an empty, unencrypted version here.
func TestSecretRotationAndAliases(t *testing.T) {
	ctx := context.Background()
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))

	// Freeze the clock: a rotation schedule must be at least five minutes in
	// the future when it is set (DC65), so a due schedule is produced by moving
	// time forward rather than by creating one in the past.
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })

	// Create with aliases (rotation unset).
	create := newNR(map[string]any{
		"secretId": "s",
		"body":     map[string]any{"versionAliases": map[string]any{"latest": float64(1)}},
	})
	resp, err := p.Create(ctx, create)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if aliases, _ := resp.Data["versionAliases"].(map[string]int); aliases["latest"] != 1 {
		t.Errorf("expected versionAliases latest=1, got %v", resp.Data["versionAliases"])
	}

	// Add a real version 1.
	if _, err := p.AddVersion(ctx, newNR(map[string]any{"name": "secrets/s", "body": map[string]any{"payload": map[string]any{"data": "aGk="}}})); err != nil {
		t.Fatalf("addVersion: %v", err)
	}

	// Enable rotation, due in an hour, and update the alias to the new version.
	due := t0.Add(time.Hour)
	upd := newNR(map[string]any{"name": "secrets/s", "body": map[string]any{
		"topics":         []any{map[string]any{"name": "projects/proj/topics/rot"}},
		"rotation":       map[string]any{"nextRotationTime": due.Format(time.RFC3339), "rotationPeriod": "3600s"},
		"versionAliases": map[string]any{"latest": float64(2)},
	}})
	resp, err = p.Update(ctx, upd)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if resp.Data["rotation"] == nil {
		t.Error("expected rotation in update response")
	}

	// Get once the schedule is due triggers lazy rotation.
	clock.SetGlobalClock(clock.FixedClock{T: due.Add(time.Minute)})
	resp, err = p.Get(ctx, newNR(map[string]any{"name": "secrets/s"}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	rot, _ := resp.Data["rotation"].(map[string]any)
	if rot["nextRotationTime"] == due.Format(time.RFC3339) {
		t.Error("expected nextRotationTime to advance after rotation")
	}

	// Rotation did not add a version: the secret still has only version 1.
	versions, err := p.secrets.ListVersions(ctx, "proj", "s")
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	if len(versions) != 1 || versions[0].VersionID != "1" {
		t.Fatalf("versions after rotation = %#v, want only version 1", versions)
	}
	if _, err := p.secrets.GetVersion(ctx, "proj", "s", "2"); err == nil {
		t.Fatal("rotation created a version 2, want notification-only rotation")
	}
}

// TestSecretRotationNotDue verifies a future rotation does not create a version.
func TestSecretRotationNotDue(t *testing.T) {
	ctx := context.Background()
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))

	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })

	create := newNR(map[string]any{
		"secretId": "s",
		"body": map[string]any{
			"topics": []any{map[string]any{"name": "projects/proj/topics/rot"}},
			"rotation": map[string]any{
				"nextRotationTime": t0.Add(24 * time.Hour).Format(time.RFC3339),
				"rotationPeriod":   "3600s",
			},
		},
	})
	if _, err := p.Create(ctx, create); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := p.Get(ctx, newNR(map[string]any{"name": "secrets/s"})); err != nil {
		t.Fatalf("get: %v", err)
	}
	if _, err := p.secrets.GetVersion(ctx, "proj", "s", "2"); err == nil {
		t.Fatal("expected no rotated version when not due")
	}
}

func TestSecretManagerCMEKRoundTrip(t *testing.T) {
	ctx := context.Background()
	kmsStore := kms.NewMemoryStore()
	enc := crypto.NewEnvelopeEncryptor(kmsStore)
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore(), enc)

	// Create KMS KeyRing & CryptoKey
	projectID := "my-project"
	location := "global"
	keyringID := "my-keyring"
	keyID := "my-key"
	err := kmsStore.CreateKeyRing(ctx, projectID, location, keyringID, kms.KeyRing{ID: keyringID})
	if err != nil {
		t.Fatal(err)
	}
	err = kmsStore.CreateCryptoKey(ctx, projectID, location, keyringID, keyID, kms.CryptoKey{ID: keyID, Purpose: "ENCRYPT_DECRYPT"})
	if err != nil {
		t.Fatal(err)
	}
	version, err := kmsStore.CreateVersion(ctx, projectID, location, keyringID, keyID, kms.Version{State: "ENABLED"})
	if err != nil {
		t.Fatal(err)
	}
	err = kmsStore.UpdatePrimaryVersion(ctx, projectID, location, keyringID, keyID, version)
	if err != nil {
		t.Fatal(err)
	}
	kmsKeyName := "projects/" + projectID + "/locations/" + location + "/keyRings/" + keyringID + "/cryptoKeys/" + keyID

	// Create Secret with CMEK
	createNR := newNR(map[string]any{
		"secretId": "cmek-secret",
		"body": map[string]any{
			"replication": map[string]any{
				"automatic": map[string]any{
					"customerManagedEncryption": map[string]any{
						"kmsKeyName": kmsKeyName,
					},
				},
			},
		},
	})
	createNR.AccountID = projectID

	created, err := p.Create(ctx, createNR)
	if err != nil {
		t.Fatalf("create secret: %v", err)
	}
	// The Secret response must echo the configured CMEK key (DC31): a client
	// reads back what it set under
	// replication.automatic.customerManagedEncryption.kmsKeyName.
	if got := cmekFromSecretMap(t, created.Data); got != kmsKeyName {
		t.Fatalf("create replication CMEK = %q, want %q (replication=%#v)", got, kmsKeyName, created.Data["replication"])
	}
	if got := cmekFromSecretMap(t, mustGetSecret(t, p, ctx, projectID, "cmek-secret").Data); got != kmsKeyName {
		t.Fatalf("get replication CMEK = %q, want %q", got, kmsKeyName)
	}

	// Add Version
	payloadB64 := "aGVsbG8sIHdvcmxk" // "hello, world"
	addNR := newNR(map[string]any{
		"name": "projects/" + projectID + "/secrets/cmek-secret",
		"body": map[string]any{
			"payload": map[string]any{
				"data": payloadB64,
			},
		},
	})
	addNR.AccountID = projectID

	addResp, err := p.AddVersion(ctx, addNR)
	if err != nil {
		t.Fatalf("add version: %v", err)
	}
	versionName := addResp.Data["name"].(string)

	// The version reports the exact CryptoKeyVersion that wrapped its payload
	// under replicationStatus.automatic.customerManagedEncryption
	// (DC44). The wrapping version is the key's primary at wrap time.
	wantKeyVersion := kmsKeyName + "/versions/" + version
	if got := cmekVersionFromVersionMap(t, addResp.Data); got != wantKeyVersion {
		t.Fatalf("AddVersion kmsKeyVersionName = %q, want %q", got, wantKeyVersion)
	}
	gotVersionNR := newNR(map[string]any{"name": versionName})
	gotVersionNR.AccountID = projectID
	gotVersion, err := p.GetVersion(ctx, gotVersionNR)
	if err != nil {
		t.Fatalf("get version: %v", err)
	}
	if got := cmekVersionFromVersionMap(t, gotVersion.Data); got != wantKeyVersion {
		t.Fatalf("GetVersion kmsKeyVersionName = %q, want %q", got, wantKeyVersion)
	}

	// Access
	accessNR := newNR(map[string]any{
		"name": versionName,
	})
	accessNR.AccountID = projectID

	accessResp, err := p.Access(ctx, accessNR)
	if err != nil {
		t.Fatalf("access version: %v", err)
	}

	payloadResp := accessResp.Data["payload"].(map[string]any)
	if payloadResp["data"] != payloadB64 {
		t.Fatalf("expected payload %q, got %q", payloadB64, payloadResp["data"])
	}

	// Patching the replication policy is observable too (DC31): the Secret etag
	// already covered kmsKeyName (DC13), so before this fix a CMEK change moved
	// the etag while the rendered resource stayed byte-identical.
	key2ID := "second-key"
	if err := kmsStore.CreateCryptoKey(ctx, projectID, location, keyringID, key2ID, kms.CryptoKey{ID: key2ID, Purpose: "ENCRYPT_DECRYPT"}); err != nil {
		t.Fatal(err)
	}
	v2, err := kmsStore.CreateVersion(ctx, projectID, location, keyringID, key2ID, kms.Version{State: "ENABLED"})
	if err != nil {
		t.Fatal(err)
	}
	if err := kmsStore.UpdatePrimaryVersion(ctx, projectID, location, keyringID, key2ID, v2); err != nil {
		t.Fatal(err)
	}
	key2Name := "projects/" + projectID + "/locations/" + location + "/keyRings/" + keyringID + "/cryptoKeys/" + key2ID

	patchNR := newNR(map[string]any{
		"name": "projects/" + projectID + "/secrets/cmek-secret",
		"body": map[string]any{
			"replication": map[string]any{
				"automatic": map[string]any{
					"customerManagedEncryption": map[string]any{"kmsKeyName": key2Name},
				},
			},
		},
	})
	patchNR.AccountID = projectID
	patched, err := p.Update(ctx, patchNR)
	if err != nil {
		t.Fatalf("patch replication: %v", err)
	}
	if got := cmekFromSecretMap(t, patched.Data); got != key2Name {
		t.Fatalf("patch replication CMEK = %q, want %q", got, key2Name)
	}
	if got := cmekFromSecretMap(t, mustGetSecret(t, p, ctx, projectID, "cmek-secret").Data); got != key2Name {
		t.Fatalf("get replication CMEK after patch = %q, want %q", got, key2Name)
	}

	// The patched secret is also readable through list.
	listNR := newNR(map[string]any{})
	listNR.AccountID = projectID
	list, err := p.List(ctx, listNR)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	items, _ := list.Data["secrets"].([]any)
	if len(items) != 1 {
		t.Fatalf("list returned %d secrets, want 1", len(items))
	}
	if got := cmekFromSecretMap(t, items[0].(map[string]any)); got != key2Name {
		t.Fatalf("list replication CMEK = %q, want %q", got, key2Name)
	}

	// A version's status is per-version, not the secret's current policy: after
	// patching to key2, a new version reports key2's wrapping version, while the
	// first version (wrapped with key1) keeps the key version that actually
	// wrapped it. Rotating key2's primary afterwards must likewise only affect
	// versions added later — the stored wrap-time tag is what makes this exact.
	addVersion := func(payload string) map[string]any {
		t.Helper()
		nr := newNR(map[string]any{
			"name": "projects/" + projectID + "/secrets/cmek-secret",
			"body": map[string]any{"payload": map[string]any{"data": payload}},
		})
		nr.AccountID = projectID
		resp, err := p.AddVersion(ctx, nr)
		if err != nil {
			t.Fatalf("add version: %v", err)
		}
		return resp.Data
	}

	// key2 was created with version v2 as its primary (above).
	second := addVersion("c2Vjb25k")
	if got, want := cmekVersionFromVersionMap(t, second), key2Name+"/versions/"+v2; got != want {
		t.Fatalf("version after patching to key2 kmsKeyVersionName = %q, want %q", got, want)
	}

	// Rotate key2's primary to a fresh version, then add another version.
	rotated, err := kmsStore.CreateVersion(ctx, projectID, location, keyringID, key2ID, kms.Version{State: "ENABLED"})
	if err != nil {
		t.Fatal(err)
	}
	if err := kmsStore.UpdatePrimaryVersion(ctx, projectID, location, keyringID, key2ID, rotated); err != nil {
		t.Fatal(err)
	}
	third := addVersion("dGhpcmQ=")
	if got, want := cmekVersionFromVersionMap(t, third), key2Name+"/versions/"+rotated; got != want {
		t.Fatalf("version after primary rotation kmsKeyVersionName = %q, want %q", got, want)
	}

	// The earlier version still reports the CryptoKeyVersion that wrapped it,
	// and the pre-patch version still reports key1's version.
	secondName, _ := second["name"].(string)
	nr2 := newNR(map[string]any{"name": secondName})
	nr2.AccountID = projectID
	stillSecond, err := p.GetVersion(ctx, nr2)
	if err != nil {
		t.Fatalf("get second version: %v", err)
	}
	if got, want := cmekVersionFromVersionMap(t, stillSecond.Data), key2Name+"/versions/"+v2; got != want {
		t.Fatalf("second version after rotation kmsKeyVersionName = %q, want %q", got, want)
	}
	nr1 := newNR(map[string]any{"name": versionName})
	nr1.AccountID = projectID
	stillFirst, err := p.GetVersion(ctx, nr1)
	if err != nil {
		t.Fatalf("get first version: %v", err)
	}
	if got := cmekVersionFromVersionMap(t, stillFirst.Data); got != wantKeyVersion {
		t.Fatalf("first version after patch kmsKeyVersionName = %q, want %q", got, wantKeyVersion)
	}
}

// TestSecretReplicationWithoutCMEK pins the no-CMEK shape: the automatic arm is
// present and empty, with no customerManagedEncryption object at all (real GCP
// omits the nested message rather than sending it empty).
func TestSecretReplicationWithoutCMEK(t *testing.T) {
	ctx := context.Background()
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))

	resp, err := p.Create(ctx, newNR(map[string]any{"secretId": "plain-secret"}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	repl, ok := resp.Data["replication"].(map[string]any)
	if !ok {
		t.Fatalf("replication = %#v, want an object", resp.Data["replication"])
	}
	auto, ok := repl["automatic"].(map[string]any)
	if !ok {
		t.Fatalf("replication.automatic = %#v, want an object", repl["automatic"])
	}
	if _, present := auto["customerManagedEncryption"]; present {
		t.Fatalf("replication.automatic.customerManagedEncryption = %#v, want absent", auto["customerManagedEncryption"])
	}
	if got := cmekFromSecretMap(t, resp.Data); got != "" {
		t.Fatalf("CMEK = %q, want empty", got)
	}

	// A version of a Google-managed secret reports the automatic arm with no
	// customerManagedEncryption, rather than an empty nested status object.
	addNR := newNR(map[string]any{
		"name": "projects/proj/secrets/plain-secret",
		"body": map[string]any{"payload": map[string]any{"data": "aGk="}},
	})
	addNR.AccountID = "proj"
	addResp, err := p.AddVersion(ctx, addNR)
	if err != nil {
		t.Fatalf("add version: %v", err)
	}
	if got := cmekVersionFromVersionMap(t, addResp.Data); got != "" {
		t.Fatalf("version kmsKeyVersionName = %q, want empty", got)
	}
}

// cmekFromSecretMap returns the CMEK key name from a rendered Secret, or "" when
// the automatic replication policy carries no customerManagedEncryption.
func cmekFromSecretMap(t *testing.T, data map[string]any) string {
	t.Helper()
	repl, ok := data["replication"].(map[string]any)
	if !ok {
		t.Fatalf("replication = %#v, want an object", data["replication"])
	}
	auto, ok := repl["automatic"].(map[string]any)
	if !ok {
		t.Fatalf("replication.automatic = %#v, want an object", repl["automatic"])
	}
	raw, present := auto["customerManagedEncryption"]
	if !present {
		return ""
	}
	cme, ok := raw.(map[string]any)
	if !ok {
		t.Fatalf("customerManagedEncryption = %#v, want an object", raw)
	}
	name, _ := cme["kmsKeyName"].(string)
	return name
}

// cmekVersionFromVersionMap returns the kmsKeyVersionName from a rendered
// SecretVersion's replicationStatus, or "" when the version carries no CMEK
// status (the automatic arm with no customerManagedEncryption).
func cmekVersionFromVersionMap(t *testing.T, data map[string]any) string {
	t.Helper()
	rs, ok := data["replicationStatus"].(map[string]any)
	if !ok {
		t.Fatalf("replicationStatus = %#v, want an object", data["replicationStatus"])
	}
	auto, ok := rs["automatic"].(map[string]any)
	if !ok {
		t.Fatalf("replicationStatus.automatic = %#v, want an object", rs["automatic"])
	}
	raw, present := auto["customerManagedEncryption"]
	if !present {
		return ""
	}
	cme, ok := raw.(map[string]any)
	if !ok {
		t.Fatalf("customerManagedEncryption = %#v, want an object", raw)
	}
	name, _ := cme["kmsKeyVersionName"].(string)
	return name
}

// mustGetSecret reads a secret back through the provider.
func mustGetSecret(t *testing.T, p *Provider, ctx context.Context, project, id string) *model.ProviderResponse {
	t.Helper()
	nr := newNR(map[string]any{"name": "projects/" + project + "/secrets/" + id})
	nr.AccountID = project
	resp, err := p.Get(ctx, nr)
	if err != nil {
		t.Fatalf("get secret %s: %v", id, err)
	}
	return resp
}

// TestSecretVersionStateEnforcement verifies Secret Manager lifecycle rules:
// a DISABLED/DESTROYED version cannot be accessed, DESTROYED is terminal, and
// destroyTime is emitted once destroyed.
func TestSecretVersionStateEnforcement(t *testing.T) {
	ctx := context.Background()
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))

	if _, err := p.Create(ctx, newNR(map[string]any{"secretId": "state-secret"})); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := p.AddVersion(ctx, newNR(map[string]any{
		"name": "secrets/state-secret", "body": map[string]any{"payload": map[string]any{"data": "aGVsbG8="}},
	})); err != nil {
		t.Fatalf("addVersion: %v", err)
	}
	version := "secrets/state-secret/versions/1"

	// DISABLED version is not accessible.
	if _, err := p.DisableVersion(ctx, newNR(map[string]any{"name": version})); err != nil {
		t.Fatalf("disable: %v", err)
	}
	_, err := p.Access(ctx, newNR(map[string]any{"name": version}))
	requireProviderCode(t, err, "FailedPrecondition")

	// Re-enabling restores access.
	if _, err := p.EnableVersion(ctx, newNR(map[string]any{"name": version})); err != nil {
		t.Fatalf("enable: %v", err)
	}
	if _, err := p.Access(ctx, newNR(map[string]any{"name": version})); err != nil {
		t.Fatalf("access after enable: %v", err)
	}

	// Destroy sets DESTROYED and destroyTime.
	resp, err := p.DestroyVersion(ctx, newNR(map[string]any{"name": version}))
	if err != nil {
		t.Fatalf("destroy: %v", err)
	}
	if resp.Data["state"] != "DESTROYED" {
		t.Fatalf("state = %v, want DESTROYED", resp.Data["state"])
	}
	if dt, _ := resp.Data["destroyTime"].(string); dt == "" {
		t.Fatalf("expected destroyTime on destroyed version, got %v", resp.Data["destroyTime"])
	}

	// DESTROYED is terminal.
	_, err = p.Access(ctx, newNR(map[string]any{"name": version}))
	requireProviderCode(t, err, "FailedPrecondition")
	_, err = p.EnableVersion(ctx, newNR(map[string]any{"name": version}))
	requireProviderCode(t, err, "FailedPrecondition")
	_, err = p.DisableVersion(ctx, newNR(map[string]any{"name": version}))
	requireProviderCode(t, err, "FailedPrecondition")
}

// TestSecretIAMRoutesRegistered pins the provider-side keys for the REST IAM
// surface: the codec's bare GetIamPolicy/SetIamPolicy/TestIamPermissions actions
// combine with the "Secret" service prefix to reach these.
func TestSecretIAMRoutesRegistered(t *testing.T) {
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))
	routes := p.Routes()
	for _, k := range []string{"Secret.GetIamPolicy", "Secret.SetIamPolicy", "Secret.TestIamPermissions"} {
		if routes[k] == nil {
			t.Errorf("missing route %q", k)
		}
	}
}

func requireProviderCode(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error %s, got nil", code)
	}
	pe, ok := err.(*model.ProviderError)
	if !ok {
		t.Fatalf("expected *model.ProviderError, got %T (%v)", err, err)
	}
	if pe.Code != code {
		t.Fatalf("expected code %s, got %s (%v)", code, pe.Code, err)
	}
}

// TestVersionMapEtagAndReplicationStatus verifies the SecretVersion response
// carries a non-empty etag (bumped on state change) and the automatic
// replicationStatus object real GCP returns.
func TestVersionMapEtagAndReplicationStatus(t *testing.T) {
	enabled := versionToMap(versionMeta{
		Name:       "projects/p/secrets/s/versions/1",
		State:      "ENABLED",
		CreateTime: "2026-01-01T00:00:00Z",
	}, nil)
	if etag, _ := enabled["etag"].(string); etag == "" {
		t.Fatalf("version map has no etag: %#v", enabled)
	}
	rs, _ := enabled["replicationStatus"].(map[string]any)
	if _, ok := rs["automatic"]; !ok {
		t.Fatalf("replicationStatus = %#v, want automatic", enabled["replicationStatus"])
	}

	disabled := versionToMap(versionMeta{
		Name:       "projects/p/secrets/s/versions/1",
		State:      "DISABLED",
		CreateTime: "2026-01-01T00:00:00Z",
	}, nil)
	if enabled["etag"] == disabled["etag"] {
		t.Errorf("etag should change with lifecycle state")
	}
}

// TestManagedRotation covers the Cloud SQL managed-rotation surface:
// EnableManagedRotation records the credentials + ACTIVE status and stores a
// generated password as version 1; RotateSecret adds version 2; the preconditions
// (rotate-before-enable, double-enable) fail FailedPrecondition; and the stored
// password round-trips through Access.
func TestManagedRotation(t *testing.T) {
	ctx := context.Background()
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))

	const name = "projects/proj/secrets/managed"
	if _, err := p.Create(ctx, newNR(map[string]any{"secretId": "managed"})); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Rotate before managed rotation is enabled → FailedPrecondition.
	if _, err := p.RotateSecret(ctx, newNR(map[string]any{"name": name})); err != nil {
		requireProviderCode(t, err, "FailedPrecondition")
	} else {
		t.Fatal("RotateSecret before enable returned nil, want FailedPrecondition")
	}

	// Enable without credentials → InvalidRequest.
	if _, err := p.EnableManagedRotation(ctx, newNR(map[string]any{"name": name, "body": map[string]any{}})); err != nil {
		requireProviderCode(t, err, "InvalidRequest")
	} else {
		t.Fatal("EnableManagedRotation without credentials returned nil, want InvalidRequest")
	}

	// Enable → version 1, status ACTIVE.
	creds := map[string]any{"instanceId": "inst", "username": "app"}
	resp, err := p.EnableManagedRotation(ctx, newNR(map[string]any{
		"name": name,
		"body": map[string]any{"cloudSqlSingleUserCredentials": creds},
	}))
	if err != nil {
		t.Fatalf("EnableManagedRotation: %v", err)
	}
	if got, _ := resp.Data["name"].(string); got != name+"/versions/1" {
		t.Fatalf("EnableManagedRotation version = %q, want %q", got, name+"/versions/1")
	}

	get, err := p.Get(ctx, newNR(map[string]any{"name": name}))
	if err != nil {
		t.Fatalf("get after enable: %v", err)
	}
	rot, _ := get.Data["rotation"].(map[string]any)
	mrs, _ := rot["managedRotationStatus"].(map[string]any)
	if mrs["state"] != "ACTIVE" {
		t.Fatalf("managedRotationStatus = %#v, want state ACTIVE", mrs)
	}

	// Enabling twice → FailedPrecondition.
	if _, err := p.EnableManagedRotation(ctx, newNR(map[string]any{
		"name": name,
		"body": map[string]any{"cloudSqlSingleUserCredentials": creds},
	})); err != nil {
		requireProviderCode(t, err, "FailedPrecondition")
	} else {
		t.Fatal("second EnableManagedRotation returned nil, want FailedPrecondition")
	}

	// Rotate → version 2.
	resp, err = p.RotateSecret(ctx, newNR(map[string]any{"name": name}))
	if err != nil {
		t.Fatalf("RotateSecret: %v", err)
	}
	if got, _ := resp.Data["name"].(string); got != name+"/versions/2" {
		t.Fatalf("RotateSecret version = %q, want %q", got, name+"/versions/2")
	}

	// The generated password round-trips through Access.
	acc, err := p.Access(ctx, newNR(map[string]any{"name": name + "/versions/1"}))
	if err != nil {
		t.Fatalf("access managed version: %v", err)
	}
	payload, _ := acc.Data["payload"].(map[string]any)
	if data, _ := payload["data"].(string); data == "" {
		t.Fatalf("managed version payload = %#v, want a non-empty password", payload)
	}

	// A schedule update must not clobber the managed-rotation status.
	if _, err := p.Update(ctx, newNR(map[string]any{
		"name": name,
		"body": map[string]any{
			"topics": []any{map[string]any{"name": "projects/proj/topics/rot"}},
			"rotation": map[string]any{
				"nextRotationTime": clock.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339),
				"rotationPeriod":   "3600s",
			},
		},
	})); err != nil {
		t.Fatalf("update rotation period: %v", err)
	}
	get, err = p.Get(ctx, newNR(map[string]any{"name": name}))
	if err != nil {
		t.Fatalf("get after update: %v", err)
	}
	rot, _ = get.Data["rotation"].(map[string]any)
	if mrs, _ = rot["managedRotationStatus"].(map[string]any); mrs["state"] != "ACTIVE" {
		t.Fatalf("managedRotationStatus after schedule update = %#v, want ACTIVE", mrs)
	}
}

// TestManagedRotationSuppliedPassword covers the optional Cloud SQL password: a
// supplied password is stored as the version payload (not replaced by a
// generated one).
func TestManagedRotationSuppliedPassword(t *testing.T) {
	ctx := context.Background()
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))

	const name = "projects/proj/secrets/managed-pw"
	if _, err := p.Create(ctx, newNR(map[string]any{"secretId": "managed-pw"})); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := p.EnableManagedRotation(ctx, newNR(map[string]any{
		"name": name,
		"body": map[string]any{"cloudSqlSingleUserCredentials": map[string]any{
			"instanceId": "inst", "username": "app", "password": "supplied-password",
		}},
	})); err != nil {
		t.Fatalf("EnableManagedRotation: %v", err)
	}
	acc, err := p.Access(ctx, newNR(map[string]any{"name": name + "/versions/1"}))
	if err != nil {
		t.Fatalf("access: %v", err)
	}
	payload, _ := acc.Data["payload"].(map[string]any)
	if data, _ := payload["data"].(string); data != base64.StdEncoding.EncodeToString([]byte("supplied-password")) {
		t.Fatalf("stored password = %q, want the supplied password", data)
	}
}

// TestManagedRotationConcurrentReads hammers GetSecret while managed rotation is
// being enabled. Run under -race, it guards against mutating the *Rotation the
// store hands out (which aliases the stored map entry) in place.
func TestManagedRotationConcurrentReads(t *testing.T) {
	ctx := context.Background()
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))

	const name = "projects/proj/secrets/managed-race"
	if _, err := p.Create(ctx, newNR(map[string]any{"secretId": "managed-race"})); err != nil {
		t.Fatalf("create: %v", err)
	}
	// Seed a rotation schedule so reads return a non-nil Rotation.
	if _, err := p.Update(ctx, newNR(map[string]any{
		"name": name,
		"body": map[string]any{
			"topics": []any{map[string]any{"name": "projects/proj/topics/rot"}},
			"rotation": map[string]any{
				"nextRotationTime": clock.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339),
				"rotationPeriod":   "3600s",
			},
		},
	})); err != nil {
		t.Fatalf("seed rotation: %v", err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = p.Get(ctx, newNR(map[string]any{"name": name}))
				}
			}
		}()
	}
	if _, err := p.EnableManagedRotation(ctx, newNR(map[string]any{
		"name": name,
		"body": map[string]any{"cloudSqlSingleUserCredentials": map[string]any{"instanceId": "inst", "username": "app"}},
	})); err != nil {
		t.Fatalf("EnableManagedRotation: %v", err)
	}
	close(stop)
	wg.Wait()
}
