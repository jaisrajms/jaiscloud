package secretmanager

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"

	"jaiscloud/internal/gcp/crypto"
	"jaiscloud/internal/gcp/store/kms"
	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// requireAborted asserts err is the etag-precondition ABORTED/409 error.
func requireAborted(t *testing.T, err error) {
	t.Helper()
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("err = %v, want a *model.ProviderError", err)
	}
	if pe.HTTPStatus != 409 || pe.Status != "ABORTED" {
		t.Fatalf("err = %d/%s, want 409 ABORTED", pe.HTTPStatus, pe.Status)
	}
}

// TestSecretDeleteEtagPrecondition covers the documented `etag` query
// parameter of secrets.delete: a mismatch is ABORTED and leaves the secret in
// place, an omitted etag succeeds, and the etag changes when the secret is
// updated (so "stale" is real, not just a bogus token).
func TestSecretDeleteEtagPrecondition(t *testing.T) {
	ctx := context.Background()
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))

	created, err := p.Create(ctx, newNR(map[string]any{"secretId": "s", "body": map[string]any{}}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	createEtag, _ := created.Data["etag"].(string)
	if createEtag == "" {
		t.Fatal("create response has no etag")
	}

	updated, err := p.Update(ctx, newNR(map[string]any{
		"name": "secrets/s",
		"body": map[string]any{"labels": map[string]any{"env": "prod"}},
	}))
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	newEtag, _ := updated.Data["etag"].(string)
	if newEtag == createEtag {
		t.Fatal("a metadata update must change the secret etag")
	}

	// The etag read before the update is now stale: the delete is rejected and
	// the secret survives.
	if _, err := p.Delete(ctx, newNR(map[string]any{"name": "secrets/s", "etag": createEtag})); err == nil {
		t.Fatal("expected ABORTED on a stale etag")
	} else {
		requireAborted(t, err)
	}
	if _, err := p.Get(ctx, newNR(map[string]any{"name": "secrets/s"})); err != nil {
		t.Fatalf("stale-etag delete removed the secret: %v", err)
	}

	// A random token is rejected too.
	if _, err := p.Delete(ctx, newNR(map[string]any{"name": "secrets/s", "etag": "BOGUS="})); err == nil {
		t.Fatal("expected ABORTED on a bogus etag")
	} else {
		requireAborted(t, err)
	}

	// The current etag passes.
	if _, err := p.Delete(ctx, newNR(map[string]any{"name": "secrets/s", "etag": newEtag})); err != nil {
		t.Fatalf("delete with the current etag: %v", err)
	}

	// An omitted etag is a no-op precondition, as documented.
	if _, err := p.Create(ctx, newNR(map[string]any{"secretId": "noetag", "body": map[string]any{}})); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := p.Delete(ctx, newNR(map[string]any{"name": "secrets/noetag"})); err != nil {
		t.Fatalf("delete without an etag: %v", err)
	}
}

// TestSecretUpdateEtagPrecondition covers Secret.etag on the update path, which
// shares the same documented semantics.
func TestSecretUpdateEtagPrecondition(t *testing.T) {
	ctx := context.Background()
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))

	created, err := p.Create(ctx, newNR(map[string]any{"secretId": "s", "body": map[string]any{}}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	stale, _ := created.Data["etag"].(string)

	first, err := p.Update(ctx, newNR(map[string]any{
		"name": "secrets/s",
		"body": map[string]any{"labels": map[string]any{"a": "1"}},
	}))
	if err != nil {
		t.Fatalf("first update: %v", err)
	}
	fresh, _ := first.Data["etag"].(string)

	if _, err := p.Update(ctx, newNR(map[string]any{
		"name": "secrets/s",
		"body": map[string]any{"labels": map[string]any{"a": "2"}, "etag": stale},
	})); err == nil {
		t.Fatal("expected ABORTED on a stale update etag")
	} else {
		requireAborted(t, err)
	}
	// The rejected write changed nothing.
	got, err := p.Get(ctx, newNR(map[string]any{"name": "secrets/s"}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	labels, _ := got.Data["labels"].(map[string]string)
	if labels["a"] != "1" {
		t.Fatalf("stale-etag update was applied: labels = %v", labels)
	}

	// The fresh etag passes.
	if _, err := p.Update(ctx, newNR(map[string]any{
		"name": "secrets/s",
		"body": map[string]any{"labels": map[string]any{"a": "2"}, "etag": fresh},
	})); err != nil {
		t.Fatalf("update with the current etag: %v", err)
	}
}

// TestVersionStateEtagPrecondition covers the etag on the disable/enable/
// destroy request bodies: a mismatch is ABORTED and the state is unchanged.
func TestVersionStateEtagPrecondition(t *testing.T) {
	ctx := context.Background()
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))

	if _, err := p.Create(ctx, newNR(map[string]any{"secretId": "s", "body": map[string]any{}})); err != nil {
		t.Fatalf("create: %v", err)
	}
	added, err := p.AddVersion(ctx, newNR(map[string]any{
		"name": "secrets/s",
		"body": map[string]any{"payload": map[string]any{"data": "aGVsbG8="}},
	}))
	if err != nil {
		t.Fatalf("addVersion: %v", err)
	}
	enabledEtag, _ := added.Data["etag"].(string)
	if enabledEtag == "" {
		t.Fatal("version response has no etag")
	}

	// Stale/bogus etag must not transition the version.
	if _, err := p.DisableVersion(ctx, newNR(map[string]any{
		"name": "secrets/s/versions/1",
		"body": map[string]any{"etag": "BOGUS="},
	})); err == nil {
		t.Fatal("expected ABORTED on a bogus version etag")
	} else {
		requireAborted(t, err)
	}
	v, err := p.GetVersion(ctx, newNR(map[string]any{"name": "secrets/s/versions/1"}))
	if err != nil {
		t.Fatalf("getVersion: %v", err)
	}
	if v.Data["state"] != "ENABLED" {
		t.Fatalf("rejected disable changed state to %v", v.Data["state"])
	}

	// The current etag passes, and the returned etag is bumped.
	disabled, err := p.DisableVersion(ctx, newNR(map[string]any{
		"name": "secrets/s/versions/1",
		"body": map[string]any{"etag": enabledEtag},
	}))
	if err != nil {
		t.Fatalf("disable with the current etag: %v", err)
	}
	disabledEtag, _ := disabled.Data["etag"].(string)
	if disabledEtag == enabledEtag {
		t.Fatal("the version etag must change on a state transition")
	}
	if disabled.Data["state"] != "DISABLED" {
		t.Fatalf("state = %v, want DISABLED", disabled.Data["state"])
	}

	// The pre-transition etag is stale for the next transition.
	if _, err := p.EnableVersion(ctx, newNR(map[string]any{
		"name": "secrets/s/versions/1",
		"body": map[string]any{"etag": enabledEtag},
	})); err == nil {
		t.Fatal("expected ABORTED on the pre-disable etag")
	} else {
		requireAborted(t, err)
	}
	reenabled, err := p.EnableVersion(ctx, newNR(map[string]any{
		"name": "secrets/s/versions/1",
		"body": map[string]any{"etag": disabledEtag},
	}))
	if err != nil {
		t.Fatalf("enable with the current etag: %v", err)
	}
	if _, err := p.DestroyVersion(ctx, newNR(map[string]any{
		"name": "secrets/s/versions/1",
		"body": map[string]any{"etag": "BOGUS="},
	})); err == nil {
		t.Fatal("expected ABORTED on a bogus destroy etag")
	}
	destroyed, err := p.DestroyVersion(ctx, newNR(map[string]any{
		"name": "secrets/s/versions/1",
		"body": map[string]any{"etag": reenabled.Data["etag"]},
	}))
	if err != nil {
		t.Fatalf("destroy with the current etag: %v", err)
	}
	if destroyed.Data["state"] != "DESTROYED" {
		t.Fatalf("state = %v, want DESTROYED", destroyed.Data["state"])
	}
}

// TestSecretEtagExcludesVersions verifies the Secret etag covers the Secret
// object only: adding a version must not invalidate a caller's etag (the
// pre-AddVersion token still deletes), while the version's own etag is
// independent.
func TestSecretEtagExcludesVersions(t *testing.T) {
	ctx := context.Background()
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))

	created, err := p.Create(ctx, newNR(map[string]any{"secretId": "s", "body": map[string]any{}}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	beforeAdd, _ := created.Data["etag"].(string)

	if _, err := p.AddVersion(ctx, newNR(map[string]any{
		"name": "secrets/s",
		"body": map[string]any{"payload": map[string]any{"data": "aGVsbG8="}},
	})); err != nil {
		t.Fatalf("addVersion: %v", err)
	}

	got, err := p.Get(ctx, newNR(map[string]any{"name": "secrets/s"}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Data["etag"] != beforeAdd {
		t.Fatalf("adding a version changed the Secret etag (%v -> %v)", beforeAdd, got.Data["etag"])
	}
	if _, err := p.Delete(ctx, newNR(map[string]any{"name": "secrets/s", "etag": beforeAdd})); err != nil {
		t.Fatalf("delete with the pre-AddVersion etag: %v", err)
	}
}

// TestVersionStateEtagOmitted verifies an omitted version etag is a no-op
// precondition (the documented opt-out), not a mismatch.
func TestVersionStateEtagOmitted(t *testing.T) {
	ctx := context.Background()
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))

	if _, err := p.Create(ctx, newNR(map[string]any{"secretId": "s", "body": map[string]any{}})); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := p.AddVersion(ctx, newNR(map[string]any{
		"name": "secrets/s",
		"body": map[string]any{"payload": map[string]any{"data": "aGVsbG8="}},
	})); err != nil {
		t.Fatalf("addVersion: %v", err)
	}
	v, err := p.DisableVersion(ctx, newNR(map[string]any{"name": "secrets/s/versions/1"}))
	if err != nil {
		t.Fatalf("disable without an etag: %v", err)
	}
	if v.Data["state"] != "DISABLED" {
		t.Fatalf("state = %v, want DISABLED", v.Data["state"])
	}
}

// TestEtagPreconditionsAreAtomic is the p2 (concurrency) assertion for DC13:
// the precondition check and the write are one atomic store operation, so a
// concurrent mutation can never slip between them and let a stale etag through.
//
// For each iteration two goroutines race:
//   - a metadata update (which changes the secret etag), and
//   - a delete carrying the etag read before the update.
//
// Exactly one of them may succeed: if the update lands first the delete must be
// ABORTED; if the delete lands first the update must 404. A non-atomic
// Get → check → write lets both succeed, which is the bug this pins.
func TestEtagPreconditionsAreAtomic(t *testing.T) {
	ctx := context.Background()
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))

	const iterations = 200
	for i := 0; i < iterations; i++ {
		id := "s" + strconv.Itoa(i)
		created, err := p.Create(ctx, newNR(map[string]any{"secretId": id, "body": map[string]any{}}))
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		etag, _ := created.Data["etag"].(string)

		var wg sync.WaitGroup
		var updateErr, deleteErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, updateErr = p.Update(ctx, newNR(map[string]any{
				"name": "secrets/" + id,
				"body": map[string]any{"labels": map[string]any{"iter": strconv.Itoa(i)}},
			}))
		}()
		go func() {
			defer wg.Done()
			_, deleteErr = p.Delete(ctx, newNR(map[string]any{"name": "secrets/" + id, "etag": etag}))
		}()
		wg.Wait()

		if updateErr == nil && deleteErr == nil {
			t.Fatalf("iteration %d: the stale-etag delete and the update both succeeded", i)
		}
		if deleteErr == nil {
			if _, err := p.Get(ctx, newNR(map[string]any{"name": "secrets/" + id})); errStatus(err) != 404 {
				t.Fatalf("iteration %d: delete succeeded but the secret is still readable (%v)", i, err)
			}
			continue
		}
		requireAborted(t, deleteErr)
	}
}

// TestVersionEtagPreconditionsAreAtomic races two transitions that both carry
// the same pre-transition etag: at most one may apply, because the winner
// changes the version etag (and, for destroy, makes the state terminal).
func TestVersionEtagPreconditionsAreAtomic(t *testing.T) {
	ctx := context.Background()
	p := New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))

	const iterations = 200
	for i := 0; i < iterations; i++ {
		secret := "v" + strconv.Itoa(i)
		if _, err := p.Create(ctx, newNR(map[string]any{"secretId": secret, "body": map[string]any{}})); err != nil {
			t.Fatalf("create: %v", err)
		}
		added, err := p.AddVersion(ctx, newNR(map[string]any{
			"name": "secrets/" + secret,
			"body": map[string]any{"payload": map[string]any{"data": "aGVsbG8="}},
		}))
		if err != nil {
			t.Fatalf("addVersion: %v", err)
		}
		etag, _ := added.Data["etag"].(string)
		versionName := "secrets/" + secret + "/versions/1"

		var wg sync.WaitGroup
		var disableErr, destroyErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, disableErr = p.DisableVersion(ctx, newNR(map[string]any{
				"name": versionName, "body": map[string]any{"etag": etag},
			}))
		}()
		go func() {
			defer wg.Done()
			_, destroyErr = p.DestroyVersion(ctx, newNR(map[string]any{
				"name": versionName, "body": map[string]any{"etag": etag},
			}))
		}()
		wg.Wait()

		if disableErr == nil && destroyErr == nil {
			t.Fatalf("iteration %d: disable and destroy both accepted the same pre-transition etag", i)
		}
	}
}
