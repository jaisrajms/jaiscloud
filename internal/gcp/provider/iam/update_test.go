package iam

import (
	"context"
	"testing"

	"jaiscloud/internal/store"
)

// TestServiceAccountUpdatePatch exercises the serviceAccounts.patch shape the
// Java/gax client sends: a PatchServiceAccountRequest wrapping the account under
// "serviceAccount" with an updateMask for displayName,description.
func TestServiceAccountUpdatePatch(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())

	if _, err := p.Create(ctx, newNR(map[string]any{"body": map[string]any{"accountId": "sa-a"}})); err != nil {
		t.Fatalf("create: %v", err)
	}
	email := "sa-a@proj.iam.gserviceaccount.com"

	nr := newNR(map[string]any{
		"name": "serviceAccounts/" + email,
		"body": map[string]any{
			"serviceAccount": map[string]any{
				"displayName": "Updated SA",
				"description": "updated via patch",
			},
			"updateMask": "displayName,description",
		},
	})
	resp, err := p.Update(ctx, nr)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if resp.Data["displayName"] != "Updated SA" {
		t.Errorf("displayName = %v, want Updated SA", resp.Data["displayName"])
	}
	if resp.Data["description"] != "updated via patch" {
		t.Errorf("description = %v, want updated via patch", resp.Data["description"])
	}

	// The change is persisted.
	got, err := p.Get(ctx, newNR(map[string]any{"name": "serviceAccounts/" + email}))
	if err != nil {
		t.Fatalf("get after update: %v", err)
	}
	if got.Data["displayName"] != "Updated SA" || got.Data["description"] != "updated via patch" {
		t.Errorf("persisted update missing: %v", got.Data)
	}

	// The etag is refreshed to reflect the new displayName.
	if etag, _ := got.Data["etag"].(string); etag == "" {
		t.Error("expected a fresh etag after update")
	}
}

// TestServiceAccountUpdateMaskAndBody verifies mask semantics: a masked field is
// overwritten, an unmasked field is preserved, and updateMask may arrive either
// in the body or the query string.
func TestServiceAccountUpdateMaskAndBody(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())

	if _, err := p.Create(ctx, newNR(map[string]any{"body": map[string]any{
		"accountId":      "sa-a",
		"serviceAccount": map[string]any{"displayName": "Original", "description": "keep me"},
	}})); err != nil {
		t.Fatalf("create: %v", err)
	}
	email := "sa-a@proj.iam.gserviceaccount.com"

	// Mask names only displayName (query-string form); the incoming description
	// must be ignored and the stored one retained.
	nr := newNR(map[string]any{
		"name":       "serviceAccounts/" + email,
		"updateMask": "displayName",
		"body": map[string]any{"serviceAccount": map[string]any{
			"displayName": "Renamed",
			"description": "should not apply",
		}},
	})
	resp, err := p.Update(ctx, nr)
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if resp.Data["displayName"] != "Renamed" {
		t.Errorf("displayName = %v, want Renamed", resp.Data["displayName"])
	}
	if resp.Data["description"] != "keep me" {
		t.Errorf("description = %v, want keep me (unmasked field preserved)", resp.Data["description"])
	}

	// Empty mask overlays every field present in the bare body.
	nr = newNR(map[string]any{
		"name": "serviceAccounts/" + email,
		"body": map[string]any{"displayName": "All Overlaid", "description": "new desc"},
	})
	resp, err = p.Update(ctx, nr)
	if err != nil {
		t.Fatalf("update empty mask: %v", err)
	}
	if resp.Data["displayName"] != "All Overlaid" || resp.Data["description"] != "new desc" {
		t.Errorf("empty-mask overlay failed: %v", resp.Data)
	}
}

// TestServiceAccountUpdateRejectsUnsupportedMaskField fails loud (400) rather
// than silently ignoring a mask path IAM does not allow patching.
func TestServiceAccountUpdateRejectsUnsupportedMaskField(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())

	if _, err := p.Create(ctx, newNR(map[string]any{"body": map[string]any{"accountId": "sa-a"}})); err != nil {
		t.Fatalf("create: %v", err)
	}

	nr := newNR(map[string]any{
		"name":       "serviceAccounts/sa-a@proj.iam.gserviceaccount.com",
		"updateMask": "oauth2ClientId",
		"body":       map[string]any{"serviceAccount": map[string]any{"oauth2ClientId": "nope"}},
	})
	if _, err := p.Update(ctx, nr); err == nil || errStatus(err) != 400 {
		t.Fatalf("expected 400 on unsupported mask field, got %v", err)
	}
}

// TestServiceAccountUpdateEtagOCC verifies a supplied etag is enforced and that
// a matching etag (or none) is accepted.
func TestServiceAccountUpdateEtagOCC(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())

	created, err := p.Create(ctx, newNR(map[string]any{"body": map[string]any{"accountId": "sa-a"}}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	etag, _ := created.Data["etag"].(string)
	email := "sa-a@proj.iam.gserviceaccount.com"

	body := func(e string) map[string]any {
		sa := map[string]any{"displayName": "New Name"}
		if e != "" {
			sa["etag"] = e
		}
		return map[string]any{"name": "serviceAccounts/" + email, "body": map[string]any{"serviceAccount": sa}}
	}

	if _, err := p.Update(ctx, newNR(body("BOGUS="))); err == nil || errStatus(err) != 409 {
		t.Fatalf("expected 409 on etag mismatch, got %v", err)
	}
	if _, err := p.Update(ctx, newNR(body(etag))); err != nil {
		t.Fatalf("expected matching etag to succeed, got %v", err)
	}
}

// TestServiceAccountUpdateMissing verifies patching a missing account is 404.
func TestServiceAccountUpdateMissing(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())

	nr := newNR(map[string]any{
		"name": "serviceAccounts/missing@proj.iam.gserviceaccount.com",
		"body": map[string]any{"serviceAccount": map[string]any{"displayName": "x"}},
	})
	if _, err := p.Update(ctx, nr); err == nil || errStatus(err) != 404 {
		t.Fatalf("expected 404 on missing SA, got %v", err)
	}
}
