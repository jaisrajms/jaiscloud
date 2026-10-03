package iam

import (
	"context"
	"testing"

	"jaiscloud/internal/store"
)

// TestServiceAccountDisableEnable covers J46 disable/enable.
func TestServiceAccountDisableEnable(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())
	if _, err := p.Create(ctx, newNR(map[string]any{"body": map[string]any{"accountId": "sa"}})); err != nil {
		t.Fatalf("create: %v", err)
	}
	email := "sa@proj.iam.gserviceaccount.com"
	name := "serviceAccounts/" + email

	dis, err := p.Disable(ctx, newNR(map[string]any{"name": name}))
	if err != nil {
		t.Fatalf("disable: %v", err)
	}
	if dis.Data["disabled"] != true {
		t.Fatalf("disabled = %v, want true", dis.Data["disabled"])
	}

	// The disabled state is persisted and visible on get.
	got, err := p.Get(ctx, newNR(map[string]any{"name": name}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Data["disabled"] != true {
		t.Fatalf("get disabled = %v, want true", got.Data["disabled"])
	}

	en, err := p.Enable(ctx, newNR(map[string]any{"name": name}))
	if err != nil {
		t.Fatalf("enable: %v", err)
	}
	if en.Data["disabled"] != false {
		t.Fatalf("disabled = %v, want false", en.Data["disabled"])
	}
}

// TestServiceAccountUndelete covers J46 undelete: a deleted account is restored
// from its tombstone; an unknown account is NotFound.
func TestServiceAccountUndelete(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())
	if _, err := p.Create(ctx, newNR(map[string]any{"body": map[string]any{"accountId": "sa"}})); err != nil {
		t.Fatalf("create: %v", err)
	}
	email := "sa@proj.iam.gserviceaccount.com"
	name := "serviceAccounts/" + email

	if _, err := p.Delete(ctx, newNR(map[string]any{"name": name})); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := p.Get(ctx, newNR(map[string]any{"name": name})); err == nil {
		t.Fatal("expected get of a deleted account to fail")
	}

	und, err := p.Undelete(ctx, newNR(map[string]any{"name": name}))
	if err != nil {
		t.Fatalf("undelete: %v", err)
	}
	if und.Data["email"] != email {
		t.Fatalf("undelete email = %v, want %v", und.Data["email"], email)
	}
	if _, err := p.Get(ctx, newNR(map[string]any{"name": name})); err != nil {
		t.Fatalf("get after undelete: %v", err)
	}

	// An account that was never created (no tombstone) is NotFound.
	if _, err := p.Undelete(ctx, newNR(map[string]any{"name": "serviceAccounts/ghost@proj.iam.gserviceaccount.com"})); err == nil {
		t.Fatal("expected NotFound undeleting an unknown account")
	}
}

// TestServiceAccountKeyDisableEnable covers J45 key disable/enable.
func TestServiceAccountKeyDisableEnable(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())
	if _, err := p.Create(ctx, newNR(map[string]any{"body": map[string]any{"accountId": "sa"}})); err != nil {
		t.Fatalf("create SA: %v", err)
	}
	email := "sa@proj.iam.gserviceaccount.com"

	kr, err := p.ServiceAccountKeyCreate(ctx, newNR(map[string]any{"name": "serviceAccounts/" + email + "/keys"}))
	if err != nil {
		t.Fatalf("key create: %v", err)
	}
	keyID, _ := kr.Data["keyId"].(string)
	keyName := "serviceAccounts/" + email + "/keys/" + keyID

	dis, err := p.ServiceAccountKeyDisable(ctx, newNR(map[string]any{"name": keyName}))
	if err != nil {
		t.Fatalf("key disable: %v", err)
	}
	if dis.Data["disabled"] != true {
		t.Fatalf("key disabled = %v, want true", dis.Data["disabled"])
	}
	if dt, _ := dis.Data["disableTime"].(string); dt == "" {
		t.Fatal("expected disableTime when a key is disabled")
	}

	en, err := p.ServiceAccountKeyEnable(ctx, newNR(map[string]any{"name": keyName}))
	if err != nil {
		t.Fatalf("key enable: %v", err)
	}
	if en.Data["disabled"] != false {
		t.Fatalf("key disabled = %v, want false", en.Data["disabled"])
	}
	if _, present := en.Data["disableTime"]; present {
		t.Fatalf("disableTime should be cleared on enable, got %v", en.Data["disableTime"])
	}
}
