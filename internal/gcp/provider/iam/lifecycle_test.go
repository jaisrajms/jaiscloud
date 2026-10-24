package iam

import (
	"context"
	"strings"
	"testing"

	"jaiscloud/internal/model"
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
	// projects.serviceAccounts.disable declares an Empty response: the account is
	// never echoed back (real GCP sends {}), so read the state through get.
	assertEmptyResponse(t, "disable", dis)

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
	assertEmptyResponse(t, "enable", en)
	got, err = p.Get(ctx, newNR(map[string]any{"name": name}))
	if err != nil {
		t.Fatalf("get after enable: %v", err)
	}
	if got.Data["disabled"] != false {
		t.Fatalf("get disabled after enable = %v, want false", got.Data["disabled"])
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
	restored, ok := und.Data["restoredAccount"].(map[string]any)
	if !ok {
		t.Fatalf("undelete response must nest the account under restoredAccount, got %v", und.Data)
	}
	if restored["email"] != email {
		t.Fatalf("undelete email = %v, want %v", restored["email"], email)
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
	createdName, _ := kr.Data["name"].(string)
	keyID := createdName[strings.LastIndex(createdName, "/")+1:]
	keyName := "serviceAccounts/" + email + "/keys/" + keyID

	dis, err := p.ServiceAccountKeyDisable(ctx, newNR(map[string]any{"name": keyName}))
	if err != nil {
		t.Fatalf("key disable: %v", err)
	}
	// keys.disable declares an Empty response; disabled/disableTime are read
	// back through keys.get, exactly as the official clients must.
	assertEmptyResponse(t, "keys.disable", dis)
	got, err := p.ServiceAccountKeyGet(ctx, newNR(map[string]any{"name": keyName}))
	if err != nil {
		t.Fatalf("key get after disable: %v", err)
	}
	if got.Data["disabled"] != true {
		t.Fatalf("key disabled = %v, want true", got.Data["disabled"])
	}
	if dt, _ := got.Data["disableTime"].(string); dt == "" {
		t.Fatal("expected disableTime when a key is disabled")
	}

	en, err := p.ServiceAccountKeyEnable(ctx, newNR(map[string]any{"name": keyName}))
	if err != nil {
		t.Fatalf("key enable: %v", err)
	}
	assertEmptyResponse(t, "keys.enable", en)
	got, err = p.ServiceAccountKeyGet(ctx, newNR(map[string]any{"name": keyName}))
	if err != nil {
		t.Fatalf("key get after enable: %v", err)
	}
	if got.Data["disabled"] != false {
		t.Fatalf("key disabled = %v, want false", got.Data["disabled"])
	}
	if _, present := got.Data["disableTime"]; present {
		t.Fatalf("disableTime should be cleared on enable, got %v", got.Data["disableTime"])
	}
}

// assertEmptyResponse pins the Discovery-declared Empty body of the four
// service-account disable/enable verbs: a 200 whose data map is empty, so the
// encoder emits {} rather than the mutated resource.
func assertEmptyResponse(t *testing.T, name string, resp *model.ProviderResponse) {
	t.Helper()
	if resp.HTTPStatus != 200 {
		t.Fatalf("%s status = %d, want 200", name, resp.HTTPStatus)
	}
	if resp.Data == nil {
		t.Fatalf("%s data = nil, want an empty object (JSON null is not Empty)", name)
	}
	if len(resp.Data) != 0 {
		t.Fatalf("%s body = %v, want the Empty response real GCP sends", name, resp.Data)
	}
}
