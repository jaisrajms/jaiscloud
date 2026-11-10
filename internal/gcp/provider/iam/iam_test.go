package iam

import (
	"context"
	"testing"

	"jaiscloud/internal/gcp/resource"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func newNR(params map[string]any) *model.NormalizedRequest {
	if params == nil {
		params = map[string]any{}
	}
	return &model.NormalizedRequest{AccountID: "proj", Params: params, ResourceID: resource.ResourceID("proj")}
}

func TestServiceAccountDisplayNameIsNotDefaulted(t *testing.T) {
	// Real IAM keeps displayName optional: a create whose serviceAccount is
	// empty answers with no displayName at all (measured 2026-11-10: create
	// body {"accountId": ..., "serviceAccount": {}}; get/list omit the field),
	// and a supplied one is echoed. Defaulting it to the account id made the
	// provider's non-computed display_name permadiff (TF2-14).
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())

	resp, err := p.Create(ctx, newNR(map[string]any{"body": map[string]any{"accountId": "bare"}}))
	if err != nil {
		t.Fatalf("create bare: %v", err)
	}
	if v, ok := resp.Data["displayName"]; ok {
		t.Fatalf("create without displayName rendered %v, want the field omitted", v)
	}

	nr := newNR(map[string]any{"name": "serviceAccounts/bare@proj.iam.gserviceaccount.com"})
	resp, err = p.Get(ctx, nr)
	if err != nil {
		t.Fatalf("get bare: %v", err)
	}
	if v, ok := resp.Data["displayName"]; ok {
		t.Fatalf("get without displayName rendered %v, want the field omitted", v)
	}

	resp, err = p.Create(ctx, newNR(map[string]any{"body": map[string]any{
		"accountId":      "named",
		"serviceAccount": map[string]any{"displayName": "Named"},
	}}))
	if err != nil {
		t.Fatalf("create named: %v", err)
	}
	if got := resp.Data["displayName"]; got != "Named" {
		t.Fatalf("create displayName = %v, want Named", got)
	}

	// List renders each account the same way get does.
	resp, err = p.List(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	accounts, _ := resp.Data["accounts"].([]any)
	seen := 0
	for _, a := range accounts {
		sa, _ := a.(map[string]any)
		email, _ := sa["email"].(string)
		switch email {
		case "bare@proj.iam.gserviceaccount.com":
			seen++
			if v, ok := sa["displayName"]; ok {
				t.Fatalf("list rendered bare displayName %v, want omitted", v)
			}
		case "named@proj.iam.gserviceaccount.com":
			seen++
			if sa["displayName"] != "Named" {
				t.Fatalf("list named displayName = %v, want Named", sa["displayName"])
			}
		}
	}
	if seen != 2 {
		t.Fatalf("listed %d of the two created accounts", seen)
	}
}

func TestServiceAccountRoundTrip(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())

	nr := newNR(map[string]any{"body": map[string]any{"accountId": "test-sa"}})
	resp, err := p.Create(ctx, nr)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if resp.Data["email"] != "test-sa@proj.iam.gserviceaccount.com" {
		t.Errorf("unexpected email: %v", resp.Data["email"])
	}

	nr = newNR(map[string]any{"name": "serviceAccounts/test-sa@proj.iam.gserviceaccount.com"})
	resp, err = p.Get(ctx, nr)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if resp.Data["email"] != "test-sa@proj.iam.gserviceaccount.com" {
		t.Errorf("unexpected email on get: %v", resp.Data["email"])
	}

	nr = newNR(nil)
	resp, err = p.List(ctx, nr)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	accounts, _ := resp.Data["accounts"].([]any)
	// The created account plus the two Google-managed defaults the project
	// always has (App Engine + Compute Engine).
	if len(accounts) != 3 {
		t.Errorf("expected 3 service accounts, got %d", len(accounts))
	}
}
