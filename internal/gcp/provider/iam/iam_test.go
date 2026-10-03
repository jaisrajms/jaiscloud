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
	if len(accounts) != 1 {
		t.Errorf("expected 1 service account, got %d", len(accounts))
	}
}
