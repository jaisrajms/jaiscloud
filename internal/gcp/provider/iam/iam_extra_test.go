package iam

import (
	"context"
	"testing"

	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func errStatus(err error) int {
	if pe, ok := err.(*model.ProviderError); ok {
		return pe.HTTPStatus
	}
	return 0
}

func TestIAMNegativesPaginationAndOCC(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())

	for _, id := range []string{"sa-a", "sa-b", "sa-c"} {
		nr := newNR(map[string]any{"body": map[string]any{"accountId": id}})
		if _, err := p.Create(ctx, nr); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}

	// saToMap includes the full schema fields.
	nr := newNR(map[string]any{"body": map[string]any{"accountId": "rich", "serviceAccount": map[string]any{"displayName": "Rich", "description": "d", "disabled": true, "oauth2ClientId": "cid"}}})
	resp, err := p.Create(ctx, nr)
	if err != nil {
		t.Fatalf("create rich: %v", err)
	}
	for _, field := range []string{"etag", "description", "disabled", "oauth2ClientId", "projectId"} {
		if _, ok := resp.Data[field]; !ok {
			t.Errorf("sa response missing %s", field)
		}
	}

	// Pagination.
	nr = newNR(map[string]any{"pageSize": "2"})
	resp, err = p.List(ctx, nr)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	accounts, _ := resp.Data["accounts"].([]any)
	if len(accounts) != 2 {
		t.Fatalf("page 1 expected 2 accounts, got %d", len(accounts))
	}
	token, _ := resp.Data["nextPageToken"].(string)
	if token == "" {
		t.Fatal("expected nextPageToken")
	}

	// 409 duplicate.
	nr = newNR(map[string]any{"body": map[string]any{"accountId": "sa-a"}})
	if _, err := p.Create(ctx, nr); err == nil || errStatus(err) != 409 {
		t.Fatalf("expected 409 on duplicate SA, got %v", err)
	}

	// 404 get missing.
	nr = newNR(map[string]any{"name": "serviceAccounts/missing@proj.iam.gserviceaccount.com"})
	if _, err := p.Get(ctx, nr); err == nil || errStatus(err) != 404 {
		t.Fatalf("expected 404 on missing SA, got %v", err)
	}

	// IAM policy 404 on missing SA.
	if _, err := p.GetIamPolicy(ctx, nr); err == nil || errStatus(err) != 404 {
		t.Fatalf("expected 404 on getIamPolicy for missing SA, got %v", err)
	}

	// GetIamPolicy default + requestedPolicyVersion.
	email := "sa-a@proj.iam.gserviceaccount.com"
	nr = newNR(map[string]any{"name": "serviceAccounts/" + email})
	pol, err := p.GetIamPolicy(ctx, nr)
	if err != nil {
		t.Fatalf("getIamPolicy: %v", err)
	}
	if pol.Data["version"] != 1 {
		t.Errorf("expected default policy version 1, got %v", pol.Data["version"])
	}

	nr = newNR(map[string]any{"name": "serviceAccounts/" + email, "options.requestedPolicyVersion": "3"})
	pol, err = p.GetIamPolicy(ctx, nr)
	if err != nil {
		t.Fatalf("getIamPolicy v3: %v", err)
	}
	if pol.Data["version"] != 3 {
		t.Errorf("expected requestedPolicyVersion 3, got %v", pol.Data["version"])
	}

	// setIamPolicy etag OCC: mismatch → HTTP 409 / google.rpc ABORTED.
	bindings := []any{map[string]any{"role": "roles/iam.serviceAccountUser", "members": []any{"allUsers"}}}
	nr = newNR(map[string]any{"name": "serviceAccounts/" + email, "body": map[string]any{"bindings": bindings, "etag": "BOGUS="}})
	_, err = p.SetIamPolicy(ctx, nr)
	if err == nil || errStatus(err) != 409 {
		t.Fatalf("expected 409 on etag mismatch, got %v", err)
	}
	pe, ok := err.(*model.ProviderError)
	if !ok {
		t.Fatalf("expected *model.ProviderError, got %T", err)
	}
	if pe.Code != "Aborted" || pe.Status != "ABORTED" {
		t.Fatalf("expected Code Aborted / Status ABORTED on etag mismatch, got Code=%q Status=%q", pe.Code, pe.Status)
	}

	// No etag → accepted, and a fresh etag is stored.
	nr = newNR(map[string]any{"name": "serviceAccounts/" + email, "body": map[string]any{"bindings": bindings}})
	set, err := p.SetIamPolicy(ctx, nr)
	if err != nil {
		t.Fatalf("setIamPolicy: %v", err)
	}
	if etag, _ := set.Data["etag"].(string); etag == "" {
		t.Error("expected a fresh etag after setIamPolicy")
	}
}

// TestIAMTestIamPermissions verifies testIamPermissions echoes the requested
// permissions and 404s on a missing service account.
func TestIAMTestIamPermissions(t *testing.T) {
	ctx := context.Background()
	p := New(store.NewMemoryResourceStore())

	if _, err := p.Create(ctx, newNR(map[string]any{"body": map[string]any{"accountId": "sa-a"}})); err != nil {
		t.Fatalf("create: %v", err)
	}

	email := "sa-a@proj.iam.gserviceaccount.com"
	nr := newNR(map[string]any{
		"name": "serviceAccounts/" + email,
		"body": map[string]any{"permissions": []any{"iam.serviceAccounts.get", "iam.serviceAccounts.list"}},
	})
	resp, err := p.TestIamPermissions(ctx, nr)
	if err != nil {
		t.Fatalf("testIamPermissions: %v", err)
	}
	perms, _ := resp.Data["permissions"].([]string)
	if len(perms) != 2 {
		t.Fatalf("expected 2 granted permissions, got %v", resp.Data["permissions"])
	}

	nr = newNR(map[string]any{"name": "serviceAccounts/missing@proj.iam.gserviceaccount.com"})
	if _, err := p.TestIamPermissions(ctx, nr); err == nil || errStatus(err) != 404 {
		t.Fatalf("expected 404 on missing SA, got %v", err)
	}
}
