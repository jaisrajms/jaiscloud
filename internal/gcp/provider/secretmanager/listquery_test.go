package secretmanager

import (
	"context"
	"testing"

	"jaiscloud/internal/gcp/crypto"
	"jaiscloud/internal/gcp/store/kms"
	secretmanagerstore "jaiscloud/internal/gcp/store/secretmanager"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

func newListProvider() *Provider {
	return New(secretmanagerstore.NewMemoryStore(), store.NewMemoryResourceStore(), crypto.NewEnvelopeEncryptor(kms.NewMemoryStore()))
}

func createSecretWithLabels(t *testing.T, p *Provider, id, team string) {
	t.Helper()
	body := map[string]any{"replication": map[string]any{"automatic": map[string]any{}}}
	if team != "" {
		body["labels"] = map[string]any{"team": team}
	}
	if _, err := p.Create(context.Background(), newNR(map[string]any{"secretId": id, "body": body})); err != nil {
		t.Fatalf("create secret %s: %v", id, err)
	}
}

func listedSecretNames(t *testing.T, resp *model.ProviderResponse) []string {
	t.Helper()
	secrets, _ := resp.Data["secrets"].([]any)
	names := make([]string, 0, len(secrets))
	for _, s := range secrets {
		m, _ := s.(map[string]any)
		n, _ := m["name"].(string)
		names = append(names, n)
	}
	return names
}

// TestListFilter verifies secrets.list honors the documented AIP-160 `filter`
// (over name and labels) and that no total count is reported once filtered.
func TestListFilter(t *testing.T) {
	ctx := context.Background()
	p := newListProvider()
	createSecretWithLabels(t, p, "db-password", "alpha")
	createSecretWithLabels(t, p, "api-key", "beta")
	createSecretWithLabels(t, p, "spare", "")

	resp, err := p.List(ctx, newNR(map[string]any{"filter": `name:"key"`}))
	if err != nil {
		t.Fatalf("filtered list: %v", err)
	}
	names := listedSecretNames(t, resp)
	if len(names) != 1 || names[0] != "projects/proj/secrets/api-key" {
		t.Fatalf(`name:"key" = %v, want only api-key`, names)
	}
	if resp.Data["totalSize"] != 0 {
		t.Errorf("totalSize = %v, want 0 (no total once filtered)", resp.Data["totalSize"])
	}

	resp, err = p.List(ctx, newNR(map[string]any{"filter": `labels.team=alpha`}))
	if err != nil {
		t.Fatalf("label-filtered list: %v", err)
	}
	if names := listedSecretNames(t, resp); len(names) != 1 || names[0] != "projects/proj/secrets/db-password" {
		t.Fatalf("labels.team=alpha = %v, want only db-password", names)
	}

	// Unfiltered: every secret, with the real total.
	resp, err = p.List(ctx, newNR(nil))
	if err != nil {
		t.Fatalf("unfiltered list: %v", err)
	}
	if names := listedSecretNames(t, resp); len(names) != 3 {
		t.Fatalf("unfiltered list = %v, want 3 secrets", names)
	}
	if resp.Data["totalSize"] != 3 {
		t.Errorf("unfiltered totalSize = %v, want 3", resp.Data["totalSize"])
	}
}

// TestListFilterVersions verifies versions.list honors a state filter.
func TestListFilterVersions(t *testing.T) {
	ctx := context.Background()
	p := newListProvider()
	createSecretWithLabels(t, p, "s", "")
	if _, err := p.AddVersion(ctx, newNR(map[string]any{"name": "secrets/s", "body": map[string]any{"payload": map[string]any{"data": "aGVsbG8="}}})); err != nil {
		t.Fatalf("add version: %v", err)
	}

	resp, err := p.ListVersions(ctx, newNR(map[string]any{"name": "secrets/s/versions", "filter": `state:ENABLED`}))
	if err != nil {
		t.Fatalf("filtered versions: %v", err)
	}
	versions, _ := resp.Data["versions"].([]any)
	if len(versions) != 1 {
		t.Fatalf("state:ENABLED = %v, want one version", versions)
	}
	if resp.Data["totalSize"] != 0 {
		t.Errorf("filtered version totalSize = %v, want 0", resp.Data["totalSize"])
	}

	resp, err = p.ListVersions(ctx, newNR(map[string]any{"name": "secrets/s/versions", "filter": `state:DESTROYED`}))
	if err != nil {
		t.Fatalf("destroyed versions: %v", err)
	}
	if versions, _ := resp.Data["versions"].([]any); len(versions) != 0 {
		t.Fatalf("state:DESTROYED = %v, want none", versions)
	}
}

// TestListRejectsBadFilter verifies an unsupported filter clause is
// INVALID_ARGUMENT rather than silently ignored.
func TestListRejectsBadFilter(t *testing.T) {
	p := newListProvider()
	_, err := p.List(context.Background(), newNR(map[string]any{"filter": `name:"a" OR name:"b"`}))
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	pe, ok := err.(*model.ProviderError)
	if !ok || pe.Code != "InvalidArgument" || pe.HTTPStatus != 400 {
		t.Fatalf("err = %#v, want InvalidArgument/400", err)
	}
}
