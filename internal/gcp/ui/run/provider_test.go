package runui

import (
	"context"
	"testing"

	runcore "jaiscloud/internal/gcp/service/run"
	runstore "jaiscloud/internal/gcp/store/run"
	"jaiscloud/internal/store"
)

func TestProviderForwardsToTheCore(t *testing.T) {
	ctx := context.Background()
	core := runcore.NewService(runstore.NewMemoryStore(), store.NewMemoryResourceStore())
	p := NewProvider(core)

	if _, err := p.CreateService(ctx, "proj", "us-central1", "svc", map[string]any{
		"template": map[string]any{"containers": []any{map[string]any{"image": "nginx:latest"}}},
	}, false); err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	got, err := p.GetService(ctx, "proj", "us-central1", "svc")
	if err != nil || got.ID != "svc" {
		t.Fatalf("GetService = %+v, %v", got, err)
	}
	if list, err := p.ListAllServices(ctx, "proj"); err != nil || len(list) != 1 {
		t.Fatalf("ListAllServices = %+v, %v", list, err)
	}
	if revs, err := p.ListRevisions(ctx, "proj", "us-central1", "svc"); err != nil || len(revs) == 0 {
		t.Fatalf("ListRevisions = %+v, %v", revs, err)
	} else if rev, err := p.GetRevision(ctx, "proj", "us-central1", "svc", revs[0].ID); err != nil || rev.ID == "" {
		t.Fatalf("GetRevision = %+v, %v", rev, err)
	}

	updated, err := p.UpdateService(ctx, "proj", "us-central1", "svc", map[string]any{
		"template": map[string]any{"containers": []any{map[string]any{"image": "nginx:2"}}},
	}, "template", false, false)
	if err != nil || updated.ID != "svc" {
		t.Fatalf("UpdateService = %+v, %v", updated, err)
	}

	pol, err := p.ServiceGetIamPolicy(ctx, "proj", "us-central1", "svc")
	if err != nil {
		t.Fatalf("ServiceGetIamPolicy: %v", err)
	}
	pol["bindings"] = []any{map[string]any{"role": "roles/run.viewer", "members": []any{"user:a@example.com"}}}
	pol, err = p.ServiceSetIamPolicy(ctx, "proj", "us-central1", "svc", map[string]any{"policy": pol})
	if err != nil || pol["etag"] == nil {
		t.Fatalf("ServiceSetIamPolicy = %+v, %v", pol, err)
	}
	perms, err := p.ServiceTestIamPermissions(ctx, "proj", "us-central1", "svc", []string{"run.services.get"})
	if err != nil || len(perms) != 1 {
		t.Fatalf("ServiceTestIamPermissions = %+v, %v", perms, err)
	}

	if _, err := p.CreateService(ctx, "proj", "us-central1", "svc2", map[string]any{}, true); err != nil {
		t.Fatalf("CreateService validateOnly: %v", err)
	}
	if _, err := p.UpdateService(ctx, "proj", "us-central1", "svc", map[string]any{}, "", true, false); err != nil {
		t.Fatalf("UpdateService validateOnly: %v", err)
	}

	if _, err := p.DeleteService(ctx, "proj", "us-central1", "svc", false, ""); err != nil {
		t.Fatalf("DeleteService: %v", err)
	}
	if _, err := p.GetService(ctx, "proj", "us-central1", "svc"); err == nil {
		t.Fatal("the service must be gone")
	}
}
