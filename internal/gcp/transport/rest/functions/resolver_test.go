package functions

import (
	"context"
	"testing"

	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/gcp/resource"
	core "jaiscloud/internal/gcp/service/functions"
	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// stubOperationResolver owns a single top-level operation name and declines
// everything else, mirroring the Service Usage REST resolver.
type stubOperationResolver struct {
	name   string
	result map[string]any
}

func (s stubOperationResolver) ResolveOperation(_ context.Context, _, name string) (map[string]any, bool, error) {
	if name != s.name {
		return nil, false, nil
	}
	return s.result, true, nil
}

// TestGetOperationFallsBackToResolver verifies a top-level operations/{id} that
// Functions does not own is resolved by a wired cross-service resolver (Service
// Usage), while an id no resolver owns still 404s through Functions.
func TestGetOperationFallsBackToResolver(t *testing.T) {
	p := NewProvider(core.NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		core.WithBlobs(blobfs.NewMemoryBlobStore())), "proj")
	want := map[string]any{"name": "operations/abc", "done": true}
	p.SetOperationResolvers(stubOperationResolver{name: "operations/abc", result: want})

	resp, err := p.GetOperation(context.Background(), &model.NormalizedRequest{
		AccountID:  "proj",
		Params:     map[string]any{"name": "operations/abc"},
		ResourceID: resource.ResourceID("proj"),
	})
	if err != nil {
		t.Fatalf("GetOperation fallback: %v", err)
	}
	if resp.Data["name"] != "operations/abc" {
		t.Fatalf("resolved operation = %v, want the fallback result", resp.Data)
	}

	if _, err := p.GetOperation(context.Background(), &model.NormalizedRequest{
		AccountID:  "proj",
		Params:     map[string]any{"name": "operations/nope"},
		ResourceID: resource.ResourceID("proj"),
	}); err == nil {
		t.Fatalf("unknown id without an owner must still error")
	}
}
