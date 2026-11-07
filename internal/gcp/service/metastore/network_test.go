package metastore

import (
	"context"
	"testing"

	"jaiscloud/internal/gcp/reference"
	"jaiscloud/internal/gcp/resource"
	metastorestore "jaiscloud/internal/gcp/store/metastore"
	"jaiscloud/internal/store"
)

// TestServiceNetworkReference covers FC4-4: a Dataproc Metastore Service's
// network is resolved against the compute network store. The documented form
// names the project by number; a malformed, cross-project or missing network is
// INVALID_ARGUMENT.
func TestServiceNetworkReference(t *testing.T) {
	ctx := context.Background()
	res := store.NewMemoryResourceStore()
	if err := res.Create(ctx, "proj", store.GlobalRegion, store.ResourceEntry{
		Type: reference.ComputeNetworkType, ID: "net1", Data: []byte(`{"name":"net1"}`),
	}); err != nil {
		t.Fatalf("seed network: %v", err)
	}
	s := NewService(metastorestore.NewMemoryStore(), WithResources(res))

	ok := []byte(`{"network":"projects/` + resource.ProjectNumber("proj") + `/global/networks/net1"}`)
	if _, _, err := s.CreateService(ctx, "proj", "us-central1", "ok", ok); err != nil {
		t.Fatalf("accept: %v", err)
	}

	for name, cfg := range map[string][]byte{
		"missing":     []byte(`{"network":"projects/` + resource.ProjectNumber("proj") + `/global/networks/absent"}`),
		"other-proj":  []byte(`{"network":"projects/other/global/networks/net1"}`),
		"malformed":   []byte(`{"network":"projects/proj/global/networks"}`),
		"wrong-shape": []byte(`{"network":"projects/proj/regions/us-central1/subnetworks/net1"}`),
	} {
		if _, _, err := s.CreateService(ctx, "proj", "us-central1", "svc-"+name, cfg); err == nil {
			t.Fatalf("%s network: create succeeded, want INVALID_ARGUMENT", name)
		} else if providerCode(err) != "InvalidArgument" {
			t.Fatalf("%s network = %v, want INVALID_ARGUMENT", name, err)
		}
	}
}
