package metastore

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	metastorestore "jaiscloud/internal/gcp/store/metastore"
)

// fakeCatalogHost records the serving-plane catalog lifecycle calls.
type fakeCatalogHost struct {
	mu     sync.Mutex
	opened map[string]int
	closed []string
	fail   error
}

func newFakeCatalogHost() *fakeCatalogHost {
	return &fakeCatalogHost{opened: map[string]int{}}
}

func (f *fakeCatalogHost) OpenCatalog(_ context.Context, id string, port int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		return f.fail
	}
	f.opened[id] = port
	return nil
}

func (f *fakeCatalogHost) CloseCatalog(_ context.Context, id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.opened, id)
	f.closed = append(f.closed, id)
}

// TestPerServiceCatalogs covers the control-plane half of NF32: a Service owns a
// Hive Metastore catalog served on its own port, two Services with the same name
// in different projects/locations get different catalogs, and the endpoint a
// client is handed actually routes to that catalog.
func TestPerServiceCatalogs(t *testing.T) {
	ctx := context.Background()
	host := newFakeCatalogHost()
	s := NewService(metastorestore.NewMemoryStore(), WithCatalogHost(host), WithHMSPort(9083))

	a, _, err := s.CreateService(ctx, "proj-a", "us-central1", "hms", nil)
	if err != nil {
		t.Fatalf("create a: %v", err)
	}
	b, _, err := s.CreateService(ctx, "proj-b", "europe-west1", "hms", nil)
	if err != nil {
		t.Fatalf("create b: %v", err)
	}
	if a.Port == b.Port {
		t.Fatalf("two Services share port %d", a.Port)
	}
	if a.Port != 9084 || b.Port != 9085 {
		t.Fatalf("ports = %d, %d; want 9084, 9085 (base+1, in creation order)", a.Port, b.Port)
	}

	idA := CatalogID("proj-a", "us-central1", "hms")
	idB := CatalogID("proj-b", "europe-west1", "hms")
	if idA == idB {
		t.Fatalf("same-named Services in different projects share a catalog id: %q", idA)
	}
	if got, want := host.opened[idA], a.Port; got != want {
		t.Fatalf("a's catalog opened on port %d, want %d", got, want)
	}
	if got, want := host.opened[idB], b.Port; got != want {
		t.Fatalf("b's catalog opened on port %d, want %d", got, want)
	}

	// The rendered endpoint and port are the Service's own catalog routing
	// address, not a cosmetic constant.
	ja := ServiceJSON(a, "proj-a")
	if ja["port"] != a.Port {
		t.Fatalf("rendered port = %v, want %d", ja["port"], a.Port)
	}
	if want := "thrift://hms.us-central1.metastore.jaiscloud.local:9084"; ja["endpointUri"] != want {
		t.Fatalf("endpointUri = %v, want %s", ja["endpointUri"], want)
	}
	jb := ServiceJSON(b, "proj-b")
	if want := "thrift://hms.europe-west1.metastore.jaiscloud.local:9085"; jb["endpointUri"] != want {
		t.Fatalf("b endpointUri = %v, want %s", jb["endpointUri"], want)
	}

	// The Dataproc attachment resolver points a cluster at exactly its
	// Service's catalog port.
	ep, err := s.MetastoreEndpoint(ctx, "projects/proj-b/locations/europe-west1/services/hms", "proj-b", "europe-west1")
	if err != nil {
		t.Fatalf("MetastoreEndpoint: %v", err)
	}
	if ep != jb["endpointUri"] {
		t.Fatalf("attachment endpoint = %q, want %v", ep, jb["endpointUri"])
	}

	// UpdateService must not reassign the port.
	upd, _, err := s.UpdateService(ctx, "proj-a", "us-central1", "hms", json.RawMessage(`{"labels":{"env":"prod"}}`), "")
	if err != nil {
		t.Fatalf("update a: %v", err)
	}
	if upd.Port != a.Port {
		t.Fatalf("update changed the port from %d to %d", a.Port, upd.Port)
	}

	// Deleting a Service closes and drops its catalog; the other keeps serving.
	if _, err := s.DeleteService(ctx, "proj-a", "us-central1", "hms"); err != nil {
		t.Fatalf("delete a: %v", err)
	}
	if _, ok := host.opened[idA]; ok {
		t.Fatal("a's catalog is still open after the Service was deleted")
	}
	if _, ok := host.opened[idB]; !ok {
		t.Fatal("deleting a closed b's catalog")
	}
}

// TestServicePortsAreNotReused verifies a new Service never takes a port an
// existing Service still holds, so endpoints stay unambiguous.
func TestServicePortsAreNotReused(t *testing.T) {
	ctx := context.Background()
	s := NewService(metastorestore.NewMemoryStore(), WithHMSPort(9083))
	first, _, err := s.CreateService(ctx, "p", "us-central1", "one", nil)
	if err != nil {
		t.Fatalf("create one: %v", err)
	}
	second, _, err := s.CreateService(ctx, "p", "us-central1", "two", nil)
	if err != nil {
		t.Fatalf("create two: %v", err)
	}
	if second.Port <= first.Port {
		t.Fatalf("second port %d is not above the first %d", second.Port, first.Port)
	}
}
