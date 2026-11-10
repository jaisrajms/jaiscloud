package metastore

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"testing"

	metastorestore "jaiscloud/internal/gcp/store/metastore"
)

// TestCreateServiceHonorsRequestedPort pins the TF2-13 fix on the create path:
// a caller-supplied port is the Service's catalog listener and the wire port,
// exactly as real Metastore echoes it (the provider is Optional+Computed and
// permadiffs otherwise). A Service that requests none keeps the emulator
// assignment above --hms-port.
func TestCreateServiceHonorsRequestedPort(t *testing.T) {
	ctx := context.Background()
	host := newFakeCatalogHost()
	s := NewService(metastorestore.NewMemoryStore(), WithCatalogHost(host), WithHMSPort(9083))

	created, _, err := s.CreateService(ctx, "proj", "us-central1", "requested", json.RawMessage(`{"port":9080}`))
	if err != nil {
		t.Fatalf("create with port: %v", err)
	}
	if created.Port != 9080 {
		t.Fatalf("port = %d, want the requested 9080", created.Port)
	}
	if got := host.opened[CatalogID("proj", "us-central1", "requested")]; got != 9080 {
		t.Fatalf("catalog opened on %d, want 9080", got)
	}
	rendered := ServiceJSON(created, "proj")
	if rendered["port"] != 9080 {
		t.Fatalf("rendered port = %v, want 9080", rendered["port"])
	}
	if want := "thrift://requested.us-central1.metastore.jaiscloud.local:9080"; rendered["endpointUri"] != want {
		t.Fatalf("endpointUri = %v, want %s", rendered["endpointUri"], want)
	}

	// A port below the base does not disturb the assignment high-water mark:
	// the next unrequested Service still starts above the base port.
	auto, _, err := s.CreateService(ctx, "proj", "us-central1", "auto", nil)
	if err != nil {
		t.Fatalf("create without port: %v", err)
	}
	if auto.Port != 9084 {
		t.Fatalf("auto port = %d, want 9084 (base+1)", auto.Port)
	}

	// A request above the high-water mark raises it, so numbered ports never
	// collide with a requested one.
	high, _, err := s.CreateService(ctx, "proj", "us-central1", "high", json.RawMessage(`{"port":9090}`))
	if err != nil {
		t.Fatalf("create high port: %v", err)
	}
	if high.Port != 9090 {
		t.Fatalf("high port = %d, want 9090", high.Port)
	}
	next, _, err := s.CreateService(ctx, "proj", "us-central1", "next", nil)
	if err != nil {
		t.Fatalf("create next: %v", err)
	}
	if next.Port != 9091 {
		t.Fatalf("next port = %d, want 9091 (above 9090)", next.Port)
	}

	// An explicit zero is proto3's unset: assign one.
	zero, _, err := s.CreateService(ctx, "proj", "us-central1", "zero", json.RawMessage(`{"port":0}`))
	if err != nil {
		t.Fatalf("create zero port: %v", err)
	}
	if zero.Port == 0 {
		t.Fatal("explicit port 0 must mean unset and be assigned")
	}
}

// TestUpdateServiceMovesServicePort covers the update path the provider drives
// with updateMask=port: the stored port and the catalog listener move together,
// the read-back matches, and a mask that names no port leaves it alone.
func TestUpdateServiceMovesServicePort(t *testing.T) {
	ctx := context.Background()
	host := newFakeCatalogHost()
	s := NewService(metastorestore.NewMemoryStore(), WithCatalogHost(host), WithHMSPort(9083))

	created, _, err := s.CreateService(ctx, "proj", "us-central1", "hms", json.RawMessage(`{"port":9080}`))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	id := CatalogID("proj", "us-central1", "hms")

	updated, _, err := s.UpdateService(ctx, "proj", "us-central1", "hms", json.RawMessage(`{"port":9082}`), "port")
	if err != nil {
		t.Fatalf("update port: %v", err)
	}
	if updated.Port != 9082 {
		t.Fatalf("port after update = %d, want 9082", updated.Port)
	}
	if got := host.opened[id]; got != 9082 {
		t.Fatalf("catalog listener on %d, want the new 9082", got)
	}
	got, err := s.GetService(ctx, "proj", "us-central1", "hms")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Port != 9082 {
		t.Fatalf("read-back port = %d, want 9082", got.Port)
	}
	if want := "thrift://hms.us-central1.metastore.jaiscloud.local:9082"; ServiceJSON(got, "proj")["endpointUri"] != want {
		t.Fatalf("read-back endpointUri = %v, want %s", ServiceJSON(got, "proj")["endpointUri"], want)
	}

	// A mask that names no port keeps the current one even when the body
	// carries a port (the provider sends only the changed fields).
	unchanged, _, err := s.UpdateService(ctx, "proj", "us-central1", "hms", json.RawMessage(`{"port":9099,"labels":{"env":"prod"}}`), "labels")
	if err != nil {
		t.Fatalf("update labels: %v", err)
	}
	if unchanged.Port != 9082 {
		t.Fatalf("labels-only update changed the port to %d, want 9082", unchanged.Port)
	}

	// The same port is a no-op, not a conflict with itself.
	same, _, err := s.UpdateService(ctx, "proj", "us-central1", "hms", json.RawMessage(`{"port":9082}`), "port")
	if err != nil {
		t.Fatalf("update to the same port: %v", err)
	}
	if same.Port != 9082 || created.Port != 9080 {
		t.Fatalf("same-port update: port = %d (created %d), want 9082 (created 9080)", same.Port, created.Port)
	}

	// An empty mask applies the body, port included.
	full, _, err := s.UpdateService(ctx, "proj", "us-central1", "hms", json.RawMessage(`{"port":9084}`), "")
	if err != nil {
		t.Fatalf("empty-mask update: %v", err)
	}
	if full.Port != 9084 {
		t.Fatalf("empty-mask port = %d, want 9084", full.Port)
	}
	if _, _, err := s.UpdateService(ctx, "proj", "us-central1", "hms", json.RawMessage(`{"port":9083}`), ""); !isCode(err, "InvalidArgument") {
		t.Fatalf("empty-mask update to the reserved port = %v, want InvalidArgument", err)
	}
}

// TestRequestedPortValidation covers the input contract: 1..65535, an integer,
// and the base --hms-port stays reserved for the legacy shared catalog.
func TestRequestedPortValidation(t *testing.T) {
	ctx := context.Background()
	s := NewService(metastorestore.NewMemoryStore(), WithHMSPort(9083))

	for _, tc := range []struct {
		name string
		body string
	}{
		{"fractional", `{"port":9080.5}`},
		{"negative", `{"port":-1}`},
		{"above 65535", `{"port":70000}`},
		{"string", `{"port":"9080"}`},
		{"legacy base", `{"port":9083}`},
	} {
		if _, _, err := s.CreateService(ctx, "proj", "us-central1", "bad", json.RawMessage(tc.body)); !isCode(err, "InvalidArgument") {
			t.Errorf("%s: create = %v, want InvalidArgument", tc.name, err)
		}
		if _, _, err := s.UpdateService(ctx, "proj", "us-central1", "bad", json.RawMessage(tc.body), "port"); !isCode(err, "InvalidArgument") {
			t.Errorf("%s: update = %v, want InvalidArgument", tc.name, err)
		}
	}

	// The reserved port follows --hms-port, not a literal 9083.
	custom := NewService(metastorestore.NewMemoryStore(), WithHMSPort(10000))
	if _, _, err := custom.CreateService(ctx, "proj", "us-central1", "svc", json.RawMessage(`{"port":10000}`)); !isCode(err, "InvalidArgument") {
		t.Fatalf("custom base port = %v, want InvalidArgument", err)
	}
	if _, _, err := custom.CreateService(ctx, "proj", "us-central1", "svc", json.RawMessage(`{"port":9083}`)); err != nil {
		t.Fatalf("9083 with a custom base = %v, want accepted", err)
	}
}

// TestRequestedPortConflict verifies two Services cannot claim one port: the
// listener is the catalog selector, so a duplicate would serve the wrong
// catalog. The conflict names the holder and frees when that Service goes away.
func TestRequestedPortConflict(t *testing.T) {
	ctx := context.Background()
	s := NewService(metastorestore.NewMemoryStore(), WithHMSPort(9083))

	if _, _, err := s.CreateService(ctx, "proj", "us-central1", "one", json.RawMessage(`{"port":9080}`)); err != nil {
		t.Fatalf("create one: %v", err)
	}
	if _, _, err := s.CreateService(ctx, "proj", "us-central1", "two", json.RawMessage(`{"port":9080}`)); !isCode(err, "AlreadyExists") {
		t.Fatalf("duplicate create = %v, want AlreadyExists", err)
	} else if !strings.Contains(err.Error(), "one") {
		t.Fatalf("conflict error %q does not name the holder", err)
	}
	// The failed create stores nothing.
	if _, err := s.GetService(ctx, "proj", "us-central1", "two"); !isCode(err, "NotFound") {
		t.Fatalf("second service after a rejected create = %v, want NotFound", err)
	}

	// The same rule applies to an update, across project/location scopes.
	two, _, err := s.CreateService(ctx, "other", "europe-west1", "two", nil)
	if err != nil {
		t.Fatalf("create two elsewhere: %v", err)
	}
	if _, _, err := s.UpdateService(ctx, "other", "europe-west1", "two", json.RawMessage(`{"port":9080}`), "port"); !isCode(err, "AlreadyExists") {
		t.Fatalf("duplicate update = %v, want AlreadyExists", err)
	}
	if got, _ := s.GetService(ctx, "other", "europe-west1", "two"); got.Port != two.Port {
		t.Fatalf("rejected update moved the port to %d, want %d", got.Port, two.Port)
	}

	// Deleting the holder releases the port.
	if _, err := s.DeleteService(ctx, "proj", "us-central1", "one"); err != nil {
		t.Fatalf("delete one: %v", err)
	}
	moved, _, err := s.UpdateService(ctx, "other", "europe-west1", "two", json.RawMessage(`{"port":9080}`), "port")
	if err != nil {
		t.Fatalf("move to a released port: %v", err)
	}
	if moved.Port != 9080 {
		t.Fatalf("port = %d after the holder was deleted, want 9080", moved.Port)
	}
}

// TestRequestedPortConcurrentCreate verifies port selection and the store write
// are one critical section: eight concurrent creates asking for the same port
// yield exactly one Service and seven AlreadyExists, never two Services sharing
// a listener.
func TestRequestedPortConcurrentCreate(t *testing.T) {
	ctx := context.Background()
	s := NewService(metastorestore.NewMemoryStore(), WithHMSPort(9083))

	const n = 8
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, err := s.CreateService(ctx, "proj", "us-central1",
				"svc-"+strconv.Itoa(i), json.RawMessage(`{"port":9080}`))
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)

	created, conflicts := 0, 0
	for err := range errs {
		switch {
		case err == nil:
			created++
		case isCode(err, "AlreadyExists"):
			conflicts++
		default:
			t.Fatalf("concurrent create = %v, want nil or AlreadyExists", err)
		}
	}
	if created != 1 || conflicts != n-1 {
		t.Fatalf("created=%d conflicts=%d, want 1 and %d", created, conflicts, n-1)
	}
	if services, err := s.store.ListAllServices(ctx); err != nil || len(services) != 1 {
		t.Fatalf("stored services = %d (err %v), want 1", len(services), err)
	}
}
