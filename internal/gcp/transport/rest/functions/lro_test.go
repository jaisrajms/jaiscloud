package functions

import (
	"context"
	"testing"
	"time"

	"jaiscloud/internal/blobfs"
	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/lro"
	core "jaiscloud/internal/gcp/service/functions"
	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/store"
)

// freezeClock pins the global clock to at and restores real time on cleanup.
func freezeClock(t *testing.T, at time.Time) {
	t.Helper()
	clock.SetGlobalClock(clock.FixedClock{T: at})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })
}

// newAsyncProvider returns a REST provider in async LRO mode with a 30s window.
func newAsyncProvider() *Provider {
	return NewProvider(core.NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		core.WithBlobs(blobfs.NewMemoryBlobStore()),
		core.WithLROMode(lro.Mode{Enabled: true, Delay: 30 * time.Second})), "proj")
}

// TestRESTAsyncCreateThenGetOperation verifies the REST surface omits
// response/updateTime while an async operation is in flight and includes them
// once GetOperation settles it.
func TestRESTAsyncCreateThenGetOperation(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	ctx := context.Background()
	p := newAsyncProvider()

	cresp, err := p.CreateFunction(ctx, newNR(map[string]any{
		"location":   "us-central1",
		"functionId": "hello",
		"body":       map[string]any{"runtime": "nodejs20", "entryPoint": "handler"},
	}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if cresp.Data["done"] != false {
		t.Fatalf("async create done = %v, want false", cresp.Data["done"])
	}
	if _, ok := cresp.Data["response"]; ok {
		t.Fatalf("in-flight operation must not include response: %v", cresp.Data)
	}
	md, _ := cresp.Data["metadata"].(map[string]any)
	if md == nil {
		t.Fatalf("metadata missing: %v", cresp.Data)
	}
	if _, ok := md["updateTime"]; ok {
		t.Fatalf("in-flight v1 metadata must not include updateTime: %v", md)
	}
	opName, _ := cresp.Data["name"].(string)
	if opName == "" {
		t.Fatalf("operation name missing: %v", cresp.Data)
	}

	get := newNR(map[string]any{"location": "us-central1", "name": opName})
	gresp, err := p.GetOperation(ctx, get)
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}
	if gresp.Data["done"] != false {
		t.Fatalf("read before delay done = %v, want false", gresp.Data["done"])
	}
	if _, ok := gresp.Data["response"]; ok {
		t.Fatalf("pending read must not include response: %v", gresp.Data)
	}

	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(30 * time.Second)})
	gresp, err = p.GetOperation(ctx, get)
	if err != nil {
		t.Fatalf("get operation after delay: %v", err)
	}
	if gresp.Data["done"] != true {
		t.Fatalf("read after delay done = %v, want true", gresp.Data["done"])
	}
	response, _ := gresp.Data["response"].(map[string]any)
	if response == nil || response["name"] != "projects/proj/locations/us-central1/functions/hello" {
		t.Fatalf("settled response = %v", response)
	}
	if md, _ := gresp.Data["metadata"].(map[string]any); md == nil || md["updateTime"] == nil {
		t.Fatalf("settled metadata missing updateTime: %v", gresp.Data)
	}
}
