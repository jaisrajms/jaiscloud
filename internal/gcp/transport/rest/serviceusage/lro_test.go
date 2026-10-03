package serviceusage

import (
	"context"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/lro"
	core "jaiscloud/internal/gcp/service/serviceusage"
	"jaiscloud/internal/store"
)

// TestResolveOperationAsync verifies the REST cross-service resolver renders an
// in-flight Service Usage operation without a response, settles it with the
// typed response after the delay, and declines names it does not own.
func TestResolveOperationAsync(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	clock.SetGlobalClock(clock.FixedClock{T: t0})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })
	ctx := context.Background()
	c := core.NewService(store.NewMemoryResourceStore(),
		core.WithLROMode(lro.Mode{Enabled: true, Delay: 30 * time.Second}))
	p := NewProvider(c, "proj")

	_, op, err := c.EnableAPI(ctx, "proj", "run.googleapis.com")
	if err != nil {
		t.Fatalf("EnableAPI: %v", err)
	}
	resolved, handled, err := p.ResolveOperation(ctx, "proj", op.Name)
	if err != nil || !handled {
		t.Fatalf("ResolveOperation: handled=%v err=%v", handled, err)
	}
	if done, _ := resolved["done"].(bool); done {
		t.Fatalf("in-flight operation must be done:false: %v", resolved)
	}
	if _, hasResp := resolved["response"]; hasResp {
		t.Fatalf("in-flight operation must not carry response: %v", resolved)
	}

	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(30 * time.Second)})
	resolved, handled, err = p.ResolveOperation(ctx, "proj", op.Name)
	if err != nil || !handled {
		t.Fatalf("ResolveOperation after delay: handled=%v err=%v", handled, err)
	}
	if done, _ := resolved["done"].(bool); !done {
		t.Fatalf("settled operation must be done:true: %v", resolved)
	}
	resp, _ := resolved["response"].(map[string]any)
	svc, _ := resp["service"].(map[string]any)
	if svc["name"] != "projects/proj/services/run.googleapis.com" {
		t.Fatalf("settled service name = %v (response %v)", svc["name"], resp)
	}
	if svc["state"] != "ENABLED" {
		t.Fatalf("settled service state = %v", svc["state"])
	}

	if _, handled, err := p.ResolveOperation(ctx, "proj", "operations/missing"); handled || err != nil {
		t.Fatalf("unknown id: handled=%v err=%v, want declined", handled, err)
	}
	if _, handled, _ := p.ResolveOperation(ctx, "proj", "projects/p/locations/us/operations/x"); handled {
		t.Fatalf("location-scoped name must be declined")
	}
}
