package workflows

import (
	"context"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/lro"
	core "jaiscloud/internal/gcp/service/workflows"
	workflowsstore "jaiscloud/internal/gcp/store/workflows"
)

// freezeClock pins the global clock to at and restores real time on cleanup.
func freezeClock(t *testing.T, at time.Time) {
	t.Helper()
	clock.SetGlobalClock(clock.FixedClock{T: at})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })
}

// TestRestAsyncCreateThenGetOperation verifies the REST surface omits
// response/endTime while an async operation is in flight and includes them once
// GetOperation settles it.
func TestRestAsyncCreateThenGetOperation(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	ctx := context.Background()
	p := NewProvider(core.NewService(workflowsstore.NewMemoryStore(),
		core.WithLROMode(lro.Mode{Enabled: true, Delay: 30 * time.Second})), "default-proj")

	cresp, err := p.CreateWorkflow(ctx, nr(map[string]any{
		"project": "proj", "location": "us-central1", "workflowId": "wf1",
		"body": map[string]any{"sourceContents": source},
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
	if _, ok := md["endTime"]; ok {
		t.Fatalf("in-flight metadata must not include endTime: %v", md)
	}
	opName, _ := cresp.Data["name"].(string)
	if opName == "" {
		t.Fatalf("operation name missing: %v", cresp.Data)
	}

	get := nr(map[string]any{"project": "proj", "name": opName})
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
	if response == nil || response["name"] != "projects/proj/locations/us-central1/workflows/wf1" {
		t.Fatalf("settled response = %v", response)
	}
	if md, _ := gresp.Data["metadata"].(map[string]any); md == nil || md["endTime"] == nil {
		t.Fatalf("settled metadata missing endTime: %v", gresp.Data)
	}
}
