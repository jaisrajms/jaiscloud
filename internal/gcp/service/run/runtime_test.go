package run

import (
	"context"
	"testing"

	runstore "jaiscloud/internal/gcp/store/run"
	"jaiscloud/internal/store"
)

// recordingRuntime records the RuntimeManager lifecycle calls the core makes.
type recordingRuntime struct {
	ensured  []string
	removed  []string
	services []string
	reset    bool
}

func (r *recordingRuntime) EnsureRevision(_ context.Context, _ runstore.Service, rev runstore.Revision) error {
	r.ensured = append(r.ensured, rev.ID)
	return nil
}

func (r *recordingRuntime) RemoveRevision(_ context.Context, rev runstore.Revision) error {
	r.removed = append(r.removed, rev.ID)
	return nil
}

func (r *recordingRuntime) RemoveService(_ context.Context, svc runstore.Service) error {
	r.services = append(r.services, svc.ID)
	return nil
}

func (r *recordingRuntime) Invoke(context.Context, InvocationRequest) (Invocation, error) {
	return Invocation{}, nil
}

func (r *recordingRuntime) Reset(context.Context) { r.reset = true }

func TestRuntimeLifecycleOnMutations(t *testing.T) {
	ctx := context.Background()
	rt := &recordingRuntime{}
	s := NewService(runstore.NewMemoryStore(), store.NewMemoryResourceStore(), WithRuntimeManager(rt))

	if _, err := s.CreateService(ctx, "p", "l", "svc", createRequest(), false); err != nil {
		t.Fatalf("create: %v", err)
	}
	if len(rt.ensured) != 1 || rt.ensured[0] != "svc-00001" {
		t.Fatalf("ensured after create = %v", rt.ensured)
	}

	// A non-template update must not churn revisions or runtimes.
	if _, err := s.UpdateService(ctx, "p", "l", "svc", map[string]any{"description": "x"}, "description", false, false); err != nil {
		t.Fatalf("non-template update: %v", err)
	}
	if len(rt.removed) != 0 || len(rt.ensured) != 1 {
		t.Fatalf("non-template update churned runtime: ensured=%v removed=%v", rt.ensured, rt.removed)
	}

	// A template-changing update tears down the serving revision then starts the
	// replacement.
	if _, err := s.UpdateService(ctx, "p", "l", "svc", createRequest(), "template", false, false); err != nil {
		t.Fatalf("template update: %v", err)
	}
	if len(rt.removed) != 1 || rt.removed[0] != "svc-00001" {
		t.Fatalf("removed after template update = %v, want [svc-00001]", rt.removed)
	}
	if len(rt.ensured) != 2 || rt.ensured[1] != "svc-00002" {
		t.Fatalf("ensured after template update = %v", rt.ensured)
	}

	if _, err := s.DeleteService(ctx, "p", "l", "svc", false, ""); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if len(rt.services) != 1 || rt.services[0] != "svc" {
		t.Fatalf("removed services = %v, want [svc]", rt.services)
	}

	s.Reset(ctx)
	if !rt.reset {
		t.Error("Reset did not reach the runtime manager")
	}
}
