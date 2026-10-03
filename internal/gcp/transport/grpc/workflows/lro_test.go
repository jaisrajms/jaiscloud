package workflows

import (
	"context"
	"testing"
	"time"

	workflowspb "cloud.google.com/go/workflows/apiv1/workflowspb"
	"google.golang.org/protobuf/types/known/emptypb"

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

// newAsyncService returns a gRPC service in async LRO mode with a 30s window.
func newAsyncService() *Service {
	return NewService(core.NewService(workflowsstore.NewMemoryStore(),
		core.WithLROMode(lro.Mode{Enabled: true, Delay: 30 * time.Second})), "default-proj")
}

// TestAsyncCreateInFlightThenResolve verifies a create in async mode returns an
// in-flight operation with no result/endTime, and that ResolveOperation settles
// it once the delay has elapsed.
func TestAsyncCreateInFlightThenResolve(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	ctx := context.Background()
	s := newAsyncService()

	op, err := s.CreateWorkflow(ctx, &workflowspb.CreateWorkflowRequest{
		Parent:     "projects/proj/locations/us-central1",
		WorkflowId: "wf1",
		Workflow:   &workflowspb.Workflow{SourceCode: &workflowspb.Workflow_SourceContents{SourceContents: source}},
	})
	if err != nil {
		t.Fatalf("CreateWorkflow: %v", err)
	}
	if op.GetDone() {
		t.Fatalf("async create must be in flight: %v", op)
	}
	if op.GetResponse() != nil || op.GetResult() != nil {
		t.Fatalf("in-flight operation must carry no result: %v", op.GetResult())
	}
	meta := &workflowspb.OperationMetadata{}
	if err := op.GetMetadata().UnmarshalTo(meta); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	if meta.GetEndTime() != nil {
		t.Fatalf("in-flight metadata must not set endTime: %v", meta.GetEndTime())
	}

	// Before the delay the resolver reports the operation still in flight.
	resolved, handled, err := s.ResolveOperation(ctx, op.GetName())
	if err != nil || !handled {
		t.Fatalf("ResolveOperation: handled=%v err=%v", handled, err)
	}
	if resolved.GetDone() {
		t.Fatalf("resolved operation before delay must be in flight")
	}

	// Past the delay the resolver settles it with the workflow as its response.
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(30 * time.Second)})
	resolved, handled, err = s.ResolveOperation(ctx, op.GetName())
	if err != nil || !handled {
		t.Fatalf("ResolveOperation: handled=%v err=%v", handled, err)
	}
	if !resolved.GetDone() || resolved.GetResponse() == nil {
		t.Fatalf("resolved operation = %v, want done with a response", resolved)
	}
	settled := &workflowspb.Workflow{}
	if err := resolved.GetResponse().UnmarshalTo(settled); err != nil {
		t.Fatalf("unmarshal resolved Workflow: %v", err)
	}
	if settled.GetName() != workflowName {
		t.Fatalf("resolved workflow name = %q, want %q", settled.GetName(), workflowName)
	}
}

// TestAsyncDeleteResolvesToEmpty verifies a settled delete operation renders
// google.protobuf.Empty as its response.
func TestAsyncDeleteResolvesToEmpty(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	ctx := context.Background()
	s := newAsyncService()

	if _, err := s.CreateWorkflow(ctx, &workflowspb.CreateWorkflowRequest{
		Parent:     "projects/proj/locations/us-central1",
		WorkflowId: "wf1",
		Workflow:   &workflowspb.Workflow{SourceCode: &workflowspb.Workflow_SourceContents{SourceContents: source}},
	}); err != nil {
		t.Fatalf("CreateWorkflow: %v", err)
	}

	op, err := s.DeleteWorkflow(ctx, &workflowspb.DeleteWorkflowRequest{Name: workflowName})
	if err != nil {
		t.Fatalf("DeleteWorkflow: %v", err)
	}
	if op.GetDone() {
		t.Fatalf("async delete must be in flight")
	}

	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(30 * time.Second)})
	resolved, handled, err := s.ResolveOperation(ctx, op.GetName())
	if err != nil || !handled {
		t.Fatalf("ResolveOperation: handled=%v err=%v", handled, err)
	}
	if !resolved.GetDone() || resolved.GetResponse() == nil {
		t.Fatalf("resolved delete = %v, want done with a response", resolved)
	}
	empty := &emptypb.Empty{}
	if err := resolved.GetResponse().UnmarshalTo(empty); err != nil {
		t.Fatalf("resolved delete response is not Empty: %v", err)
	}
}

// TestResolveOperationDeclinesUnknownNames verifies the resolver leaves names it
// does not own to the shared terminal stub: a well-formed but absent operation
// and a name outside the workflows shape are both handled=false.
func TestResolveOperationDeclinesUnknownNames(t *testing.T) {
	ctx := context.Background()
	s := newAsyncService()

	resolved, handled, err := s.ResolveOperation(ctx, "projects/proj/locations/global/operations/does-not-exist")
	if handled || err != nil || resolved != nil {
		t.Fatalf("unknown operation: handled=%v err=%v op=%v; want fall-through", handled, err, resolved)
	}
	if _, handled, _ := s.ResolveOperation(ctx, "projects/proj/regions/us-central1/operations/x"); handled {
		t.Fatalf("regional name must not be handled by workflows")
	}
	if _, handled, _ := s.ResolveOperation(ctx, "operations/top-level"); handled {
		t.Fatalf("top-level name must not be handled by workflows")
	}
}
