package managedkafka

import (
	"context"
	"testing"
	"time"

	managedkafkapb "cloud.google.com/go/managedkafka/apiv1/managedkafkapb"
	"google.golang.org/protobuf/types/known/emptypb"

	"jaiscloud/internal/clock"
	"jaiscloud/internal/gcp/lro"
	core "jaiscloud/internal/gcp/service/managedkafka"
	mkstore "jaiscloud/internal/gcp/store/managedkafka"
)

// freezeClock pins the global clock to at and restores real time on cleanup.
func freezeClock(t *testing.T, at time.Time) {
	t.Helper()
	clock.SetGlobalClock(clock.FixedClock{T: at})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })
}

// newAsyncService returns a gRPC service in async LRO mode with a 30s window.
func newAsyncService() *Service {
	return NewService(core.NewService(mkstore.NewMemoryStore(),
		core.WithLROMode(lro.Mode{Enabled: true, Delay: 30 * time.Second})), "proj")
}

// TestAsyncCreateInFlightThenResolve verifies a create in async mode returns an
// in-flight operation with no result/endTime, and that ResolveOperation settles
// it once the delay has elapsed.
func TestAsyncCreateInFlightThenResolve(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	ctx := context.Background()
	s := newAsyncService()

	op, err := s.CreateCluster(ctx, &managedkafkapb.CreateClusterRequest{
		Parent:    "projects/proj/locations/us-central1",
		ClusterId: "c1",
		Cluster:   &managedkafkapb.Cluster{Labels: map[string]string{"env": "test"}},
	})
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if op.GetDone() {
		t.Fatalf("async create must be in flight: %v", op)
	}
	if op.GetResponse() != nil || op.GetResult() != nil {
		t.Fatalf("in-flight operation must carry no result: %v", op.GetResult())
	}
	var meta managedkafkapb.OperationMetadata
	if err := op.GetMetadata().UnmarshalTo(&meta); err != nil {
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

	// Past the delay the resolver settles it with the cluster as its response.
	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(30 * time.Second)})
	resolved, handled, err = s.ResolveOperation(ctx, op.GetName())
	if err != nil || !handled {
		t.Fatalf("ResolveOperation: handled=%v err=%v", handled, err)
	}
	if !resolved.GetDone() || resolved.GetResponse() == nil {
		t.Fatalf("resolved operation = %v, want done with a response", resolved)
	}
	settled := &managedkafkapb.Cluster{}
	if err := resolved.GetResponse().UnmarshalTo(settled); err != nil {
		t.Fatalf("unmarshal resolved Cluster: %v", err)
	}
	if settled.GetName() != "projects/proj/locations/us-central1/clusters/c1" {
		t.Fatalf("resolved cluster name = %q", settled.GetName())
	}
	if settled.GetLabels()["env"] != "test" {
		t.Fatalf("resolved cluster labels = %v", settled.GetLabels())
	}
}

// TestAsyncDeleteResolvesToEmpty verifies a settled delete operation renders
// google.protobuf.Empty as its response.
func TestAsyncDeleteResolvesToEmpty(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	ctx := context.Background()
	s := newAsyncService()

	if _, err := s.CreateCluster(ctx, &managedkafkapb.CreateClusterRequest{
		Parent:    "projects/proj/locations/us-central1",
		ClusterId: "c1",
		Cluster:   &managedkafkapb.Cluster{},
	}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	op, err := s.DeleteCluster(ctx, &managedkafkapb.DeleteClusterRequest{
		Name: "projects/proj/locations/us-central1/clusters/c1",
	})
	if err != nil {
		t.Fatalf("DeleteCluster: %v", err)
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
// and a name outside the managedkafka shape are both handled=false.
func TestResolveOperationDeclinesUnknownNames(t *testing.T) {
	ctx := context.Background()
	s := newAsyncService()

	resolved, handled, err := s.ResolveOperation(ctx, "projects/proj/locations/global/operations/does-not-exist")
	if handled || err != nil || resolved != nil {
		t.Fatalf("unknown operation: handled=%v err=%v op=%v; want fall-through", handled, err, resolved)
	}
	if _, handled, _ := s.ResolveOperation(ctx, "projects/proj/regions/us-central1/operations/x"); handled {
		t.Fatalf("regional name must not be handled by managedkafka")
	}
	if _, handled, _ := s.ResolveOperation(ctx, "operations/top-level"); handled {
		t.Fatalf("top-level name must not be handled by managedkafka")
	}
}
