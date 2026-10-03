package functions

import (
	"context"
	"testing"
	"time"

	functionspb "cloud.google.com/go/functions/apiv1/functionspb"
	apiv2functionspb "cloud.google.com/go/functions/apiv2/functionspb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

// newAsyncV1 returns a v1 gRPC service in async LRO mode with a 30s window.
func newAsyncV1() *Service {
	return NewService(core.NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		core.WithBlobs(blobfs.NewMemoryBlobStore()),
		core.WithLROMode(lro.Mode{Enabled: true, Delay: 30 * time.Second})), "proj", "http://localhost:8080")
}

// newAsyncV2 returns a v2 gRPC service in async LRO mode with a 30s window.
func newAsyncV2() *ServiceV2 {
	return NewServiceV2(core.NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		core.WithBlobs(blobfs.NewMemoryBlobStore()),
		core.WithLROMode(lro.Mode{Enabled: true, Delay: 30 * time.Second})), "proj", "http://localhost:8080")
}

// TestAsyncCreateV1ResolvesAfterDelay verifies a v1 async create returns an
// in-flight operation with no result/updateTime, and that ResolveOperation
// settles it with the CloudFunction response once the delay has elapsed.
func TestAsyncCreateV1ResolvesAfterDelay(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	ctx := context.Background()
	s := newAsyncV1()

	op, err := s.CreateFunction(ctx, createV1Req("f1"))
	if err != nil {
		t.Fatalf("CreateFunction: %v", err)
	}
	if op.GetDone() {
		t.Fatalf("async create must be in flight: %v", op)
	}
	if op.GetResult() != nil || op.GetResponse() != nil {
		t.Fatalf("in-flight operation must carry no result: %v", op.GetResult())
	}
	var meta functionspb.OperationMetadataV1
	if err := op.GetMetadata().UnmarshalTo(&meta); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	if meta.GetUpdateTime() != nil {
		t.Fatalf("in-flight metadata must not set updateTime: %v", meta.GetUpdateTime())
	}

	resolved, handled, err := s.ResolveOperation(ctx, op.GetName())
	if err != nil || !handled {
		t.Fatalf("ResolveOperation: handled=%v err=%v", handled, err)
	}
	if resolved.GetDone() {
		t.Fatalf("resolved operation before delay must be in flight")
	}

	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(30 * time.Second)})
	resolved, handled, err = s.ResolveOperation(ctx, op.GetName())
	if err != nil || !handled {
		t.Fatalf("ResolveOperation: handled=%v err=%v", handled, err)
	}
	if !resolved.GetDone() || resolved.GetResponse() == nil {
		t.Fatalf("resolved operation = %v, want done with a response", resolved)
	}
	var fn functionspb.CloudFunction
	if err := resolved.GetResponse().UnmarshalTo(&fn); err != nil {
		t.Fatalf("unmarshal resolved CloudFunction: %v", err)
	}
	if fn.GetName() != "projects/proj/locations/us-central1/functions/f1" {
		t.Fatalf("resolved function name = %q", fn.GetName())
	}

	// A top-level name for an unknown id is still a NotFound.
	if _, handled, err := s.ResolveOperation(ctx, "operations/missing"); !handled || status.Code(err) != codes.NotFound {
		t.Fatalf("unknown v1 op: handled=%v err=%v, want handled+NotFound", handled, err)
	}
}

// TestAsyncCreateV2ResolvesAfterDelay verifies a v2 async create returns an
// in-flight operation with no result/endTime, and that ResolveOperation settles
// it with the Function response once the delay has elapsed.
func TestAsyncCreateV2ResolvesAfterDelay(t *testing.T) {
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, t0)
	ctx := context.Background()
	s := newAsyncV2()

	op, err := s.CreateFunction(ctx, &apiv2functionspb.CreateFunctionRequest{
		Parent:     "projects/proj/locations/us-central1",
		FunctionId: "f2",
		Function: &apiv2functionspb.Function{
			BuildConfig: &apiv2functionspb.BuildConfig{Runtime: "nodejs20", EntryPoint: "handler"},
		},
	})
	if err != nil {
		t.Fatalf("CreateFunction: %v", err)
	}
	if op.GetDone() {
		t.Fatalf("async create must be in flight: %v", op)
	}
	if op.GetResult() != nil || op.GetResponse() != nil {
		t.Fatalf("in-flight operation must carry no result: %v", op.GetResult())
	}
	var meta apiv2functionspb.OperationMetadata
	if err := op.GetMetadata().UnmarshalTo(&meta); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	if meta.GetEndTime() != nil {
		t.Fatalf("in-flight metadata must not set endTime: %v", meta.GetEndTime())
	}

	clock.SetGlobalClock(clock.FixedClock{T: t0.Add(30 * time.Second)})
	resolved, handled, err := s.ResolveOperation(ctx, op.GetName())
	if err != nil || !handled {
		t.Fatalf("ResolveOperation: handled=%v err=%v", handled, err)
	}
	if !resolved.GetDone() || resolved.GetResponse() == nil {
		t.Fatalf("resolved operation = %v, want done with a response", resolved)
	}
	var fn apiv2functionspb.Function
	if err := resolved.GetResponse().UnmarshalTo(&fn); err != nil {
		t.Fatalf("unmarshal resolved Function: %v", err)
	}
	if fn.GetName() != "projects/proj/locations/us-central1/functions/f2" {
		t.Fatalf("resolved function name = %q", fn.GetName())
	}

	// An unknown id under a location-scoped name is not claimed by functions.
	if _, handled, _ := s.ResolveOperation(ctx, "projects/proj/locations/us-central1/operations/missing"); handled {
		t.Fatal("v2 resolver should not claim an unknown operation id")
	}
}
