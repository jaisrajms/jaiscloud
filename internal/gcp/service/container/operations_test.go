package container

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"jaiscloud/internal/clock"
	containerstore "jaiscloud/internal/gcp/store/container"
	"jaiscloud/internal/model"
)

// freezeClock pins the process clock for a test that exercises the simulated
// operation window, and restores wall time when the test ends.
func freezeClock(t *testing.T, at time.Time) {
	t.Helper()
	clock.SetGlobalClock(clock.FixedClock{T: at})
	t.Cleanup(func() { clock.SetGlobalClock(clock.RealClock{}) })
}

// advanceClock moves a frozen clock forward.
func advanceClock(t *testing.T, at time.Time) {
	t.Helper()
	clock.SetGlobalClock(clock.FixedClock{T: at})
}

// newWindowedService returns a core whose operations stay in flight for window.
func newWindowedService(window time.Duration, opts ...Option) *Service {
	return NewService(containerstore.NewMemoryStore(),
		append([]Option{WithOperationWindow(window)}, opts...)...)
}

// wantIncompatibleOperation asserts err is the 400 FAILED_PRECONDITION real GKE
// returns for a mutation that overlaps a running operation, naming blockingOp.
func wantIncompatibleOperation(t *testing.T, err error, blockingOp string) {
	t.Helper()
	if err == nil {
		t.Fatal("want a FailedPrecondition conflict, got nil")
	}
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("want *model.ProviderError, got %T (%v)", err, err)
	}
	if pe.Code != "FailedPrecondition" || pe.HTTPStatus != 400 || pe.Status != "FAILED_PRECONDITION" {
		t.Fatalf("error = %+v, want FailedPrecondition/400/FAILED_PRECONDITION", pe)
	}
	if !strings.HasPrefix(pe.Message, "Cluster is running incompatible operation ") {
		t.Fatalf("message = %q, want the real GKE conflict message", pe.Message)
	}
	if !strings.Contains(pe.Message, blockingOp) {
		t.Fatalf("message = %q, want it to name %q", pe.Message, blockingOp)
	}
}

// wantResourceExhausted asserts err is the 429 concurrent-operations quota
// rejection.
func wantResourceExhausted(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("want a ResourceExhausted throttle, got nil")
	}
	var pe *model.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("want *model.ProviderError, got %T (%v)", err, err)
	}
	if pe.Code != "ResourceExhausted" || pe.HTTPStatus != 429 || pe.Status != "RESOURCE_EXHAUSTED" {
		t.Fatalf("error = %+v, want ResourceExhausted/429/RESOURCE_EXHAUSTED", pe)
	}
}

// TestOperationWindowDefaultsToInstantDone pins the behaviour every existing
// client relies on: without a simulated window nothing is ever in flight, so
// back-to-back mutations are all admitted and each operation is DONE.
func TestOperationWindowDefaultsToInstantDone(t *testing.T) {
	ctx := context.Background()
	s := newTestService()

	op, err := s.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{Name: "c1"})
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if op.Status != containerstore.OperationStatusDone || !op.EndTime.Equal(op.StartTime) {
		t.Fatalf("create op = %+v, want instant DONE", op)
	}
	for i := 0; i < 3; i++ {
		if _, err := s.SetLabels(ctx, "p", "us-central1", "c1", map[string]string{"n": "v"}); err != nil {
			t.Fatalf("mutation %d rejected with no window open: %v", i, err)
		}
	}
}

// TestInFlightOperationRejectsClusterMutation is the DC15 core probe: a
// cluster-level operation runs for one window, an overlapping cluster mutation
// is the 400 FAILED_PRECONDITION real GKE answers, and once the window elapses
// the operation settles DONE and the mutation is admitted.
func TestInFlightOperationRejectsClusterMutation(t *testing.T) {
	ctx := context.Background()
	at := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, at)
	s := newWindowedService(10 * time.Minute)

	createOp, err := s.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{Name: "c1"})
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if createOp.Status != containerstore.OperationStatusRunning {
		t.Fatalf("create op status = %q, want RUNNING", createOp.Status)
	}
	if !createOp.EndTime.IsZero() {
		t.Fatalf("running operation carries endTime %v; real GKE sets it on completion", createOp.EndTime)
	}
	polled, err := s.GetOperation(ctx, "p", "us-central1", createOp.Name)
	if err != nil {
		t.Fatalf("GetOperation: %v", err)
	}
	if polled.Status != containerstore.OperationStatusRunning {
		t.Fatalf("polled status = %q, want RUNNING while the window is open", polled.Status)
	}

	_, err = s.SetAddonsConfig(ctx, "p", "us-central1", "c1", containerstore.AddonsConfig{})
	wantIncompatibleOperation(t, err, createOp.Name)

	// The conflict must not have applied the change or recorded an operation.
	ops, err := s.ListOperations(ctx, "p", "us-central1")
	if err != nil {
		t.Fatalf("ListOperations: %v", err)
	}
	if len(ops) != 1 {
		t.Fatalf("operations = %d, want the rejected mutation recorded none", len(ops))
	}

	// One window later the operation settles and the same mutation is admitted.
	advanceClock(t, at.Add(10*time.Minute))
	settled, err := s.GetOperation(ctx, "p", "us-central1", createOp.Name)
	if err != nil {
		t.Fatalf("GetOperation after window: %v", err)
	}
	if settled.Status != containerstore.OperationStatusDone {
		t.Fatalf("settled status = %q, want DONE", settled.Status)
	}
	if want := at.Add(10 * time.Minute); !settled.EndTime.Equal(want) {
		t.Fatalf("settled endTime = %v, want %v", settled.EndTime, want)
	}
	if _, err := s.SetAddonsConfig(ctx, "p", "us-central1", "c1", containerstore.AddonsConfig{}); err != nil {
		t.Fatalf("mutation after settle: %v", err)
	}
}

// TestNodePoolOperationsConflictPerPool proves the target granularity: one
// operation per node pool, so a different pool of the same cluster still
// mutates in parallel while a cluster-level change waits for both.
func TestNodePoolOperationsConflictPerPool(t *testing.T) {
	ctx := context.Background()
	at := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, at)
	s := newWindowedService(10 * time.Minute)

	if _, err := s.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{Name: "c1"}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	// Let the cluster create settle so pool operations are admitted.
	advanceClock(t, at.Add(10*time.Minute))

	opA, err := s.CreateNodePool(ctx, "p", "us-central1", "c1",
		containerstore.NodePool{Name: "pool-a", InitialNodeCount: 1})
	if err != nil {
		t.Fatalf("CreateNodePool(pool-a): %v", err)
	}
	if opA.Status != containerstore.OperationStatusRunning {
		t.Fatalf("pool-a operation = %+v, want RUNNING", opA)
	}

	// A different pool is a different target and is admitted.
	opB, err := s.CreateNodePool(ctx, "p", "us-central1", "c1",
		containerstore.NodePool{Name: "pool-b", InitialNodeCount: 1})
	if err != nil {
		t.Fatalf("CreateNodePool(pool-b) overlapped pool-a: %v", err)
	}
	if opB.Status != containerstore.OperationStatusRunning {
		t.Fatalf("pool-b operation = %+v, want RUNNING", opB)
	}

	// The same pool is rejected, naming the operation already running on it.
	_, err = s.SetNodePoolSize(ctx, "p", "us-central1", "c1", "pool-a", 3)
	wantIncompatibleOperation(t, err, opA.Name)

	// A cluster-level change overlaps both pool operations.
	_, err = s.SetLegacyAbac(ctx, "p", "us-central1", "c1", true)
	wantIncompatibleOperation(t, err, "")
}

// TestConcurrentOperationQuotaThrottles opens the simulated load bound: with a
// quota of one operation per project/location, a second in-flight operation is
// the 429 RESOURCE_EXHAUSTED real GKE returns, while another location keeps its
// own budget.
func TestConcurrentOperationQuotaThrottles(t *testing.T) {
	ctx := context.Background()
	at := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, at)
	s := newWindowedService(10*time.Minute, WithConcurrentOperationQuota(1))

	if _, err := s.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{Name: "c1"}); err != nil {
		t.Fatalf("CreateCluster(c1): %v", err)
	}
	_, err := s.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{Name: "c2"})
	wantResourceExhausted(t, err)

	// The quota is per project/location, not global.
	if _, err := s.CreateCluster(ctx, "p", "us-east1", containerstore.Cluster{Name: "c2"}); err != nil {
		t.Fatalf("CreateCluster in another location: %v", err)
	}

	// Once the window elapses the slot is free again.
	advanceClock(t, at.Add(10*time.Minute))
	if _, err := s.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{Name: "c2"}); err != nil {
		t.Fatalf("CreateCluster after window: %v", err)
	}
}

// TestConcurrentMutationsAdmitExactlyOneOperation proves the target check and
// the operation record are one atomic step: a burst of overlapping mutations of
// one cluster admits exactly one and rejects the rest as conflicts.
func TestConcurrentMutationsAdmitExactlyOneOperation(t *testing.T) {
	ctx := context.Background()
	at := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	freezeClock(t, at)
	s := newWindowedService(time.Hour)

	if _, err := s.CreateCluster(ctx, "p", "us-central1", containerstore.Cluster{Name: "c1"}); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	advanceClock(t, at.Add(time.Hour))

	const n = 8
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = s.SetLabels(ctx, "p", "us-central1", "c1", map[string]string{"k": "v"})
		}(i)
	}
	wg.Wait()

	admitted, rejected := 0, 0
	for _, err := range errs {
		switch {
		case err == nil:
			admitted++
		case isFailedPrecondition(err):
			rejected++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if admitted != 1 || rejected != n-1 {
		t.Fatalf("admitted %d, rejected %d; want exactly one mutation admitted", admitted, rejected)
	}
}

func isFailedPrecondition(err error) bool {
	var pe *model.ProviderError
	return errors.As(err, &pe) && pe.Code == "FailedPrecondition"
}
