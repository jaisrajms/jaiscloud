package functions

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	lambdaexec "jaiscloud/internal/executor/lambda"
	"jaiscloud/internal/gcp/gcperr"
	functionsstore "jaiscloud/internal/gcp/store/functions"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// blockingExecutor blocks every Invoke until releaseAll is called, so a test can
// hold an invocation in flight and observe the admission gate from a second
// goroutine.
type blockingExecutor struct {
	release     chan struct{}
	releaseOnce sync.Once
	mu          sync.Mutex
	entered     int
}

func newBlockingExecutor() *blockingExecutor {
	return &blockingExecutor{release: make(chan struct{})}
}

func (e *blockingExecutor) Invoke(ctx context.Context, req lambdaexec.InvokeRequest) (lambdaexec.InvokeResult, error) {
	e.mu.Lock()
	e.entered++
	e.mu.Unlock()
	select {
	case <-e.release:
		return lambdaexec.InvokeResult{Payload: req.Payload}, nil
	case <-ctx.Done():
		return lambdaexec.InvokeResult{}, ctx.Err()
	}
}
func (e *blockingExecutor) DeleteFunction(context.Context, string) {}
func (e *blockingExecutor) Reset(context.Context)                  {}
func (e *blockingExecutor) Close() error                           { return nil }

func (e *blockingExecutor) releaseAll() { e.releaseOnce.Do(func() { close(e.release) }) }

func (e *blockingExecutor) enteredCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.entered
}

// waitEntered waits until n invocations have entered the executor.
func waitEntered(t *testing.T, e *blockingExecutor, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if e.enteredCount() >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("executor entered %d times, want >= %d", e.enteredCount(), n)
}

// assertResourceExhausted asserts err is the GCP-native admission failure and
// that both transports resolve it the same way (RESOURCE_EXHAUSTED / HTTP 429 /
// codes.ResourceExhausted).
func assertResourceExhausted(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected a ResourceExhausted error, got nil")
	}
	var perr *model.ProviderError
	if !errors.As(err, &perr) {
		t.Fatalf("expected *model.ProviderError, got %T: %v", err, err)
	}
	if perr.Code != "ResourceExhausted" {
		t.Fatalf("code = %q, want ResourceExhausted (%v)", perr.Code, err)
	}
	if perr.HTTPStatus != 429 {
		t.Fatalf("http status = %d, want 429 (%v)", perr.HTTPStatus, err)
	}
	status, httpStatus := gcperr.Resolve(perr)
	if status != gcperr.ResourceExhausted || httpStatus != 429 {
		t.Fatalf("gcperr.Resolve = (%q, %d), want (RESOURCE_EXHAUSTED, 429)", status, httpStatus)
	}
	if code, ok := gcperr.GRPCCodeForStatus(status); !ok || code.String() != "ResourceExhausted" {
		t.Fatalf("gRPC code = %v (ok=%v), want ResourceExhausted", code, ok)
	}
}

func newAdmissionService(t *testing.T, accountCap int64) *Service {
	t.Helper()
	return NewService(functionsstore.NewMemoryStore(), store.NewMemoryResourceStore(),
		WithAccountConcurrencyLimit(accountCap))
}

func createAdmissionFn(t *testing.T, s *Service, id string, in FunctionInput) {
	t.Helper()
	if _, _, err := s.CreateFunction(context.Background(), "proj", "us-central1", id, in, V2); err != nil {
		t.Fatalf("CreateFunction %s: %v", id, err)
	}
}

// TestCallFunctionThrottlesAtFunctionCapacity verifies a function configured
// with maxInstanceCount (and no per-instance concurrency) admits one in-flight
// invocation and rejects the next with RESOURCE_EXHAUSTED / 429, then admits
// again once the slot is released.
func TestCallFunctionThrottlesAtFunctionCapacity(t *testing.T) {
	s := newAdmissionService(t, 0)
	createAdmissionFn(t, s, "fn", FunctionInput{Runtime: "nodejs20", MaxInstanceCount: 1})
	ex := newBlockingExecutor()
	s.executor = ex
	defer ex.releaseAll()

	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, _, _, err := s.CallFunction(context.Background(), "proj", "us-central1", "fn", "first"); err != nil {
			t.Errorf("first call: %v", err)
		}
	}()
	waitEntered(t, ex, 1)

	_, _, _, err := s.CallFunction(context.Background(), "proj", "us-central1", "fn", "second")
	assertResourceExhausted(t, err)

	ex.releaseAll()
	<-done

	// The slot was released: the next invocation is admitted.
	if _, _, _, err := s.CallFunction(context.Background(), "proj", "us-central1", "fn", "third"); err != nil {
		t.Fatalf("call after release: %v", err)
	}
}

// TestCallFunctionCapacityIncludesPerInstanceConcurrency verifies the capacity
// is maxInstanceCount × maxInstanceRequestConcurrency: one instance serving two
// concurrent requests admits two and rejects the third.
func TestCallFunctionCapacityIncludesPerInstanceConcurrency(t *testing.T) {
	s := newAdmissionService(t, 0)
	createAdmissionFn(t, s, "fn", FunctionInput{
		Runtime:                       "nodejs20",
		MaxInstanceCount:              1,
		MaxInstanceRequestConcurrency: 2,
	})
	ex := newBlockingExecutor()
	s.executor = ex
	defer ex.releaseAll()

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, _, _, err := s.CallFunction(context.Background(), "proj", "us-central1", "fn", "in"); err != nil {
				t.Errorf("admitted call: %v", err)
			}
		}()
	}
	waitEntered(t, ex, 2)

	_, _, _, err := s.CallFunction(context.Background(), "proj", "us-central1", "fn", "over")
	assertResourceExhausted(t, err)

	ex.releaseAll()
	wg.Wait()
}

// TestCallFunctionUnlimitedWhenUnconfigured verifies a function with no
// configured maxInstanceCount is unthrottled: the gate's fast path leaves mock
// and default-mode behavior untouched.
func TestCallFunctionUnlimitedWhenUnconfigured(t *testing.T) {
	s := newAdmissionService(t, 0)
	createAdmissionFn(t, s, "fn", FunctionInput{Runtime: "nodejs20"})
	ex := newBlockingExecutor()
	s.executor = ex
	defer ex.releaseAll()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, _, _, err := s.CallFunction(context.Background(), "proj", "us-central1", "fn", "x"); err != nil {
				t.Errorf("unconfigured call: %v", err)
			}
		}()
	}
	waitEntered(t, ex, 8)
	ex.releaseAll()
	wg.Wait()
}

// TestCallFunctionAccountCapThrottles verifies the project-wide account cap
// rejects across function boundaries: two functions with no per-function limit
// still share the project admission budget.
func TestCallFunctionAccountCapThrottles(t *testing.T) {
	s := newAdmissionService(t, 1)
	createAdmissionFn(t, s, "a", FunctionInput{Runtime: "nodejs20"})
	createAdmissionFn(t, s, "b", FunctionInput{Runtime: "nodejs20"})
	ex := newBlockingExecutor()
	s.executor = ex
	defer ex.releaseAll()

	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, _, _, err := s.CallFunction(context.Background(), "proj", "us-central1", "a", "x"); err != nil {
			t.Errorf("first call: %v", err)
		}
	}()
	waitEntered(t, ex, 1)

	_, _, _, err := s.CallFunction(context.Background(), "proj", "us-central1", "b", "y")
	assertResourceExhausted(t, err)

	ex.releaseAll()
	<-done
}

// TestConcurrencyGateReset verifies Reset drops in-flight counts, so a new
// invocation is admitted even if a pre-reset invocation never released.
func TestConcurrencyGateReset(t *testing.T) {
	g := newConcurrencyGate()
	pre, err := g.acquire("p", "k", 1, 0)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if _, err := g.acquire("p", "k", 1, 0); err == nil {
		t.Fatal("expected throttle before reset")
	}
	g.reset()
	post, err := g.acquire("p", "k", 1, 0)
	if err != nil {
		t.Fatalf("acquire after reset: %v", err)
	}
	// Releasing the pre-reset slot must not disturb the post-reset counter.
	pre()
	if _, err := g.acquire("p", "k", 1, 0); err == nil {
		t.Fatal("expected throttle while post-reset slot is held")
	}
	post()
}

// TestConcurrencyGateProjectRejectionReleasesFunctionSlot verifies that when the
// project cap rejects an invocation whose function slot was already reserved,
// the function slot is rolled back (no leak).
func TestConcurrencyGateProjectRejectionReleasesFunctionSlot(t *testing.T) {
	g := newConcurrencyGate()
	// Function capacity 2, project cap 1.
	relA, err := g.acquire("p", "fn", 2, 1)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	// The second acquire reserves the second function slot, then the project
	// cap rejects it; the function slot must be released.
	_, err = g.acquire("p", "fn", 2, 1)
	assertResourceExhausted(t, err)
	if got := g.counter(g.funcs, "fn").n.Load(); got != 1 {
		t.Fatalf("function counter = %d after project rejection, want 1 (slot leaked)", got)
	}
	relA()
	if got := g.counter(g.funcs, "fn").n.Load(); got != 0 {
		t.Fatalf("function counter = %d after release, want 0", got)
	}
}

func TestEffectiveConcurrencyLimit(t *testing.T) {
	cases := []struct {
		name string
		fn   functionsstore.Function
		want int64
	}{
		{"unconfigured is unlimited", functionsstore.Function{}, 0},
		{"max only", functionsstore.Function{MaxInstanceCount: 5}, 5},
		{"max times per-instance concurrency", functionsstore.Function{MaxInstanceCount: 5, MaxInstanceRequestConcurrency: 80}, 400},
		{"negative concurrency treated as one", functionsstore.Function{MaxInstanceCount: 5, MaxInstanceRequestConcurrency: -1}, 5},
		{"negative max is unlimited", functionsstore.Function{MaxInstanceCount: -1, MaxInstanceRequestConcurrency: 10}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := effectiveConcurrencyLimit(tc.fn); got != tc.want {
				t.Fatalf("effectiveConcurrencyLimit(%+v) = %d, want %d", tc.fn, got, tc.want)
			}
		})
	}
}
