package container

import (
	"context"
	"fmt"
	"strings"
	"time"

	"jaiscloud/internal/clock"
	containerstore "jaiscloud/internal/gcp/store/container"
	"jaiscloud/internal/model"
)

// This file models GKE's concurrent-operation rules (DC15).
//
// Real GKE runs at most one operation against a cluster at a time and at most
// one against any single node pool: pools of one cluster may upgrade in
// parallel, but a cluster-level change serializes the whole cluster. A mutating
// call that overlaps an operation still running on its target is rejected with
// the 400 FAILED_PRECONDITION real GKE answers — "Cluster is running
// incompatible operation operation-…" — and a project/location that has used up
// its concurrent-operations quota is throttled with 429 RESOURCE_EXHAUSTED.
//
// The metadata emulator has no asynchronous lifecycle, so by default every
// operation is created DONE: nothing is ever in flight and neither rule can
// fire, which is the behaviour every existing client and test relies on.
// WithOperationWindow opens a simulated in-flight window instead: the operation
// a mutation returns starts RUNNING and settles to DONE that long after it
// started, observed lazily on read (the same lazy-settlement pattern the
// Dataproc cluster/job delays use) and honoured by the conflict check.
// WithConcurrentOperationQuota caps how many operations may be in flight in one
// project/location while such a window is open. Both knobs are process
// configuration, not persisted state: they survive a store reset, and a restart
// simply begins with nothing in flight.

// Option configures a GKE core at construction.
type Option func(*Service)

// WithOperationWindow simulates GKE's operation lifecycle: a mutation's
// operation starts RUNNING and settles to DONE d after it started, so the
// single-operation-per-target rule and the concurrent-operations quota become
// observable. The default (0) keeps the emulator's instant-DONE behaviour.
func WithOperationWindow(d time.Duration) Option {
	return func(s *Service) {
		if d > 0 {
			s.operationWindow = d
		}
	}
}

// WithConcurrentOperationQuota caps the number of operations that may be in
// flight in one project/location. A request beyond the cap is rejected with
// RESOURCE_EXHAUSTED / HTTP 429 — the concurrent-operations quota surface real
// GKE exposes. A value <= 0 (the default) means unlimited. The cap only has an
// effect while WithOperationWindow is open, because otherwise no operation is
// ever in flight.
func WithConcurrentOperationQuota(n int) Option {
	return func(s *Service) {
		if n > 0 {
			s.concurrentOperationsQuota = n
		}
	}
}

// acquireOperation applies GKE's concurrent-operation rules to one mutating
// call. It is held from the target check until the caller has recorded its
// operation, so two overlapping requests cannot both pass the check before
// either becomes visible (a check-then-create race would let both through).
//
// The call is rejected with FAILED_PRECONDITION when an operation is already
// running on the target (the cluster, or that one node pool), and with
// RESOURCE_EXHAUSTED when the project/location has no concurrent-operation slot
// left. With no simulated window nothing can be in flight, so the fast path
// neither locks nor scans the operation store. The returned release must be
// called exactly once, after the mutation's operation has been recorded.
func (s *Service) acquireOperation(ctx context.Context, project, location, clusterLink, poolLink string) (func(), error) {
	if s.operationWindow <= 0 {
		return func() {}, nil
	}
	s.opMu.Lock()
	if err := s.checkOperationConflict(ctx, project, location, clusterLink, poolLink); err != nil {
		s.opMu.Unlock()
		return nil, err
	}
	return s.opMu.Unlock, nil
}

// checkOperationConflict rejects a mutation whose target already has an
// operation running, or whose project/location has exhausted its
// concurrent-operation quota. Callers hold s.opMu.
func (s *Service) checkOperationConflict(ctx context.Context, project, location, clusterLink, poolLink string) error {
	ops, err := s.store.ListOperations(ctx, project, location)
	if err != nil {
		return mapStoreErr(err)
	}
	now := clock.Now()
	inFlight := 0
	for _, op := range ops {
		if !s.operationInFlight(op, now) {
			continue
		}
		inFlight++
		if operationConflicts(op, clusterLink, poolLink) {
			return incompatibleOperation(op.Name)
		}
	}
	if s.concurrentOperationsQuota > 0 && inFlight >= s.concurrentOperationsQuota {
		return concurrentOperationsExceeded(project, location)
	}
	return nil
}

// operationConflicts reports whether an in-flight operation blocks a request on
// the given target. An operation on the cluster itself blocks every mutation of
// the cluster; an operation on one node pool blocks only that pool, so
// different pools of one cluster still mutate in parallel, as on real GKE.
func operationConflicts(op containerstore.Operation, clusterLink, poolLink string) bool {
	if op.TargetLink == clusterLink {
		return true
	}
	if poolLink == "" {
		// A cluster-level request: any node-pool operation on this cluster
		// touches the same cluster record, so it conflicts too.
		return strings.HasPrefix(op.TargetLink, clusterLink+"/nodePools/")
	}
	return op.TargetLink == poolLink
}

// operationInFlight reports whether op is still inside its simulated window.
func (s *Service) operationInFlight(op containerstore.Operation, now time.Time) bool {
	if s.operationWindow <= 0 || op.Status == containerstore.OperationStatusDone {
		return false
	}
	return now.Before(op.StartTime.Add(s.operationWindow))
}

// settleOperation returns op as a reader observes it. An operation whose window
// has elapsed is DONE with the deadline it settled at as its endTime, so a
// client polling the operation sees the transition without a background
// worker. The stored record itself is not rewritten: its startTime plus the
// configured window is all the state the transition needs.
func (s *Service) settleOperation(op containerstore.Operation) containerstore.Operation {
	if s.operationInFlight(op, clock.Now()) {
		return op
	}
	if op.Status != containerstore.OperationStatusDone {
		op.Status = containerstore.OperationStatusDone
		op.EndTime = op.StartTime.Add(s.operationWindow)
	}
	return op
}

// incompatibleOperation is the verbatim real GKE rejection for a mutation that
// overlaps an operation already running on the same target: HTTP 400 with
// google.rpc status FAILED_PRECONDITION, naming the blocking operation.
func incompatibleOperation(opName string) error {
	return &model.ProviderError{
		Code:       "FailedPrecondition",
		Message:    "Cluster is running incompatible operation " + opName,
		HTTPStatus: 400,
		Status:     "FAILED_PRECONDITION",
	}
}

// concurrentOperationsExceeded is the concurrent-operations quota rejection
// (RESOURCE_EXHAUSTED / HTTP 429), worded like the Google quota failure it
// stands in for.
func concurrentOperationsExceeded(project, location string) error {
	return &model.ProviderError{
		Code: "ResourceExhausted",
		Message: fmt.Sprintf("Quota exceeded for quota metric 'Concurrent operations' and "+
			"limit 'Concurrent operations per project' of service 'container.googleapis.com' "+
			"for consumer 'project:%s' in location '%s'.", project, location),
		HTTPStatus: 429,
		Status:     "RESOURCE_EXHAUSTED",
	}
}
