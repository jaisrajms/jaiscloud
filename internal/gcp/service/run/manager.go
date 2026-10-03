package run

import (
	"context"
	"errors"

	runstore "jaiscloud/internal/gcp/store/run"
)

// ErrNoReadyRuntime is returned by a RuntimeManager when a request cannot be
// forwarded because no revision runtime is ready. The REST transport maps it to
// 503, matching the Cloud Run contract.
var ErrNoReadyRuntime = errors.New("no ready runtime for Cloud Run service")

// InvocationRequest is a data-plane request forwarded to a running revision.
type InvocationRequest struct {
	// Host is the request Host header, from which the runtime resolves the
	// target service when Service is unset.
	Host string
	// Service is the target service; the runtime resolves its latest ready
	// revision endpoint.
	Service runstore.Service
	Method  string
	Path    string
	Query   string
	Headers map[string]string
	Body    []byte
}

// Invocation is the raw HTTP response returned by a revision runtime.
type Invocation struct {
	Status  int
	Headers map[string]string
	Body    []byte
}

// RuntimeManager is the seam between the Cloud Run metadata control plane and
// the container runtime. W1.1 ships only MockRuntime (no containers run); W1.2
// adds a k8s implementation that launches a Pod + ClusterIP Service per revision
// and reverse-proxies to it. The metadata core does not change.
type RuntimeManager interface {
	// EnsureRevision makes a revision's runtime ready. In k8s mode this starts
	// the Pod + Service and waits for the port; the mock is a no-op.
	EnsureRevision(ctx context.Context, svc runstore.Service, rev runstore.Revision) error
	// RemoveRevision tears down a revision's runtime (service delete, template
	// replacement, reset, orphan sweep).
	RemoveRevision(ctx context.Context, rev runstore.Revision) error
	// RemoveService tears down every runtime of a service.
	RemoveService(ctx context.Context, svc runstore.Service) error
	// Invoke forwards a data-plane request to the service's latest ready
	// revision, returning its status/headers/body. Missing service or no ready
	// runtime is an error (mapped by the provider).
	Invoke(ctx context.Context, req InvocationRequest) (Invocation, error)
	// Reset tears down every runtime (/_jaiscloud/reset).
	Reset(ctx context.Context)
}

// MockRuntime is the only RuntimeManager that ships in W1.1: a Cloud Run service
// is a metadata record, not a running container, so lifecycle calls are no-ops
// and invocation reports that no runtime is ready.
type MockRuntime struct{}

// EnsureRevision is a no-op.
func (MockRuntime) EnsureRevision(context.Context, runstore.Service, runstore.Revision) error {
	return nil
}

// RemoveRevision is a no-op.
func (MockRuntime) RemoveRevision(context.Context, runstore.Revision) error { return nil }

// RemoveService is a no-op.
func (MockRuntime) RemoveService(context.Context, runstore.Service) error { return nil }

// Invoke reports that no runtime is ready (the mock runs no container).
func (MockRuntime) Invoke(context.Context, InvocationRequest) (Invocation, error) {
	return Invocation{}, ErrNoReadyRuntime
}

// Reset is a no-op.
func (MockRuntime) Reset(context.Context) {}
