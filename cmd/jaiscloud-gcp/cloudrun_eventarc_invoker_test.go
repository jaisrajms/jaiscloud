package main

import (
	"bytes"
	"context"
	"net/http"
	"testing"

	"jaiscloud/internal/gcp/service/run"
	runstore "jaiscloud/internal/gcp/store/run"
	"jaiscloud/internal/model"
	"jaiscloud/internal/store"
)

// fakeRunRuntime is a RuntimeManager whose Invoke records the forwarded request
// and always succeeds, so the Eventarc invoker adapter can be tested without a
// cluster.
type fakeRunRuntime struct {
	last run.InvocationRequest
}

func (fakeRunRuntime) EnsureRevision(context.Context, runstore.Service, runstore.Revision) error {
	return nil
}
func (fakeRunRuntime) RemoveRevision(context.Context, runstore.Revision) error { return nil }
func (fakeRunRuntime) RemoveService(context.Context, runstore.Service) error   { return nil }
func (fakeRunRuntime) Reset(context.Context)                                   {}

func (f *fakeRunRuntime) Invoke(_ context.Context, req run.InvocationRequest) (run.Invocation, error) {
	f.last = req
	return run.Invocation{Status: http.StatusOK, Body: []byte("ok")}, nil
}

func runServiceBody() map[string]any {
	return map[string]any{"template": map[string]any{
		"containers": []any{map[string]any{"image": "nginx:latest"}},
	}}
}

func TestCloudRunEventarcInvokerDeliversThroughRuntime(t *testing.T) {
	rt := &fakeRunRuntime{}
	core := run.NewService(runstore.NewMemoryStore(), store.NewMemoryResourceStore(), run.WithRuntimeManager(rt))
	if _, err := core.CreateService(context.Background(), "proj", "us-central1", "svc", runServiceBody(), false); err != nil {
		t.Fatalf("create service: %v", err)
	}

	inv := cloudRunEventarcInvoker{run: core}
	headers := map[string]string{"ce-id": "x"}
	status, err := inv.Invoke(context.Background(), "proj", "us-central1", "svc", "/hook", headers, []byte("payload"))
	if err != nil || status != http.StatusOK {
		t.Fatalf("Invoke = (%d, %v), want (200, nil)", status, err)
	}
	if rt.last.Service.ID != "svc" || rt.last.Method != http.MethodPost || rt.last.Path != "/hook" {
		t.Fatalf("runtime request = %+v", rt.last)
	}
	if !bytes.Equal(rt.last.Body, []byte("payload")) {
		t.Errorf("runtime body = %q, want payload", rt.last.Body)
	}
	if rt.last.Headers["ce-id"] != "x" {
		t.Errorf("runtime headers = %+v, want ce-id", rt.last.Headers)
	}
}

func TestCloudRunEventarcInvokerMapsNoReadyRuntime(t *testing.T) {
	// The default MockRuntime reports ErrNoReadyRuntime; the invoker maps it to
	// 503 (a service exists but has no running revision).
	core := run.NewService(runstore.NewMemoryStore(), store.NewMemoryResourceStore())
	if _, err := core.CreateService(context.Background(), "proj", "us-central1", "svc", runServiceBody(), false); err != nil {
		t.Fatalf("create service: %v", err)
	}
	inv := cloudRunEventarcInvoker{run: core}
	status, err := inv.Invoke(context.Background(), "proj", "us-central1", "svc", "/", nil, []byte("{}"))
	if status != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", status)
	}
	if err == nil {
		t.Fatal("err = nil, want a no-ready-runtime error")
	}
}

func TestCloudRunEventarcInvokerMapsNotFound(t *testing.T) {
	core := run.NewService(runstore.NewMemoryStore(), store.NewMemoryResourceStore())
	inv := cloudRunEventarcInvoker{run: core}
	status, err := inv.Invoke(context.Background(), "proj", "us-central1", "missing", "/", nil, []byte("{}"))
	if status != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", status)
	}
	if err == nil {
		t.Fatal("err = nil, want a not-found error")
	}
}

func TestProviderHTTPStatus(t *testing.T) {
	if got := providerHTTPStatus(model.NewProviderError("Unavailable", "x", http.StatusServiceUnavailable)); got != http.StatusServiceUnavailable {
		t.Errorf("ProviderError status = %d, want 503", got)
	}
	if got := providerHTTPStatus(nil); got != http.StatusBadGateway {
		t.Errorf("nil status = %d, want 502", got)
	}
}
