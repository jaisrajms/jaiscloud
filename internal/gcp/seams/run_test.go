package seams_test

import (
	"bytes"
	"context"
	"net/http"
	"testing"

	"jaiscloud/internal/clock"
	gcpadapter "jaiscloud/internal/gcp/adapter"
	"jaiscloud/internal/gcp/seams"
	"jaiscloud/internal/gcp/service/run"
	runstore "jaiscloud/internal/gcp/store/run"
	restrun "jaiscloud/internal/gcp/transport/rest/run"
	"jaiscloud/internal/provider"
	"jaiscloud/internal/store"
)

// fakeRunRuntime records the forwarded request and always succeeds, so the
// Eventarc delivery path can be tested without a cluster.
type fakeRunRuntime struct{ last run.InvocationRequest }

func (*fakeRunRuntime) EnsureRevision(context.Context, runstore.Service, runstore.Revision) error {
	return nil
}
func (*fakeRunRuntime) RemoveRevision(context.Context, runstore.Revision) error { return nil }
func (*fakeRunRuntime) RemoveService(context.Context, runstore.Service) error   { return nil }
func (*fakeRunRuntime) Reset(context.Context)                                   {}
func (f *fakeRunRuntime) Invoke(_ context.Context, req run.InvocationRequest) (run.Invocation, error) {
	f.last = req
	return run.Invocation{Status: http.StatusOK, Body: []byte("ok")}, nil
}

func runBody() map[string]any {
	return map[string]any{"template": map[string]any{"containers": []any{map[string]any{"image": "nginx:latest"}}}}
}

// seam builds the Cloud Run seam over a real registry holding the REST provider.
func seam(core *run.Service) *seams.Run {
	reg := provider.NewRegistry().Register(restrun.NewProvider(core))
	return seams.New(reg, gcpadapter.NewAdapter(""), clock.RealClock{}, 0, "us-central1", "proj").Run()
}

func TestRunInvokeDeliversThroughTheRegistryToTheRuntime(t *testing.T) {
	rt := &fakeRunRuntime{}
	core := run.NewService(runstore.NewMemoryStore(), store.NewMemoryResourceStore(), run.WithRuntimeManager(rt))
	if _, err := core.CreateService(context.Background(), "proj", "us-central1", "svc", runBody(), false); err != nil {
		t.Fatal(err)
	}
	status, err := seam(core).Invoke(context.Background(), "proj", "us-central1", "svc", "/hook", map[string]string{"ce-id": "x"}, []byte("payload"))
	if err != nil || status != http.StatusOK {
		t.Fatalf("Invoke = (%d, %v), want (200, nil)", status, err)
	}
	if rt.last.Service.ID != "svc" || rt.last.Method != http.MethodPost || rt.last.Path != "/hook" {
		t.Fatalf("runtime request = %+v", rt.last)
	}
	if !bytes.Equal(rt.last.Body, []byte("payload")) || rt.last.Headers["Ce-Id"] != "x" {
		t.Errorf("runtime saw body %q headers %v", rt.last.Body, rt.last.Headers)
	}
}

// The default mock runtime reports no ready runtime: a service that exists but
// has no running revision is 503.
func TestRunInvokeMapsNoReadyRuntime(t *testing.T) {
	core := run.NewService(runstore.NewMemoryStore(), store.NewMemoryResourceStore())
	if _, err := core.CreateService(context.Background(), "proj", "us-central1", "svc", runBody(), false); err != nil {
		t.Fatal(err)
	}
	status, err := seam(core).Invoke(context.Background(), "proj", "us-central1", "svc", "/", nil, []byte("{}"))
	if status != http.StatusServiceUnavailable || err == nil {
		t.Fatalf("Invoke = (%d, %v), want 503 and an error", status, err)
	}
}

func TestRunInvokeMapsNotFound(t *testing.T) {
	core := run.NewService(runstore.NewMemoryStore(), store.NewMemoryResourceStore())
	status, err := seam(core).Invoke(context.Background(), "proj", "us-central1", "missing", "/", nil, []byte("{}"))
	if status != http.StatusNotFound || err == nil {
		t.Fatalf("Invoke = (%d, %v), want 404 and an error", status, err)
	}
}
