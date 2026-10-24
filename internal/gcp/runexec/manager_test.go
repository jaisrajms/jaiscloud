package runexec

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	runcore "jaiscloud/internal/gcp/service/run"
	runstore "jaiscloud/internal/gcp/store/run"
	"jaiscloud/internal/model"
)

func assertProviderStatus(t *testing.T, err error, want int) {
	t.Helper()
	var perr *model.ProviderError
	if !errors.As(err, &perr) {
		t.Fatalf("err = %v, want *model.ProviderError", err)
	}
	if perr.HTTPStatus != want {
		t.Fatalf("status = %d, want %d (%v)", perr.HTTPStatus, want, err)
	}
}

func TestEnsureRevisionRegistersAndRemoveServiceSweeps(t *testing.T) {
	client := fake.NewSimpleClientset()
	m := New(Config{Client: client, Namespace: "jaiscloud", Probe: func(string) bool { return true }})

	svc, rev := revisionWithContainer(map[string]any{
		"image": "nginx:latest",
		"ports": []any{map[string]any{"containerPort": float64(80)}},
	})
	ctx := context.Background()
	if err := m.EnsureRevision(ctx, svc, rev); err != nil {
		t.Fatalf("EnsureRevision: %v", err)
	}

	name := workloadName(svc, rev)
	if _, err := client.CoreV1().Pods("jaiscloud").Get(ctx, name, metav1.GetOptions{}); err != nil {
		t.Fatalf("revision pod not created: %v", err)
	}
	if _, err := client.CoreV1().Services("jaiscloud").Get(ctx, name, metav1.GetOptions{}); err != nil {
		t.Fatalf("revision service not created: %v", err)
	}

	host := normalizeHost(runcore.InvocationAuthority("p", "l", "svc"))
	svcName := runcore.ServiceName("p", "l", "svc")
	if hostOK, svcOK := m.reg.lookup(svcName, host); !hostOK || !svcOK {
		t.Fatalf("registry missing target: byHost=%v byService=%v", hostOK, svcOK)
	}

	if err := m.RemoveService(ctx, svc); err != nil {
		t.Fatalf("RemoveService: %v", err)
	}
	if _, err := client.CoreV1().Pods("jaiscloud").Get(ctx, name, metav1.GetOptions{}); err == nil {
		t.Error("revision pod survived RemoveService")
	}
	if _, svcOK := m.reg.lookup(svcName, host); svcOK {
		t.Error("service still registered after RemoveService")
	}
}

func TestRemoveRevisionDeregistersOnlyWhenLatest(t *testing.T) {
	client := fake.NewSimpleClientset()
	m := New(Config{Client: client, Namespace: "jaiscloud", Probe: func(string) bool { return true }})
	svc, rev1 := revisionWithContainer(map[string]any{"image": "nginx:latest"})
	ctx := context.Background()
	if err := m.EnsureRevision(ctx, svc, rev1); err != nil {
		t.Fatalf("EnsureRevision rev1: %v", err)
	}
	rev2 := rev1
	rev2.ID = "svc-00002"
	if err := m.EnsureRevision(ctx, svc, rev2); err != nil {
		t.Fatalf("EnsureRevision rev2: %v", err)
	}

	// Removing the older revision must keep the newer target registered.
	if err := m.RemoveRevision(ctx, rev1); err != nil {
		t.Fatalf("RemoveRevision: %v", err)
	}
	if _, err := client.CoreV1().Pods("jaiscloud").Get(ctx, workloadName(svc, rev1), metav1.GetOptions{}); err == nil {
		t.Error("old revision pod survived RemoveRevision")
	}
	tgt := m.reg.serviceTarget(runcore.ServiceName("p", "l", "svc"))
	if tgt == nil || tgt.revision != rev2.ID {
		t.Fatalf("latest target = %+v, want revision %s", tgt, rev2.ID)
	}
}

func TestManagerInvokeProxyStatusMapping(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/slow":
			time.Sleep(500 * time.Millisecond)
		case "/err":
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("boom"))
			return
		}
		w.Header().Set("X-Upstream", "yes")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("hello " + r.URL.RawQuery))
	}))
	defer upstream.Close()

	m := New(Config{Client: fake.NewSimpleClientset(), Proxy: &http.Client{Timeout: 100 * time.Millisecond}})
	host := runcore.InvocationAuthority("p", "l", "svc")
	svc := runstore.Service{ProjectID: "p", Location: "l", ID: "svc"}
	tgt := &target{
		serviceName: runcore.ServiceName("p", "l", "svc"),
		revision:    "svc-00001",
		backend:     upstream.URL,
	}
	m.reg.put(tgt.serviceName, normalizeHost(host), tgt)

	ctx := context.Background()
	inv, err := m.Invoke(ctx, runcore.InvocationRequest{Host: host, Method: http.MethodGet, Path: "/", Query: "a=1"})
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if inv.Status != http.StatusCreated || string(inv.Body) != "hello a=1" || inv.Headers["X-Upstream"] != "yes" {
		t.Fatalf("Invoke = %d %q %v", inv.Status, inv.Body, inv.Headers)
	}

	// Upstream error status is passed through unchanged.
	inv, err = m.Invoke(ctx, runcore.InvocationRequest{Host: host, Method: http.MethodGet, Path: "/err"})
	if err != nil || inv.Status != http.StatusInternalServerError || string(inv.Body) != "boom" {
		t.Fatalf("error passthrough = %d %q, %v", inv.Status, inv.Body, err)
	}

	// Path-form invocation resolves by service, not host.
	inv, err = m.Invoke(ctx, runcore.InvocationRequest{Service: svc, Method: http.MethodGet, Path: "/", Query: "b=2"})
	if err != nil || string(inv.Body) != "hello b=2" {
		t.Fatalf("service-routed Invoke = %q, %v", inv.Body, err)
	}

	// Timeout maps to 504.
	_, err = m.Invoke(ctx, runcore.InvocationRequest{Host: host, Method: http.MethodGet, Path: "/slow"})
	assertProviderStatus(t, err, http.StatusGatewayTimeout)

	// Dial failure maps to 502.
	tgt.backend = "http://127.0.0.1:1"
	_, err = m.Invoke(ctx, runcore.InvocationRequest{Host: host, Method: http.MethodGet, Path: "/"})
	assertProviderStatus(t, err, http.StatusBadGateway)
}

func TestManagerInvokeNotFoundAndUnavailable(t *testing.T) {
	m := New(Config{Client: fake.NewSimpleClientset()})
	ctx := context.Background()

	_, err := m.Invoke(ctx, runcore.InvocationRequest{
		Host: "svc-abcdef012345.us-central1.run.app", Method: http.MethodGet, Path: "/",
	})
	assertProviderStatus(t, err, http.StatusNotFound)

	_, err = m.Invoke(ctx, runcore.InvocationRequest{
		Service: runstore.Service{ProjectID: "p", Location: "l", ID: "svc"}, Method: http.MethodGet, Path: "/",
	})
	assertProviderStatus(t, err, http.StatusServiceUnavailable)
}

func TestManagerResetClearsAndSweeps(t *testing.T) {
	client := fake.NewSimpleClientset()
	m := New(Config{Client: client, Namespace: "jaiscloud", Probe: func(string) bool { return true }})
	svc, rev := revisionWithContainer(map[string]any{"image": "nginx:latest"})
	ctx := context.Background()
	if err := m.EnsureRevision(ctx, svc, rev); err != nil {
		t.Fatalf("EnsureRevision: %v", err)
	}

	m.Reset(ctx)

	if hosts, services := m.reg.size(); hosts != 0 || services != 0 {
		t.Errorf("registry not cleared: %d hosts / %d services", hosts, services)
	}
	if _, err := client.CoreV1().Pods("jaiscloud").Get(ctx, workloadName(svc, rev), metav1.GetOptions{}); err == nil {
		t.Error("revision pod survived Reset")
	}
}
