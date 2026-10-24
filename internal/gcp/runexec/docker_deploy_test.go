package runexec

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"jaiscloud/internal/docker"
	runcore "jaiscloud/internal/gcp/service/run"
)

// TestDockerEnsureRevisionContainerAddressing is the CDE1 regression for Cloud
// Run: a containerized emulator publishes no host port, joins the shared
// network, waits for the revision by container name, and registers that same
// name as the proxy backend.
func TestDockerEnsureRevisionContainerAddressing(t *testing.T) {
	f := newFakeDocker(0)
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)

	var probed []string
	m := NewDocker(DockerConfig{
		Client:       dockerTestClient(srv),
		Probe:        func(addr string) bool { probed = append(probed, addr); return true },
		InstanceID:   "inst0001",
		ReadyTimeout: 3 * time.Second,
		Addressing:   docker.Addressing{Mode: docker.DeployContainer, Network: "shared-net"},
	})

	svc, rev := revisionWithContainer(map[string]any{
		"image": "nginx:latest",
		"ports": []any{map[string]any{"containerPort": float64(80)}},
	})
	if err := m.EnsureRevision(context.Background(), svc, rev); err != nil {
		t.Fatalf("EnsureRevision: %v", err)
	}

	if len(f.created) != 1 {
		t.Fatalf("created %d containers, want 1", len(f.created))
	}
	var body map[string]any
	for _, b := range f.created {
		body = b
	}
	hc, _ := body["HostConfig"].(map[string]any)
	if pb, ok := hc["PortBindings"]; ok {
		t.Errorf("container deployment must publish no host port; got %v", pb)
	}
	if got := hc["NetworkMode"]; got != "shared-net" {
		t.Errorf("NetworkMode = %v, want the shared network", got)
	}

	name := dockerContainerName("inst0001", svc.ProjectID, svc.Location, svc.ID, rev.ID)
	if len(probed) == 0 {
		t.Fatal("readiness probe never ran")
	}
	if want := name + ":80"; probed[0] != want {
		t.Errorf("readiness probe = %q, want %q", probed[0], want)
	}

	svcName := runcore.ServiceName(svc.ProjectID, svc.Location, svc.ID)
	target := m.reg.serviceTarget(svcName)
	if target == nil {
		t.Fatalf("no registered target for %s", svcName)
	}
	if want := "http://" + name + ":80"; target.backend != want {
		t.Errorf("backend = %q, want %q", target.backend, want)
	}
}
