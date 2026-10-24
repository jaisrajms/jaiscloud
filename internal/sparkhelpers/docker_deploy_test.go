package sparkhelpers

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"jaiscloud/internal/docker"
)

// TestDockerDriverContainerDeployment is the CDE1 regression for Dataproc: a
// containerized emulator must not map host.docker.internal (the daemon host,
// where the emulator is not) and must join the shared network so the driver can
// reach the emulator by name.
func TestDockerDriverContainerDeployment(t *testing.T) {
	f := newFakeDockerAPI()
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)

	d := NewDockerDriver(DockerConfig{
		Client:       dockerDriverTestClient(srv),
		InstanceID:   "inst0001",
		PollInterval: 5 * time.Millisecond,
		ReapTimeout:  2 * time.Second,
		Addressing:   docker.Addressing{Mode: docker.DeployContainer, Network: "shared-net"},
	})

	if _, err := d.Submit(context.Background(), pysparkTestJob("j1")); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	var body map[string]any
	for _, b := range f.created {
		body = b
	}
	hc, _ := body["HostConfig"].(map[string]any)
	if eh, ok := hc["ExtraHosts"]; ok {
		t.Errorf("container deployment must not map host.docker.internal; got %v", eh)
	}
	if hc["NetworkMode"] != "shared-net" {
		t.Errorf("NetworkMode = %v, want the shared network", hc["NetworkMode"])
	}
}
