package kafka

import (
	"context"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"jaiscloud/internal/docker"
)

// TestDockerBrokerContainerAddressing is the CDE1 regression for Managed Kafka:
// a containerized emulator cannot reach a published loopback port, so the broker
// joins the shared network and advertises its container name, which is what the
// admin pool and any workload on that network resolve.
func TestDockerBrokerContainerAddressing(t *testing.T) {
	f := newFakeDockerAPI()
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)

	b := newDockerBroker(Config{
		Mode:         string(ModeDocker),
		Image:        "redpanda:test",
		InstanceID:   "inst0001",
		DockerClient: kafkaDockerClient(srv),
		ReadyTimeout: 3 * time.Second,
		Addressing:   docker.Addressing{Mode: docker.DeployContainer, Network: "shared-net"},
	}, discardLogger())

	var readied []string
	b.ready = func(_ context.Context, addr string) error {
		readied = append(readied, addr)
		return nil
	}

	key := ClusterKey{Project: "proj", Location: "us-central1", Cluster: "My_Cluster"}
	got, err := b.EnsureCluster(context.Background(), key.Project, key.Location, key.Cluster)
	if err != nil {
		t.Fatalf("EnsureCluster: %v", err)
	}
	name := dockerBrokerName("inst0001", key)
	want := name + ":" + strconv.Itoa(kafkaPort)
	if got != want {
		t.Errorf("advertised address = %q, want %q", got, want)
	}
	if len(readied) != 1 || readied[0] != want {
		t.Errorf("readiness dialled %v, want [%s]", readied, want)
	}

	var body map[string]any
	for _, cb := range f.created {
		body = cb
	}
	hc, _ := body["HostConfig"].(map[string]any)
	if pb, ok := hc["PortBindings"]; ok {
		t.Errorf("container deployment must publish no host port; got %v", pb)
	}
	if hc["NetworkMode"] != "shared-net" {
		t.Errorf("NetworkMode = %v, want the shared network", hc["NetworkMode"])
	}
	cmd, _ := body["Cmd"].([]any)
	found := false
	for _, a := range cmd {
		if s, ok := a.(string); ok && s == "PLAINTEXT://"+want {
			found = true
		}
	}
	if !found {
		t.Errorf("advertise-kafka-addr missing from Cmd: %v", cmd)
	}
}
