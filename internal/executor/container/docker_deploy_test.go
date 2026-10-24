package container

import (
	"context"
	"net/http"
	"testing"

	"jaiscloud/internal/docker"
)

// TestDockerExecutor_ContainerDeploymentPublishesNoHostPort is the CDE1
// regression: when the emulator itself runs in a container, a workload's
// published host port is on the daemon's host and unreachable from the
// emulator's own network namespace, so the executor must publish nothing and
// join the shared network to address the function by container name.
func TestDockerExecutor_ContainerDeploymentPublishesNoHostPort(t *testing.T) {
	fake := &fakeDockerTransport{}
	client := docker.New(docker.Config{Client: &http.Client{Transport: fake}})

	e := NewDockerExecutor(
		Config{
			Mode:       "docker",
			InstanceID: "cde1",
			Network:    "jaiscloud_jaiscloud",
			Addressing: docker.Addressing{
				Mode:     docker.DeployContainer,
				Network:  "jaiscloud_jaiscloud",
				BindRoot: t.TempDir(),
			},
		},
		testProfile{}, nil, WithDockerClient(client),
	)
	defer e.Close()

	name, _, _, _, err := e.startContainer(context.Background(), Request{FunctionName: "fn"}, "img:1", 9100)
	if err != nil {
		t.Fatalf("startContainer: %v", err)
	}
	if fake.createBody == nil {
		t.Fatal("no container create body captured")
	}
	hostCfg, _ := fake.createBody["HostConfig"].(map[string]any)
	if pb, ok := hostCfg["PortBindings"]; ok {
		t.Errorf("container deployment must publish no host port; got %v", pb)
	}
	if got := hostCfg["NetworkMode"]; got != "jaiscloud_jaiscloud" {
		t.Errorf("NetworkMode = %v, want the shared network", got)
	}
	// The invoke request addresses the container by name on the shared network,
	// never the (unreachable) host loopback port.
	if got, want := e.addr.URL(name, e.profile.InvocationPort(), 9100), "http://"+name+":8080"; got != want {
		t.Errorf("invoke URL = %q, want %q", got, want)
	}
}

// TestDockerExecutor_HostDeploymentStillPublishesLoopbackPort guards the
// supported host-process deployment against the container-mode change: a zero
// Addressing keeps the loopback published port and the executor's network.
func TestDockerExecutor_HostDeploymentStillPublishesLoopbackPort(t *testing.T) {
	fake := &fakeDockerTransport{}
	client := docker.New(docker.Config{Client: &http.Client{Transport: fake}})

	e := NewDockerExecutor(
		Config{Mode: "docker", InstanceID: "host", Network: "jaiscloud-net"},
		testProfile{}, nil, WithDockerClient(client),
	)
	defer e.Close()

	if _, _, _, _, err := e.startContainer(context.Background(), Request{FunctionName: "fn"}, "img:1", 9100); err != nil {
		t.Fatalf("startContainer: %v", err)
	}
	hostCfg, _ := fake.createBody["HostConfig"].(map[string]any)
	pb, ok := hostCfg["PortBindings"].(map[string]any)
	if !ok {
		t.Fatalf("host deployment must publish a host port; HostConfig = %v", hostCfg)
	}
	bindings, _ := pb["8080/tcp"].([]any)
	if len(bindings) != 1 {
		t.Fatalf("host PortBindings = %v, want one 8080/tcp binding", pb)
	}
	if binding, _ := bindings[0].(map[string]any); binding["HostPort"] != "9100" {
		t.Errorf("host binding = %v, want HostPort 9100", bindings[0])
	}
	if got := e.addr.HostPort("jc-fn", 8080, 9100); got != "127.0.0.1:9100" {
		t.Errorf("host address = %q, want 127.0.0.1:9100", got)
	}
}
