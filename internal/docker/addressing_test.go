package docker

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// resolve isolates ResolveAddressing from the ambient environment: tests must
// never depend on whether the test runner itself happens to be in a container.
func resolve(t *testing.T, env map[string]string, network string) Addressing {
	t.Helper()
	for _, k := range []string{EnvDeployMode, EnvNetwork, EnvBindRoot} {
		t.Setenv(k, "")
		os.Unsetenv(k)
	}
	for k, v := range env {
		t.Setenv(k, v)
	}
	return ResolveAddressing(network)
}

func TestResolveAddressingHostByDefault(t *testing.T) {
	a := resolve(t, map[string]string{EnvDeployMode: "host"}, "jaiscloud-net")
	if a.Containerized() {
		t.Fatalf("mode = %q, want host", a.Mode)
	}
	if a.Network != "jaiscloud-net" {
		t.Errorf("network = %q, want the executor's own", a.Network)
	}
	if err := a.Validate(); err != nil {
		t.Errorf("host mode must validate: %v", err)
	}
	if !a.PublishHostPorts() {
		t.Error("host mode must publish host ports")
	}
}

func TestResolveAddressingContainerNeedsNetwork(t *testing.T) {
	a := resolve(t, map[string]string{EnvDeployMode: "container"}, "")
	if !a.Containerized() {
		t.Fatalf("mode = %q, want container", a.Mode)
	}
	if err := a.Validate(); err == nil {
		t.Fatal("container mode without a network must not validate")
	}
	// An executor's own host-mode default network must not satisfy container
	// mode: it is not a network the container deployment created.
	b := resolve(t, map[string]string{EnvDeployMode: "container"}, "jaiscloud-net")
	if b.Network != "" {
		t.Errorf("container mode took the host-mode default network %q", b.Network)
	}
	if err := b.Validate(); err == nil {
		t.Fatal("a host-mode default network must not validate container mode")
	}
}

func TestResolveAddressingNetworkEnvOverridesExecutorDefault(t *testing.T) {
	a := resolve(t, map[string]string{EnvDeployMode: "container", EnvNetwork: "jaiscloud_jaiscloud"}, "jaiscloud-net")
	if a.Network != "jaiscloud_jaiscloud" {
		t.Fatalf("network = %q, want the env override", a.Network)
	}
	if err := a.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
}

func TestResolveAddressingModeOverrideBeatsDetection(t *testing.T) {
	// A host-networked container can share the daemon's host despite the
	// container markers; the explicit override must win.
	a := resolve(t, map[string]string{EnvDeployMode: "HOST"}, "net")
	if a.Containerized() {
		t.Fatal("explicit host mode must win over detection")
	}
}

func TestAddressingHostPortAndURL(t *testing.T) {
	host := Addressing{Mode: DeployHost}
	if got, want := host.HostPort("jc-fn", 8080, 9101), "127.0.0.1:9101"; got != want {
		t.Errorf("host HostPort = %q, want %q", got, want)
	}
	if got, want := host.URL("jc-fn", 8080, 9101), "http://127.0.0.1:9101"; got != want {
		t.Errorf("host URL = %q, want %q", got, want)
	}

	ctr := Addressing{Mode: DeployContainer, Network: "net"}
	if got, want := ctr.HostPort("jc-fn", 8080, 0), "jc-fn:8080"; got != want {
		t.Errorf("container HostPort = %q, want %q", got, want)
	}
	if got, want := ctr.URL("jc-fn", 8080, 0), "http://jc-fn:8080"; got != want {
		t.Errorf("container URL = %q, want %q", got, want)
	}
}

// TestDNSNameCapsLabelLength is the fix for the container-mode addressing limit:
// a 70-byte Cloud Run revision name cannot be resolved by Docker's embedded DNS,
// so the alias (and the address) is capped at a DNS label and registered on the
// network.
func TestDNSNameCapsLabelLength(t *testing.T) {
	short := "jc-cloudrun-985693e2-cloudrun-cde1-dbg-df89cdd23d"
	if got := DNSName(short); got != short {
		t.Errorf("DNSName(short) = %q, want it unchanged", got)
	}
	lng := "jc-cloudrun-985693e2-cloudrun-cr-docker-1792009794234603076-94407970c2"
	got := DNSName(lng)
	if len(got) > DNSLabelMax {
		t.Errorf("DNSName(long) = %q, len %d > %d", got, len(got), DNSLabelMax)
	}
	if !strings.HasPrefix(got, "jc-cloudrun-") || strings.HasSuffix(got, "-") {
		t.Errorf("DNSName(long) = %q, want an instance-prefixed DNS label", got)
	}
	if DNSName(lng) != got {
		t.Error("DNSName must be deterministic")
	}
	if DNSName(lng+"x") == got {
		t.Error("distinct names must not collide")
	}
	if up := DNSName("JC-CloudRun-UPPER"); up != "jc-cloudrun-upper" {
		t.Errorf("DNSName must lowercase: %q", up)
	}

	a := Addressing{Mode: DeployContainer, Network: "shared-net"}
	if want := DNSName(lng) + ":80"; a.HostPort(lng, 80, 0) != want {
		t.Errorf("container address = %q, want %q", a.HostPort(lng, 80, 0), want)
	}
	nc := a.NetworkingConfig(lng)
	if nc == nil {
		t.Fatal("container mode must register a network alias")
	}
	endpoints, _ := nc["EndpointsConfig"].(map[string]any)
	netCfg, _ := endpoints["shared-net"].(map[string]any)
	aliases, _ := netCfg["Aliases"].([]string)
	if len(aliases) != 1 || aliases[0] != DNSName(lng) {
		t.Errorf("alias = %v, want [%s]", aliases, DNSName(lng))
	}
	if (Addressing{Mode: DeployHost}).NetworkingConfig(lng) != nil {
		t.Error("host mode must not register a network alias")
	}
}

func TestAddressingPortBindings(t *testing.T) {
	host := Addressing{Mode: DeployHost}
	got := host.PortBindings(8080, 9101)
	binding, ok := got["8080/tcp"].([]map[string]any)
	if !ok || len(binding) != 1 {
		t.Fatalf("host PortBindings = %#v", got)
	}
	if binding[0]["HostPort"] != "9101" || binding[0]["HostIp"] != "127.0.0.1" {
		t.Errorf("host binding = %#v", binding[0])
	}
	// An ephemeral request leaves HostPort empty for the daemon to assign.
	if b := host.PortBindings(8080, 0)["8080/tcp"].([]map[string]any)[0]["HostPort"]; b != "" {
		t.Errorf("ephemeral HostPort = %q, want empty", b)
	}
	if got := (Addressing{Mode: DeployContainer, Network: "net"}).PortBindings(8080, 9101); got != nil {
		t.Errorf("container PortBindings = %#v, want nil (no host-published port)", got)
	}
}

func TestAddressingTempDir(t *testing.T) {
	host := Addressing{Mode: DeployHost}
	dir, err := host.TempDir("jc-host-*")
	if err != nil {
		t.Fatalf("host TempDir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	if filepath.Dir(dir) == "" || filepath.Dir(dir) == "." {
		t.Fatalf("host TempDir = %q, want an absolute path", dir)
	}

	// Container mode without a shared root cannot produce a daemon-visible bind
	// source: the failure must be explicit, not a silently-empty mount.
	ctr := Addressing{Mode: DeployContainer, Network: "net"}
	if _, err := ctr.TempDir("jc-ctr-*"); err == nil {
		t.Fatal("container TempDir without a bind root must fail")
	}

	root := t.TempDir()
	ctr.BindRoot = root
	dir, err = ctr.TempDir("jc-ctr-*")
	if err != nil {
		t.Fatalf("container TempDir: %v", err)
	}
	if filepath.Dir(dir) != root {
		t.Fatalf("container TempDir = %q, want a child of the bind root %q", dir, root)
	}
}

func TestProbeReportsContainerMisconfiguration(t *testing.T) {
	// A reachable socket plus an unusable addressing must fail the probe: this
	// is the CDE1 trap where the socket probe reported "reachable" and the
	// first invoke failed instead.
	_, srv := testClient(t, newFakeAPI())
	_ = srv
	if err := (Addressing{Mode: DeployContainer}).Validate(); err == nil {
		t.Fatal("container addressing without a network must fail validation")
	}
}

func TestEmulatorContainerizedIsConservative(t *testing.T) {
	// The dev/CI runner here is not containerized, and no marker is consulted
	// when an explicit mode is set — guard the override path.
	t.Setenv(EnvDeployMode, "host")
	if a := ResolveAddressing(""); a.Containerized() {
		t.Fatal("explicit host mode must never resolve to container")
	}
	// Probe on an unreachable socket must surface the dial error, not a nil.
	if err := Probe(context.Background(), "/nonexistent/docker.sock", Addressing{}); err == nil {
		t.Fatal("Probe against a missing socket must fail")
	}
}
